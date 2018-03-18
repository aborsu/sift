import math
import numpy
import ujson as json
from bisect import bisect_left, bisect_right
from operator import add
from collections import Counter

from sift.models.links import EntityVocab
from sift.dataset import ModelBuilder, Documents, Model, Mentions, IndexedMentions, Vocab
from sift.util import ngrams, iter_sent_spans, trim_link_subsection, trim_link_protocol

from sift import logging
log = logging.getLogger()

class TermFrequencies(ModelBuilder, Model):
    """ Get term frequencies over a corpus """
    def __init__(self, lowercase, max_ngram):
        self.lowercase = lowercase
        self.max_ngram = max_ngram

    def build(self, docs):
        m = docs.map(lambda d: d['text'])
        if self.lowercase:
            m = m.map(str.lower)

        return m\
            .flatMap(lambda text: ngrams(text, self.max_ngram))\
            .map(lambda t: (t, 1))\
            .reduceByKey(add)\
            .filter(lambda k_v: k_v[1] > 1)

    @staticmethod
    def format_item(self, xxx_todo_changeme):
        (term, count) = xxx_todo_changeme
        return {
            '_id': term,
            'count': count,
        }

class EntityMentions(ModelBuilder, Mentions):
    """ Get aggregated sentence context around links in a corpus """
    def __init__(self, sentence_window = 1, lowercase=False, normalize_url=True, strict_sentences=True):
        self.sentence_window = sentence_window
        self.lowercase = lowercase
        self.strict_sentences = strict_sentences
        self.normalize_url = normalize_url

    @staticmethod
    def iter_mentions(doc, window = 1, norm_url=True, strict=True):
        sent_spans = list(iter_sent_spans(doc['text']))
        sent_offsets = [s.start for s in sent_spans]

        for link in doc['links']:
            # align the link span over sentence spans in the document
            # mention span may cross sentence bounds if sentence tokenisation is dodgy
            # if so, the entire span between bounding sentences will be used as context
            sent_start_idx = bisect_right(sent_offsets, link['start']) - 1
            sent_end_idx = bisect_left(sent_offsets, link['stop']) - 1

            lhs_offset = window / 2
            rhs_offset = (window - lhs_offset) - 1
            sent_start_idx = max(0, sent_start_idx - lhs_offset)
            sent_end_idx = min(len(sent_spans)-1, sent_end_idx + rhs_offset)
            sent_offset = sent_spans[sent_start_idx].start

            span = (link['start'] - sent_offset, link['stop'] - sent_offset)
            target = link['target']
            if norm_url:
                target = trim_link_subsection(link['target'])
                target = trim_link_protocol(target)
            mention = doc['text'][sent_spans[sent_start_idx].start:sent_spans[sent_end_idx].stop]

            # filter out instances where the mention span is the entire sentence
            if span == (0, len(mention)):
                continue

            if strict:
                # filter out list item sentences
                sm = mention.strip()
                if not sm or sm.startswith('*') or sm[-1] not in '.!?"\'':
                    continue

            yield target, doc['_id'], mention, span

    def build(self, docs):
        m = docs.flatMap(lambda d: self.iter_mentions(d, self.sentence_window, self.normalize_url, self.strict_sentences))
        if self.lowercase:
            m = m.map(lambda t_src_m_s: (t_src_m_s[0], t_src_m_s[1], t_src_m_s[2].lower(), t_src_m_s[3]))
        return m

class IndexMappedMentions(EntityMentions, IndexedMentions):
    """ Entity mention corpus with terms mapped to numeric indexes """
    def build(self, sc, docs, vocab):
        tv = sc.broadcast(dict(vocab.map(lambda r: (r['_id'], r['rank'])).collect()))
        return super(IndexMappedMentions, self)\
            .build(docs)\
            .map(lambda m: self.transform(m, tv))

    @staticmethod
    def transform(xxx_todo_changeme4, vocab):
        (target, source, text, span) = xxx_todo_changeme4
        vocab = vocab.value

        start, stop = span
        pre = list(ngrams(text[:start], 1))
        ins = list(ngrams(text[start:stop], 1))
        post = list(ngrams(text[stop:], 1))
        indexes = [vocab.get(t, len(vocab)-1) for t in (pre+ins+post)]

        return target, source, indexes, (len(pre), len(pre)+len(ins))

class TermDocumentFrequencies(ModelBuilder):
    """ Get document frequencies for terms in a corpus """
    def __init__(self, lowercase=False, max_ngram=1, min_df=2):
        self.lowercase = lowercase
        self.max_ngram = max_ngram
        self.min_df = min_df

    def build(self, docs):
        m = docs.map(lambda d: d['text'])
        if self.lowercase:
            m = m.map(lambda text: text.lower())

        return m\
            .flatMap(lambda text: set(ngrams(text, self.max_ngram)))\
            .map(lambda t: (t, 1))\
            .reduceByKey(add)\
            .filter(lambda k_v2: k_v2[1] > self.min_df)

class TermVocab(TermDocumentFrequencies, Vocab):
    """ Generate unique indexes for termed based on their document frequency ranking. """
    def __init__(self, max_rank, min_rank=100, *args, **kwargs):
        self.max_rank = max_rank
        self.min_rank = min_rank
        super(TermVocab, self).__init__(*args, **kwargs)

    def build(self, docs):
        m = super(TermVocab, self)\
            .build(docs)\
            .map(lambda t_df: (t_df[1], t_df[0]))\
            .sortByKey(False)\
            .zipWithIndex()\
            .map(lambda df_t_idx: (df_t_idx[0][1], (df_t_idx[0][0], df_t_idx[1])))

        if self.min_rank != None:
            m = m.filter(lambda t_df_idx: t_df_idx[1][1] >= self.min_rank)
        if self.max_rank != None:
            m = m.filter(lambda t_df_idx1: t_df_idx1[1][1] < self.max_rank)
        return m

    @staticmethod
    def format_item(xxx_todo_changeme5):
        (term, (f, idx)) = xxx_todo_changeme5
        return {
            '_id': term,
            'count': f,
            'rank': idx
        }

class TermIdfs(TermDocumentFrequencies, Model):
    """ Compute tf-idf weighted token counts over sentence contexts around links in a corpus """
    def build(self, corpus):
        log.info('Counting documents in corpus...')
        N = float(corpus.count())
        dfs = super(TermIdfs, self).build(corpus)

        log.info('Building idf model: N=%i', N)
        return dfs\
            .map(lambda term_df_rank: (term_df_rank[0], term_df_rank[1][0]))\
            .mapValues(lambda df: math.log(N/df))

    @staticmethod
    def format_item(xxx_todo_changeme6):
        (term, idf) = xxx_todo_changeme6
        return {
            '_id': term,
            'idf': idf,
        }

class EntityMentionTermFrequency(ModelBuilder, Model):
    """ Compute tf-idf weighted token counts over sentence contexts around links in a corpus """
    def __init__(self, max_ngram=1, normalize = True):
        self.max_ngram = max_ngram
        self.normalize = normalize

    def build(self, mentions, idfs):
        m = mentions\
            .map(lambda target_span_text: (target_span_text[0], target_span_text[1][1]))\
            .mapValues(lambda v: ngrams(v, self.max_ngram))\
            .flatMap(lambda target_tokens: (((target_tokens[0], t), 1) for t in target_tokens[1]))\
            .reduceByKey(add)\
            .map(lambda target_token_count: (target_token_count[0][1], (target_token_count[0][0], target_token_count[1])))\
            .leftOuterJoin(idfs)\
            .filter(lambda token_target_count_idf: token_target_count_idf[1][1] != None)\
            .map(lambda token_target_count_idf3: (token_target_count_idf3[0][0], (token_target_count_idf3[0], math.sqrt(token_target_count_idf3[0][1])*token_target_count_idf3[1][1])))\
            .groupByKey()

        return m.mapValues(self.normalize_counts if self.normalize else list)

    @staticmethod
    def normalize_counts(counts):
        norm = numpy.linalg.norm([v for _, v in counts])
        return [(k, v/norm) for k, v in counts]

    @staticmethod
    def format_item(xxx_todo_changeme7):
        (link, counts) = xxx_todo_changeme7
        return {
            '_id': link,
            'counts': dict(counts),
        }
