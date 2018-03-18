import ujson as json

from operator import add
from collections import Counter
from itertools import chain

from sift.dataset import ModelBuilder, Documents, Model
from sift.util import trim_link_subsection, trim_link_protocol, ngrams

from sift import logging
log = logging.getLogger()

class EntityCounts(ModelBuilder, Model):
    """ Inlink counts """
    def __init__(self, min_count=1, filter_target=None):
        self.min_count = min_count
        self.filter_target = filter_target

    def build(self, docs):
        links = docs\
            .flatMap(lambda d: d['links'])\
            .map(lambda l: l['target'])\
            .map(trim_link_subsection)\
            .map(trim_link_protocol)

        if self.filter_target:
            links = links.filter(lambda l: l.startswith(self.filter_target))

        return links\
            .map(lambda l: (l, 1))\
            .reduceByKey(add)\
            .filter(lambda t_c: t_c[1] > self.min_count)

    @staticmethod
    def format_item(xxx_todo_changeme):
        (target, count) = xxx_todo_changeme
        return {
            '_id': target,
            'count': count
        }

class EntityNameCounts(ModelBuilder, Model):
    """ Entity counts by name """
    def __init__(self, lowercase=False, filter_target=None):
        self.lowercase = lowercase
        self.filter_target = filter_target

    def iter_anchor_target_pairs(self, doc):
        for link in doc['links']:
            target = link['target']
            target = trim_link_subsection(target)
            target = trim_link_protocol(target)

            anchor = doc['text'][link['start']:link['stop']].strip()

            if self.lowercase:
                anchor = anchor.lower()

            if anchor and target:
                yield anchor, target

    def build(self, docs):
        m = docs.flatMap(lambda d: self.iter_anchor_target_pairs(d))

        if self.filter_target:
            m = m.filter(lambda a_t: a_t[1].startswith(self.filter_target))

        return m\
            .groupByKey()\
            .mapValues(Counter)

    @staticmethod
    def format_item(xxx_todo_changeme6):
        (anchor, counts) = xxx_todo_changeme6
        return {
            '_id': anchor,
            'counts': dict(counts),
            'total': sum(counts.values())
        }

class NamePartCounts(ModelBuilder, Model):
    """
    Occurrence counts for ngrams at different positions within link anchors.
        'B' - beginning of span
        'E' - end of span
        'I' - inside span
        'O' - outside span
    """
    def __init__(self, max_ngram=2, lowercase=False, filter_target=None):
        self.lowercase = lowercase
        self.filter_target = filter_target
        self.max_ngram = max_ngram

    def iter_anchors(self, doc):
        for link in doc['links']:
            anchor = doc['text'][link['start']:link['stop']].strip()
            if self.lowercase:
                anchor = anchor.lower()
            if anchor:
                yield anchor

    @staticmethod
    def iter_span_count_types(anchor, n):
        parts = list(ngrams(anchor, n, n))
        if parts:
            yield parts[0], 'B'
            yield parts[-1], 'E'
            for i in range(1, len(parts)-1):
                yield parts[i], 'I'

    def build(self, docs):
        part_counts = docs\
            .flatMap(self.iter_anchors)\
            .flatMap(lambda a: chain.from_iterable(self.iter_span_count_types(a, i) for i in range(1, self.max_ngram+1)))\
            .map(lambda p: (p, 1))\
            .reduceByKey(add)\
            .map(lambda term_spantype_count: (term_spantype_count[0][0], (term_spantype_count[0][1], term_spantype_count[1])))

        part_counts += docs\
            .flatMap(lambda d: ngrams(d['text'], self.max_ngram))\
            .map(lambda t: (t, 1))\
            .reduceByKey(add)\
            .filter(lambda t_c2: t_c2[1] > 1)\
            .map(lambda t_c3: (t_c3[0], ('O', t_c3[1])))

        return part_counts\
            .groupByKey()\
            .mapValues(dict)\
            .filter(lambda t_cs: 'O' in t_cs[1] and len(t_cs[1]) > 1)

    @staticmethod
    def format_item(xxx_todo_changeme7):
        (term, part_counts) = xxx_todo_changeme7
        return {
            '_id': term,
            'counts': dict(part_counts)
        }

class EntityInlinks(ModelBuilder, Model):
    """ Inlink sets for each entity """
    def build(self, docs):
        return docs\
            .flatMap(lambda d: ((d['_id'], l) for l in set(l['target'] for l in d['links'])))\
            .mapValues(trim_link_subsection)\
            .mapValues(trim_link_protocol)\
            .map(lambda k_v: (k_v[1], k_v[0]))\
            .groupByKey()\
            .mapValues(list)

    @staticmethod
    def format_item(xxx_todo_changeme8):
        (target, inlinks) = xxx_todo_changeme8
        return {
            '_id': target,
            'inlinks': inlinks
        }

class EntityVocab(ModelBuilder, Model):
    """ Generate unique indexes for entities in a corpus. """
    def __init__(self, min_rank=0, max_rank=10000):
        self.min_rank = min_rank
        self.max_rank = max_rank

    def build(self, docs):
        log.info('Building entity vocab: df rank range=(%i, %i)', self.min_rank, self.max_rank)
        m = super(EntityVocab, self)\
            .build(docs)\
            .map(lambda target_count: (target_count[1], target_count[0]))\
            .sortByKey(False)\
            .zipWithIndex()\
            .map(lambda df_t_idx: (df_t_idx[0][1], (df_t_idx[0][0], df_t_idx[1])))

        if self.min_rank != None:
            m = m.filter(lambda t_df_idx: t_df_idx[1][1] >= self.min_rank)
        if self.max_rank != None:
            m = m.filter(lambda t_df_idx1: t_df_idx1[1][1] < self.max_rank)
        return m

    @staticmethod
    def format_item(xxx_todo_changeme9):
        (term, (f, idx)) = xxx_todo_changeme9
        return {
            '_id': term,
            'count': f,
            'rank': idx
        }

    @staticmethod
    def load(sc, path, fmt=json):
        log.info('Loading entity-index mapping: %s ...', path)
        return sc\
            .textFile(path)\
            .map(fmt.loads)\
            .map(lambda r: (r['_id'], (r['count'], r['rank'])))

class EntityComentions(ModelBuilder, Model):
    """ Entity comentions """
    @staticmethod
    def iter_unique_links(doc):
        links = set()
        for l in doc['links']:
            link = trim_link_subsection(l['target'])
            link = trim_link_protocol(link)
            if link not in links:
                yield link
                links.add(link)

    def build(self, docs):
        return docs\
            .map(lambda d: (d['_id'], list(self.iter_unique_links(d))))\
            .filter(lambda uri_es: uri_es[1])

    @staticmethod
    def format_item(xxx_todo_changeme10):
        (uri, es) = xxx_todo_changeme10
        return {
            '_id': uri,
            'entities': es
        }

class MappedEntityComentions(EntityComentions):
    """ Entity comentions with entities mapped to a numeric index """
    def build(self, docs, entity_vocab):
        ev = sc.broadcast(dict(ev.collect()))
        return super(MappedEntityComentions, self)\
            .build(docs)\
            .map(lambda uri_es4: (uri_es4[0], [ev.value[e] for e in uri_es4[1] if e in ev.value]))\
            .filter(lambda uri_es5: uri_es5[1])