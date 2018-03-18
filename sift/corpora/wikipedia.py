import ujson as json

from sift.corpora import wikicorpus
from sift.dataset import ModelBuilder, Model, Redirects, Documents

from sift import logging
log = logging.getLogger()

class WikipediaCorpus(ModelBuilder, Model):
    def build(self, sc, path):
        PAGE_DELIMITER = "\n  </page>\n"
        PAGE_START = '<page>\n'
        PAGE_END = '</page>'
        return sc\
            .newAPIHadoopFile(
                path,
                "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                "org.apache.hadoop.io.LongWritable",
                "org.apache.hadoop.io.Text",
                conf = { "textinputformat.record.delimiter": PAGE_DELIMITER })\
            .map(lambda __part: (__part[1].find(PAGE_START), __part[1]))\
            .filter(lambda offset__: offset__[0] >= 0)\
            .map(lambda offset_content: offset_content[1][offset_content[0]:]+PAGE_END)\
            .map(wikicorpus.extract_page)\
            .map(WikipediaCorpus.format_item)

    @staticmethod
    def format_item(xxx_todo_changeme):
        (title, ns, pid, redirect, content) = xxx_todo_changeme
        return {
            '_id': title,
            'pid': pid,
            'namespace': ns,
            'redirect': redirect,
            'content': content
        }

class WikipediaRedirects(ModelBuilder, Redirects):
    """ Extract a set of redirects from wikipedia """
    def __init__(self, resolve_transitive=False):
        self.resolve_transitive = resolve_transitive

    def build(self, pages, verbose=False):
        pfx = wikicorpus.wikilink_prefix
        redirects = pages\
            .filter(lambda page: page['redirect'] != None)\
            .map(lambda page: (page['_id'], page['redirect']))\
            .mapValues(wikicorpus.normalise_wikilink)\
            .map(lambda s_t2: (s_t2[0], pfx+s_t2[1]))

        if self.resolve_transitive:
            redirects = redirects.cache()

            num_targets = redirects\
                .map(lambda k_v1: k_v1[1])\
                .distinct()\
                .count()

            redirects = redirects\
                .map(lambda s_t: (s_t[1], s_t[0])).leftOuterJoin(redirects)\
                .map(lambda target_source_redirect: (target_source_redirect[1][0], target_source_redirect[1][1] or target_source_redirect[0]))

            if verbose:
                redirects = redirects.cache()
                final_num_targets = redirects.map(lambda k_v: k_v[1]).distinct().count()
                log.info('Resolved %i transitive redirects...', num_targets - final_num_targets)

        return redirects.distinct()

class WikipediaArticles(ModelBuilder, Documents):
    """ Prepare a corpus of documents from wikipedia """
    def build(self, corpus, redirects=None):
        articles = corpus\
            .filter(lambda page: page['namespace'] == '0' and page['redirect'] == None and page['content'])\
            .map(lambda page: (page['_id'], page['content']))\
            .map(wikicorpus.remove_markup)\
            .mapValues(wikicorpus.extract_links)

        if redirects:
            redirects = redirects.map(lambda r: (r['_id'], r['target']))
            articles.cache()

            # redirect set is typically too large to be broadcasted for a map-side join
            articles = articles\
                .flatMap(lambda pid_text_links: ((t, (pid_text_links[0], span)) for t, span in pid_text_links[1][1]))\
                .leftOuterJoin(redirects)\
                .map(lambda t_pid_span_r: (t_pid_span_r[0][0], (t_pid_span_r[1][1] if t_pid_span_r[1][1] else t_pid_span_r[0], t_pid_span_r[0][1])))\
                .groupByKey()\
                .mapValues(list)\
                .join(articles)\
                .map(lambda pid_links_text__: (pid_links_text__[0], (pid_links_text__[1][0], pid_links_text__[1][0])))

        return articles
