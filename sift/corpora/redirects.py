import urllib.request, urllib.parse, urllib.error
import ujson as json

from sift.dataset import Model, DocumentModel
from sift.util import trim_link_protocol, iter_sent_spans, ngrams

from sift import logging
log = logging.getLogger()

class MapRedirects(Model):
    """ Map redirects """
    def __init__(self, *args, **kwargs):
        self.from_path = kwargs.pop('from_path')
        self.to_path = kwargs.pop('to_path')

    def prepare(self, sc):
        return {
            "from_rds": self.load(sc, self.from_path).cache(),
            "to_rds": self.load(sc, self.to_path).cache()
        }

    @staticmethod
    def map_redirects(source, target):
        return source\
            .map(lambda s_t: (s_t[1], s_t[0]))\
            .leftOuterJoin(target)\
            .map(lambda t_s_r: (t_s_r[1][0], t_s_r[1][1] or t_s_r[0]))\
            .distinct()

    def build(self, from_rds, to_rds):
        # map source of destination kb
        # e.g. (a > b) and (a > c) becomes (b > c)
        mapped_to = to_rds\
            .leftOuterJoin(from_rds)\
            .map(lambda s_t_f: (s_t_f[1][1] or s_t_f[0], s_t_f[1][0]))\

        # map target of origin kb
        # e.g. (a > b) and (b > c) becomes (a > c)
        mapped_from = from_rds\
            .map(lambda s_t1: (s_t1[1], s_t1[0]))\
            .leftOuterJoin(mapped_to)\
            .map(lambda t_s_r2: (t_s_r2[1][0], t_s_r2[1][1]))\
            .filter(lambda s_t3: s_t3[1])

        rds = (mapped_from + mapped_to).distinct()
        rds.cache()

        log.info('Resolving transitive mappings over %i redirects...', rds.count())
        rds = self.map_redirects(rds, rds)

        log.info('Resolved %i redirects...', rds.count())
        return rds

    @staticmethod
    def load(sc, path, fmt=json):
        log.info('Using redirects: %s', path)
        return sc\
            .textFile(path)\
            .map(fmt.loads)\
            .map(lambda r: (r['_id'], r['target']))

    def format_items(self, model):
        return model\
            .map(lambda source_target: {
                '_id': source_target[0],
                'target': source_target[1]
            })

    @classmethod
    def add_arguments(cls, p):
        super(MapRedirects, cls).add_arguments(p)
        p.add_argument('from_path', metavar='FROM_REDIRECTS_PATH')
        p.add_argument('to_path', metavar='TO_REDIRECTS_PATH')
        return p

class RedirectDocuments(DocumentModel):
    """ Map links in a corpus via a set of redirects """
    def __init__(self, **kwargs):
        self.redirect_path = kwargs.pop('redirects_path')
        super(RedirectDocuments, self).__init__(**kwargs)

    def prepare(self, sc):
        params = super(RedirectDocuments, self).prepare(sc)
        params['redirects'] = self.load(sc, self.redirect_path).cache()
        return params

    def build(self, corpus, redirects):
        articles = corpus.map(lambda d: (d['_id'], d))

        def map_doc_links(doc, rds):
            for l in doc['links']:
                l['target'] = rds[l['target']]
            return doc

        return corpus\
            .map(lambda d: (d['_id'], set(l['target'] for l in d['links'])))\
            .flatMap(lambda pid_links: [(t, pid_links[0]) for t in pid_links[1]])\
            .leftOuterJoin(redirects)\
            .map(lambda t_pid_r: (t_pid_r[1][0], (t_pid_r[0], t_pid_r[1][1] if t_pid_r[1][1] else t_pid_r[0])))\
            .groupByKey()\
            .mapValues(dict)\
            .join(articles)\
            .map(lambda pid_rds_doc: map_doc_links(pid_rds_doc[1][1], pid_rds_doc[1][0]))

    def format_items(self, model):
        return model

    @classmethod
    def add_arguments(cls, p):
        super(RedirectDocuments, cls).add_arguments(p)
        p.add_argument('redirects_path', metavar='REDIRECTS_PATH')
        return p
