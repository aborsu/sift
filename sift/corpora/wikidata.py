import ujson as json

from sift.corpora import wikicorpus
from sift.dataset import ModelBuilder, Model, Relations

from sift import logging
log = logging.getLogger()

ENTITY_PREFIX = 'Q'
PREDICATE_PREFIX = 'P'

class WikidataCorpus(ModelBuilder, Model):
    @staticmethod
    def iter_item_for_line(line):
        line = line.strip()
        if line != '[' and line != ']':
            yield json.loads(line.rstrip(',\n'))

    def build(self, sc, path):
        return sc\
            .textFile(path)\
            .flatMap(self.iter_item_for_line)\
            .map(lambda i: (i['id'], i))

    @staticmethod
    def format_item(xxx_todo_changeme):
        (wid, item) = xxx_todo_changeme
        return {
            '_id': wid,
            'data': item
        }

class WikidataRelations(ModelBuilder, Relations):
    """ Prepare a corpus of relations from wikidata """
    @staticmethod
    def iter_relations_for_item(item):
        for pid, statements in item.get('claims', {}).items():
            for statement in statements:
                if statement['mainsnak'].get('snaktype') == 'value':
                    datatype = statement['mainsnak'].get('datatype')
                    if datatype == 'wikibase-item':
                        yield pid, int(statement['mainsnak']['datavalue']['value']['numeric-id'])
                    elif datatype == 'time':
                        yield pid, statement['mainsnak']['datavalue']['value']['time']
                    elif datatype == 'string' or datatype == 'url':
                        yield pid, statement['mainsnak']['datavalue']['value']

    def build(self, corpus):
        entities = corpus\
            .filter(lambda item: item['_id'].startswith(ENTITY_PREFIX))

        entity_labels = entities\
            .map(lambda item: (item['_id'], item['data'].get('labels', {}).get('en', {}).get('value', None)))\
            .filter(lambda pid_label: pid_label[1])\
            .map(lambda pid_label1: (int(pid_label1[0][1:]), pid_label1[1]))

        wiki_entities = entities\
            .map(lambda item: (item['data'].get('sitelinks', {}).get('enwiki', {}).get('title', None), item['data']))\
            .filter(lambda e__: e__[0])\
            .cache()
       
        predicate_labels = corpus\
            .filter(lambda item: item['_id'].startswith(PREDICATE_PREFIX))\
            .map(lambda item: (item['_id'], item['data'].get('labels', {}).get('en', {}).get('value', None)))\
            .filter(lambda pid_label2: pid_label2[1])\
            .cache()

        relations = wiki_entities\
            .flatMap(lambda eid_item: ((pid, (value, eid_item[0])) for pid, value in self.iter_relations_for_item(eid_item[1])))\
            .join(predicate_labels)\
            .map(lambda pid_value_eid_label: (pid_value_eid_label[0][0], (pid_value_eid_label[1][1], pid_value_eid_label[0][1])))

        return relations\
            .leftOuterJoin(entity_labels)\
            .map(lambda value_label_eid_value_label: (value_label_eid_value_label[0][1], (value_label_eid_value_label[0][0], value_label_eid_value_label[1][1] or value_label_eid_value_label[0])))\
            .groupByKey()\
            .mapValues(dict)
