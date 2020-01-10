from .. import elastic
from elasticsearch_dsl import Search
from . import Wikifier


class WikifyByES(Wikifier):
    def __init__(self, es_index_name):
        self.index_name = es_index_name
        self.es = elastic.es

    def wikify(self, text):
        q = Search(using=self.es, index=self.index_name).query("match", text=text)
        hits = q.execute()
        try:
            return [{'title': hit.title, 'score': hit.meta.score} for hit in hits[0:min(self.wikification_size, len(hits))]]
        except IndexError:
            return None

