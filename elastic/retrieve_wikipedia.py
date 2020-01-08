"""
Provides helper functions for retrieving data from ES indices of wikipedia articles.
"""

from os.path import join as pjoin
from elasticsearch import Elasticsearch, helpers


def export_es_data(es, out_dir, index_name, field):
    """
    export documents from ES index
    :param es: Elasticsearch object
    :param out_dir: folder to hold all the documents
    :param index_name: ES index name
    :param field: specify the field you want to export
    :return: a fold of txt files with the doc id as names ("[doc_id].txt")
    """
    index = helpers.scan(es, query={"query": {"match_all": {}}}, index=index_name)
    for i, doc in enumerate(index, 1):
        with open(pjoin(out_dir, f"{doc['_id']}.txt"), 'w') as f:
            abstract = doc['_source'].get(field)
            if abstract:
                f.write(abstract)
        if i % 1000 == 0:
            print(f"working on {i} documents!")


if __name__ == "__main__":
    es = Elasticsearch("http://tarski.cs-i.brandeis.edu:9200/")
    export_es_data(es, out_dir='wiki_data', index_name='enwiki-nuke_tech', field='opening_text')
    export_es_data(es, out_dir='dtra_data', index_name='dtriac-19d', field='text')