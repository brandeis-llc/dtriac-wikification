"""
Provides helper functions for retrieving data from ES indices of wikipedia articles.
"""

from os.path import join as pjoin
from elasticsearch import Elasticsearch, helpers
from config import ES_HOST


def export_es_data(es, out_dir, index_name, field, verbose=1):
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
            value = doc['_source'].get(field)
            if value:
                f.write(value)
        if verbose > 0:
            if i % 1000 == 0:
                print(f"working on {i} documents!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )
    parser.add_argument(
        '-i', '--index',
        default='',
        action='store',
        nargs='?',
        help='ES index name.'
    )
    parser.add_argument(
        '-f', '--field',
        default='',
        action='store',
        nargs='?',
        help='Field name to retrieve.'
    )
    parser.add_argument(
        '-o', '--outname',
        default='',
        action='store',
        nargs='?',
        help='Directory name to put output. Retrieved field value will be saved with <_id>.txt in this directory. '
             '<_id> is the index number of the document in the ES index. '
    )
    args = parser.parse_args()
    es = Elasticsearch(ES_HOST)
    export_es_data(es, out_dir=args.outname, index_name=args.index, field=args.field)
