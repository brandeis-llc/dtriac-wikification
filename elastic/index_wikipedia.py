#! /usr/bin/env python3

"""
Script to index a subset of wikipedia cirrus dump into a local ES index.
The subset can be specified as a list of titles of wiki articles.
For the moment, the script only processes regulat main wiki articles (namespace=0).

It requires a specially formatted `title_index_file`. As in a cirrus-format wiki dump file,
a wiki document is represented in two lines (one with metadata and another with document contents).
The `title_index_file` is simply a list of titles (one at a line) of the documents in a cirrus dump file,
ordered in the same order of appearance in the original dump.
"""
import gzip
import json
import subprocess
from urllib import request
from config import ES_HOST

from elasticsearch import Elasticsearch as ES
from elasticsearch import helpers as es_helpers


def load_title_index(title_index_filename):
    titles_f = open(title_index_filename)
    titles = {}
    for i, title in enumerate(titles_f, 1):
        titles[title.strip()] = i
    return titles


def delete_es_index(index_name):
    es = ES(ES_HOST)
    es.indices.delete(index=index_name, ignore=[400, 404])


def create_wikipedia_es_index(index_name):
    es = ES(ES_HOST)
    settings_dump_url = 'https://en.wikipedia.org/w/api.php?action=cirrus-settings-dump&format=json&formatversion=2'
    settings = json.loads((request.urlopen(settings_dump_url).read()))
    mappings_dump_url = 'https://en.wikipedia.org/w/api.php?action=cirrus-mapping-dump&format=json&formatversion=2'
    mappings = json.loads((request.urlopen(mappings_dump_url).read()))

    configurations = {
        'settings': {
            'index': {
                'analysis': settings['content']['page']['index']['analysis'],
                'similarity': settings['content']['page']['index']['similarity']
            }
        },
        'mappings':  {'page': mappings['content']['page']}
    }
    es.indices.create(index_name, body=configurations)


def get_line(filename, line_num):
    return subprocess.check_output(['sed', f'{line_num}q;d', filename])


def slice_and_index(subset_name, titles, dump_file, es_index_name):
    """
    Function to generate bulk-ready files of a slice of wikipeida, given a list of pages to include in the slice and
    file-like object of a cirrus-format wiki dump. When a name of elasticsearch index is given, the slices will be
    directly indexed instead of be output to bulk files.
    """

    bulk_size = 1000

    tot_batch_num = len(titles) // bulk_size
    batch_digits = len(str(tot_batch_num))
    cur_batch_num = 0
    cur_batch = []

    def get_bulk_fname():
        return f'{subset_name}-{cur_batch_num:0{batch_digits}}'

    def write_batch_to_bulkfile():
        print(f"WRITING BATCH {cur_batch_num}/{tot_batch_num}")
        with open(get_bulk_fname(), 'wb') as bulk_file:
            for line in cur_batch:
                if isinstance(line, str):
                    line = line.encode()
                bulk_file.write(line)

    def cur_batch_to_bulk_iterable(es_index_name):
        print(f"INDEXING BATCH {cur_batch_num}/{tot_batch_num}")
        for item_num in range(0, len(cur_batch), 2):
            meta = json.loads(cur_batch[item_num])
            source = json.loads(cur_batch[item_num+1])
            meta['index']['_index'] = es_index_name
            meta['index']['_source'] = source
            yield meta['index']

    def index_batch_by_bulk(es_index_name):
        es = ES(ES_HOST, timeout=30, max_retries=10, retry_on_timeout=True)
        es_helpers.bulk(es, cur_batch_to_bulk_iterable(es_index_name))

    def process_batch():
        if es_index_name is None:
            write_batch_to_bulkfile()
        else:
            index_batch_by_bulk(es_index_name)

    def remove_if_has(l, i):
        if i in l:
            l.remove(i)

    if es_index_name is not None:
        es_index_name = es_index_name.lower()
        delete_es_index(es_index_name)
        create_wikipedia_es_index(es_index_name)

    if isinstance(dump_file, str):
        if dump_file.endswith('.gz'):
            dump_file = gzip.open(dump_file, 'r')
        elif dump_file.endswith('.json'):
            dump_file = open(dump_file, 'r')

    while len(titles) > 0:
        article_metadata_str = dump_file.readline()
        if article_metadata_str is None or len(article_metadata_str) < 2:
            print(f"{len(titles)} articles left in the subset but the dump file ended early. LEFT: ")
            print(titles)
            break
        article_metadata = json.loads(article_metadata_str)
        pagetype = article_metadata['index']['_type']
        pageid = article_metadata['index']['_id']
        if not pagetype == 'page' or not pageid.isnumeric():
            next(dump_file)
            continue
        article_contents_str = dump_file.readline()
        article_contents = json.loads(article_contents_str)
        if article_contents['namespace'] == 0:
            title = None
            article_title = article_contents.get('title')
            article_aliases = [redirect['title'] for redirect in article_contents['redirect'] if redirect.get('namespace') == 0]
            if article_title in titles:
                title = article_title
            else:
                for alias in article_aliases:
                    if alias in titles:
                        title = alias
                        break
            if title is not None:
                print(f"FOUND: {title}, ", end="", flush=True)
                remove_if_has(titles, title)
                map(lambda x: remove_if_has(titles, x), article_aliases)
                cur_batch.append(article_metadata_str)
                cur_batch.append(article_contents_str)
                if len(cur_batch) >= bulk_size * 2:
                    process_batch()
                    cur_batch = []
                    cur_batch_num += 1

    if len(cur_batch) > 0:
        process_batch()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )
    parser.add_argument(
        '-e', '--esindex',
        required=False,
        default=None,
        action='store',
        nargs='?',
        help='Pass a ES index name to index to elastic search directly instead of writing bulk-index ready files. '
             'Be careful as an existing ES index will be deleted.'
    )
    parser.add_argument(
        '-d', '--dump',
        required=True,
        action='store',
        nargs='?',
        help='Pass a cirrus-format wiki dump file name to generate files for bulk-index to elasticsearch.'
             'Cirrus dump files are distributed with names of `enwiki-DATE-cirrussearch-content.json.gz`,'
             'but this script can only take `.json` (uncompressed) file. '
             'Also, the dump must be accompanied (in the same directory) by a corresponding `title_index_file`, '
             'and the `title_index_file` must be named as `enwiki-DATE-cirrussearch-titles.txt` '
             '(DATE must be the same).'
    )
    parser.add_argument(
        '-s', '--slice',
        required=True,
        action='store',
        nargs='?',
        help='Specify a wikipedia slice for elasticsearch indexing. '
             'Specification is done by simply listing all article titles in a file, one title at a line. '
    )
    args = parser.parse_args()
    dump_name = args.dump
    title_index_name = dump_name.replace('-content.json', '-titles.txt')
    slice_and_index(f'{args.slice}-bulk', [title.strip() for title in open(args.slice)], dump_name, args.esindex)
