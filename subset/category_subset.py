#! /usr/bin/env python3

"""
Simple script to get IDs and titles of wiki articles included in a category and its subcategories.
Usage: ./X.py "category_name"
The results will be printed in the STDOUT. Each line contains ID and title of an wiki article,
separated by a tab character.
"""
import json
import re
from queue import Queue
from typing import List
from urllib import request

from elasticsearch import Elasticsearch as ES
from elasticsearch import helpers as es_helpers
from mwclient import Site
from mwclient.listing import Category
from mwclient.page import Page

ES_HOST = 'localhost:9200'
HTTP_JSON_HEADER = {'Content-Type': 'application/json'}

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


def get_pages(category_name, visited_cats):
    enwiki = Site('en.wikipedia.org')
    pages = set()
    queue = Queue()
    queue.put(category_name)
    while not queue.empty():
        cat = enwiki.categories[queue.get()]
        for member in cat.members():
            if isinstance(member, Category):
                cat_name = member.name[9:]
                if cat_name not in visited_cats:
                    visited_cats.add(cat_name)
                    queue.put(cat_name)
            elif isinstance(member, Page):
                # don't want to include special pages, prefixed with mediawiki prefixes
                # such as "Template:XXX", "File:YYY", ...
                if not re.match(r'[A-Z][a-z]+:[^ ]', member.name):
                    pages.add(member)
        # print(f'({len(visited_cats):05}) {category_name}: {len(pages):05}')
    return pages


def print_pages(pages: List[Page]):
    # print("==================================")
    for page in pages:
        print(f'{page.pageid}\t{page.name}')


def generate_bulks(subset_name, titles, dump_file, es_index_name):
    """
    Function to generate bulk-ready (in terms of elasticsearch) files of a subset of wikipeida,
    given a list of pages in include in the subset and file-like object of a cirrus-format wiki dump
    :param subset_name: prefix to use to name result files
    :param pages: list of wiki article IDs (integer)
    :param dump_file: file-like object of wiki dump formatted for cirrussearch (obtainable from https://dumps.wikimedia.org/other/cirrussearch/)
    :return: Nothing returned, but bulk-index ready files will be generated, named after the subset name. Each file will contain 500 wiki articles.
    """

    bulk_size = 4

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
            source = cur_batch[item_num+1]
            meta['index']['_index'] = es_index_name
            meta['index']['_source'] = source
            yield meta['index']


    def index_batch_by_bulk(es_index_name):
        es = ES(ES_HOST)
        es_helpers.bulk(es, cur_batch_to_bulk_iterable(es_index_name))

    def process_batch():
        if es_index_name is None:
            write_batch_to_bulkfile()
        else:
            index_batch_by_bulk(es_index_name)


    if es_index_name is not None:
        delete_es_index(es_index_name)
        create_wikipedia_es_index(es_index_name)
    while len(titles) > 0:
        article_metadata_str = dump_file.readline()
        if article_metadata_str is None or len(article_metadata_str) < 2:
            print(f"{len(titles)} articles left in the subset but the dump file ended early. LEFT: ")
            print(titles)
            break
        article_metadata = json.loads(article_metadata_str)
        pagetype = article_metadata['index']['_type']
        if not pagetype == 'page':
            next(dump_file)
        else:
            article_contents_str = dump_file.readline()
            article_contents = json.loads(article_contents_str)
            title = article_contents['title']
            # try:
            #     qid = int(article_contents['wikibase_item'][1:])
            # except KeyError:
                # for some reason, the dump has no QID for this document
                # qid = title
            if title in titles:
                print(f"FOUND: {title}, ", end="", flush=True)
                titles.remove(title)
                cur_batch.append(article_metadata_str)
                cur_batch.append(article_contents_str)
                if len(cur_batch) >= bulk_size * 2:
                    process_batch()
                    cur_batch = []
                    cur_batch_num += 1
    if len(cur_batch) > 0:
        process_batch()


def get_pages_dicts(category_name):
    enwiki = Site('en.wikipedia.org')
    pages = {}
    cat = enwiki.categories[category_name]
    for member in cat.members():
        if isinstance(member, Category):
            cat_name = member.name[9:]
            pages.update(get_pages(cat_name))
        elif isinstance(member, Page):
            cur_pages = pages.get(category_name, [])
            cur_pages.append(member)
            print(member.name)
            pages[category_name] = cur_pages
    return pages


if __name__ == '__main__':
    import argparse
    import os
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )

    parser.add_argument(
        'category',
        metavar="CAT",
        type=str,
        action='store',
        help='A wikipedia (en) category to use for subsetting. IF <category>.txt (spaces replaced with \'_\') DOES exists in cwd, it will use it instead of querying wikipedia for pages.'
    )
    parser.add_argument(
        '-e', '--elasticsearch',
        required=False,
        default=None,
        action='store',
        nargs='?',
        help='Pass a ES index name to index to elastic search directly, instead of writing bulk-index ready files. Be careful as an existing ES index will be deleted.'
    )
    parser.add_argument(
        '-b', '--bulkdump',
        required=False,
        default=None,
        action='store',
        nargs='?',
        help='Pass a cirrus-format wiki dump file name to generate files for bulk-index to elasticsearch'
    )
    args = parser.parse_args()
    category = args.category.replace(' ', '_')
    category_txt_fname = f'{category}.txt'
    if args.bulkdump is None:
        print_pages(get_pages(category, set()))
    else:
        import gzip
        if os.path.exists(category_txt_fname):
            with open(category_txt_fname) as category_txt_f:
                print("using an existing page list")
                ids = list(map(int, map(lambda x: x.strip().split('\t')[1], [line for line in category_txt_f.readlines() if len(line) > 1])))
        else:
            print("retrieving a page list online")
            ids = [page.name for page in get_pages(category, set())]
        with gzip.open(args.bulkdump, 'r') as dump:
            generate_bulks(category, ids, dump, args.elasticsearch)
