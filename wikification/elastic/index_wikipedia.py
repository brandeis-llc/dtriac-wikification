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
import glob
import json
from collections import Iterator
from urllib import request

from . import es
from elasticsearch import helpers as es_helpers


def load_title_index(title_index_filename):
    titles_f = open(title_index_filename)
    titles = {}
    for i, title in enumerate(titles_f, 1):
        titles[title.strip()] = i
    return titles


def delete_es_index(index_name):
    es.indices.delete(index=index_name, ignore=[400, 404])


def create_wikipedia_es_index(index_name):
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


def init_index(es_index_name):
    es_index_name = es_index_name.lower()
    delete_es_index(es_index_name)
    create_wikipedia_es_index(es_index_name)


def index_from_bulkfiles(bulk_file_prefix, es_index_name):
    for bulk_file in glob.glob(f"{bulk_file_prefix}*"):
        es.bulk(body=open(bulk_file), index=es_index_name)


def index_from_bulkiterator(documents: Iterator, es_index_name):
    """
    Note that bulkiterator coming from `slice` module should have es index name encoded in the json
    """
    es_helpers.bulk(es, documents, index=es_index_name)


