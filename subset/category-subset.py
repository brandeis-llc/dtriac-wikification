#! /usr/bin/env python3

"""
Simple script to get IDs and titles of wiki articles included in a category and its subcategories.
Usage: ./X.py "category_name"
The results will be printed in the STDOUT. Each line contains ID and title of an wiki article,
separated by a tab character.
"""

from typing import List
import re
from mwclient import Site

from mwclient.listing import Category
from mwclient.page import Page


def get_pages(category_name, visited_cats):
    enwiki = Site('en.wikipedia.org')
    pages = set()
    cat = enwiki.categories[category_name]
    for member in cat.members():
        if isinstance(member, Category):
            cat_name = member.name[9:]
            if cat_name not in visited_cats:
                visited_cats.add(cat_name)
                pages.update(get_pages(cat_name, visited_cats))
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
    import sys
    category = sys.argv[1]
    print_pages(get_pages(category, set()))
