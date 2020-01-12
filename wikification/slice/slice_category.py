#! /usr/bin/env python3

"""
Simple script to get titles of wiki articles included in a category and its subcategories.
The results will be printed in the STDOUT. Each line contains title of an wiki article.
NOTE that the results probably have duplicate articles, originally tagged with multiple categories.
"""
import math
import sys

import wikipediaapi

wiki = wikipediaapi.Wikipedia(language='en', extract_format=wikipediaapi.ExtractFormat.WIKI)


def get_categorymembers(categorymembers,
                        max_level,
                        visited_articles,
                        visited_categories,
                        revisit_categories=False,
                        article_name=True,
                        category_name=True,
                        level=1,
                        out=sys.stdout,
                        ignore_special=True):
    pages = []
    subcats = []
    for cat in categorymembers.values():
        #  if article_name and not (ignore_special and cat.ns != wikipediaapi.Namespace.MAIN):
        if not (ignore_special and cat.ns != wikipediaapi.Namespace.MAIN):
            pages.append(cat)
            #  out.write(f"{cat.title}\n")
            visited_articles.add(cat.title)
        if cat.ns == wikipediaapi.Namespace.CATEGORY and level < max_level:
            if revisit_categories or cat.title not in visited_categories:
                subcats.append(cat)
    out.write(f'{len(pages)}\n')
    if article_name:
        for page in pages:
            out.write(f"{page.title}\n")
    for subcat in subcats:
        if category_name:
            out.write(f"{'*' * (level+1)}: {subcat.title}\n")
        visited_categories.add(subcat.title)
        visited_articles.union(
            get_categorymembers(subcat.categorymembers,
                                max_level=max_level,
                                visited_articles=visited_articles,
                                visited_categories=visited_categories,
                                revisit_categories=revisit_categories,
                                article_name=article_name,
                                category_name=category_name,
                                level=level+1,
                                out=out,
                                ignore_special=ignore_special)
        )
    return visited_articles


def get_pages_only(root_category, out=sys.stdout):
    root_cat_page = wiki.page(f"Category:{root_category}")
    return get_categorymembers(root_cat_page.categorymembers, max_level=math.inf, visited_articles=set(), visited_categories=set(), category_name=False, out=out)


def get_category_tree(root_category, max_level, out=sys.stdout):
    root_cat_page = wiki.page(f"Category:{root_category}")
    out.write(f"*: {root_cat_page.title}\n")
    return get_categorymembers(root_cat_page.categorymembers, max_level=max_level, visited_articles=set(), visited_categories=set(), out=out, article_name=False)


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
        '-t', '--tree',
        default=0,
        type=int,
        action='store',
        nargs='?',
        help='Prints out hierarchical tree information with the given category name. Pass the max depth as an argument to this.'
    )
    args = parser.parse_args()
    category = args.category.replace(' ', '_')
    if args.tree > 0:
        get_category_tree(category, max_level=args.tree)
    else:
        get_pages_only(category)
