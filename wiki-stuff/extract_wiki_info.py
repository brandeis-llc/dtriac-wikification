# extract_wiki_info.py

import argparse
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import re
import time

"""

Input: csv file listing all topics for ~250 pdf files (about 500 distinct topics);
    categories KB list all the high-level categories for each wiki article;
    texts KB is the wiki dump for October, 2016
    nif links KB contains all the outgoing links for each article;
    anchor texts KB contains all the ways each article is referred to in all other articles (will be helpful for coreference)


Output: Two json files

    (1) JSON file for all topics (basically reformatting of csv file)
    (2) JSON file for all associated wiki data (text, categories, links, anchor texts)

"""

"""

Sample command line:

python3 extract_wiki_info.py \
--topics_csv ../data/dtriac-wiki_topics.csv \
--categories_kb ../data/article_categories_en.ttl \
--texts_kb ~/wiki/extracted/ \
--nif_links_kb ../data/nif_text_links_en.ttl \
--anchor_texts_kb ../data/anchor_text_en.ttl

"""


def get_wiki_topics(wiki_topics_path):
    df = pd.read_csv(wiki_topics_path)

    data = {}

    for idx, row in df.iterrows():
        if not pd.isnull(row['wiki_topic1']):

            wiki_topics = [row.wiki_topic1]
            if not pd.isnull(row.wiki_topic2):
                wiki_topics.append(row.wiki_topic2)

            if not pd.isnull(row.wiki_topic3):
                wiki_topics.append(row.wiki_topic3)

            if not pd.isnull(row.wiki_topic4):
                wiki_topics.append(row.wiki_topic4)

            if not pd.isnull(row.title):
                title = row.title
            else:
                title = ''

            if not pd.isnull(row.year):
                year = row.year
            else:
                year = ''

            data[row.filename] = {"wiki_topics": wiki_topics, "title": title, "year": year}

    all_topics = []

    for k, v in data.items():
        all_topics.extend(v['wiki_topics'])

    all_topics = sorted(list(set(all_topics)))

    print("Number of topics:", len(all_topics))

    return data, all_topics


def get_wiki_categories(final_topic_obj, categories_path):
    all_topics = list(final_topic_obj.keys())

    with open(categories_path, 'r') as f:
        # file = f.readlines()
        for line in f:

            concept = line.split()[0].replace("<http://dbpedia.org/resource/", "")
            concept = concept[:-1]
            if concept in set(all_topics):
                category = line.split()[2].replace("<http://dbpedia.org/resource/Category:", "")
                category = category[:-1]
                final_topic_obj[concept]['categories'].append(category)

    return final_topic_obj


def get_wiki_text(final_topic_obj, wiki_text_path):
    all_topics = list(final_topic_obj.keys())

    # all_topics = [top.replace("_", " ") for top in all_topics]

    for root, dirs, files in os.walk(wiki_text_path):

        for file in files:

            infile = open(root + "/" + file, "r")
            contents = infile.read()
            soup = BeautifulSoup(contents, 'lxml')
            docs = soup.find_all('doc')

            for doc in docs:

                title = doc['title'].replace(" ", "_")

                if title in all_topics:
                    final_topic_obj[title]['text'] = doc.get_text().strip()
                # print(doc.get_text())
                # print()

    return final_topic_obj


def get_nif_links(final_topic_obj, links_path):
    all_topics = list(final_topic_obj.keys())

    with open(links_path, 'r') as f:
        # file = f.readlines()
        for line in f:

            if 'rdf#taIdentRef' in line:

                topic = re.sub(r'\?.+', '', line.split()[0])
                topic = topic.replace("<http://dbpedia.org/resource/", "")

                if topic in set(all_topics):

                    link = line.split()[2].replace("<http://dbpedia.org/resource/", "")
                    link = link[:-1]
                    if link not in set(final_topic_obj[topic]['out_links']):
                        final_topic_obj[topic]['out_links'].append(link)

    return final_topic_obj


def get_anchor_texts(final_topic_obj, anchors_path):
    all_topics = list(final_topic_obj.keys())

    with open(anchors_path, 'r') as f:

        for line in f:

            concept = line.split()[0].replace("<http://dbpedia.org/resource/", "")
            concept = concept[:-1]
            if concept in set(all_topics):

                pattern = r'^.*\"(.*)\".*$'

                match = re.search(pattern, line)

                anchor_text = match.group(1)

                if anchor_text not in set(final_topic_obj[concept]['anchor_texts']):
                    final_topic_obj[concept]['anchor_texts'].append(anchor_text)

    return final_topic_obj


def main():
    print()
    print("Getting info for all associated wiki topics (text, categories, links, etc)")
    print()

    parser = argparse.ArgumentParser(description='Retrieving info for wiki topics.')
    parser.add_argument('--topics_csv', required=True, type=os.path.abspath,
                        help='path to the wiki_topics csv file from Google Drive')
    parser.add_argument('--categories_kb', required=True, type=os.path.abspath,
                        help='path to the wiki categories KB')
    parser.add_argument('--texts_kb', required=True, type=os.path.abspath,
                        help='path to the wiki text KB')
    parser.add_argument('--nif_links_kb', required=True, type=os.path.abspath,
                        help='path to the wiki nif links KB')
    parser.add_argument('--anchor_texts_kb', required=True, type=os.path.abspath,
                        help='path to the wiki anchor texts KB')

    opt = parser.parse_args()
    print()
    print(opt)
    print()

    data, all_topics = get_wiki_topics(opt.topics_csv)

    # for k, v in data.items():
    # 	print(k, v)
    # print()

    final_topic_obj = {}

    for topic in all_topics:
        final_topic_obj[topic] = {'categories': [], 'text': '', 'anchor_texts': [], 'out_links': []}

    toc = time.time()
    final_topic_obj = get_wiki_categories(final_topic_obj, opt.categories_kb)
    tic = time.time()
    print("time for categories:", tic - toc)

    toc = time.time()
    final_topic_obj = get_wiki_text(final_topic_obj, opt.texts_kb)
    tic = time.time()
    print("time for texts:", tic - toc)

    toc = time.time()
    final_topic_obj = get_nif_links(final_topic_obj, opt.nif_links_kb)
    tic = time.time()
    print("time for links:", tic - toc)

    toc = time.time()
    final_topic_obj = get_anchor_texts(final_topic_obj, opt.anchor_texts_kb)
    tic = time.time()
    print("time for anchor texts:", tic - toc)

    for k, v in final_topic_obj.items():
        print(k, v)
        print()

    with open('dtriac_docids_with_wiki_topics.json', 'w') as g:
        json.dump(data, g, indent=4)

    with open('wiki_topics_with_metadata.json', 'w') as f:
        json.dump(final_topic_obj, f, indent=4)


if __name__ == '__main__':
    main()
