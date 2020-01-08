import json
import csv
import requests
from collections import defaultdict

URL = 'http://tarski.cs-i.brandeis.edu:9200/dtriac-19d/_doc/'
QUERY_URL = 'http://tarski.cs-i.brandeis.edu:9200/dtriac-19d/_search/'

with open('wiki_topics_with_metadata.json') as f:
    data = json.load(f)

posts = {}

with open('new_dtriac_wiki_topics.csv','r') as f:
    reader = csv.DictReader(f)
    wiki2id = defaultdict(str)
    for row in reader:
        tmp_categories = []
        tmp_text = ""
        tmp_articles = []
        for x in range(1,5):
            if len(row['wiki_topic'+str(x)]) > 0:
                # wiki2id[row['wiki_topic'+str(x)] = row['docid']
                tmp_categories.extend(data[row['wiki_topic'+str(x)]]['categories'])
                tmp_articles.append(row['wiki_topic'+str(x)])
                tmp_text += data[row['wiki_topic'+str(x)]]['text']
        posts[row['docid']] = {'doc': {'wiki_articles':tmp_articles, 'wiki_categories':tmp_categories, 'wiki_text':tmp_text}}

for article in posts.keys():
    query = article+'/tesseract-300dpi-20p.txt'
    response = requests.get(QUERY_URL, json={'query':{'match':{'docname':{'query':query, 'fuzziness':0}}}})
    response = json.loads(response.content)
    article_id = response['hits']['hits'][0]['_id']
    requests.post(URL+article_id+'/_update', json=posts[article])
