import pandas as pd
import numpy as np

import json
from collections import defaultdict, Counter


def title_year2json(file_name, out_name):
    csv_df = pd.read_csv(file_name)
    title_year = csv_df[['title', 'year']]
    years = Counter([year.split()[-1] for year in title_year['year'].values if isinstance(year, str)])
    graph = defaultdict(list)
    for year, count in years.items():
        graph['nodes'].append({'id': year, 'group': 'anchor', 'val': str(count)})
    for pair in title_year.values:
        if isinstance(pair[1], str):
            graph['nodes'].append({'id': pair[0], 'group': 'title', 'val': str(1)})
            graph['links'].append({"source": pair[0], "target": pair[1], "val": 1})

    with open(out_name, 'w', encoding='utf8') as fh:
        fh.write(json.dumps(graph, sort_keys=True, indent=4))


if __name__ == "__main__":
    wiki_topics_csv = 'new_dtriac_wiki_topics.csv'
    out_json = 'wiki_graph.json'
    title_year2json(wiki_topics_csv, out_json)
