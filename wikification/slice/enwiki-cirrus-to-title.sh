#! /bin/bash

DUMP=/data/random-dataset-trunk/wikidumps/enwiki-20191223-cirrussearch-content.json
TITLES_ONLY=/data/random-dataset-trunk/wikidumps/enwiki-20191223-cirrussearch-titles.json

cat $DUMP | sed -n 'n;p' | jq -r '. |.title' | tee $TITLES_ONLY
