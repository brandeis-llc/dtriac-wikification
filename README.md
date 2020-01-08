# Slicing Wikipedia 

Code here is for slicing wikipedia and indexing the slice into local elasticsearch index. 

## Requirments 

1. A Wikipedia cirrus-format dump
1. Local elasticsearch instance
1. Python dependencies as specified in `requirements.txt`


## Slicing 

Currently only slicing by category name is supported. 

### By category 

By slicing by a category, you'd get a subset of wikipedia articles that fall under any of subcategories of it including itself. 

1. First, we need to get a list of wikipedia article titles under a category name. This can be done using `sclive/slice_category` script 
    1. This script can also be used to print out category hierarchy. See help message for details. 
to generate an ordered list of titles of documents from a cirrus wikidump. 
1. Once we have the list (note that the list printed out from `slice_category` script is not sort/uniq-ed), next use `slice/slice_dump` script actually slice a cirrus-format wikipedia dump. The slice can be stored as files with ES-ready json objects, or can stay in memory as a python generator. Also see the help message for details. 

## Indexing to Elasticsearch

Use `elastic/index_wikipedia` contains two functions to index two types of a slice of wikidump generated from `slice/slice_dump` script. However, this script does not have CLI, thus instead you should consider using `./slice_and_index` script if you want to index directly from command line. See help message for details. 

