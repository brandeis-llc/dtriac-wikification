# Slicing Wikipedia 

Code here is for slicing wikipedia and indexing the slice into local elasticsearch index. 

## Requirments 

1. A Wikipedia cirrus-format dump
1. Local elasticsearch instance


## Slicing 

Currently only slicing by category name is supported. 

### By category 

Slicing by a category, you'd get a subset of wikipedia articles that fall under any of subcategories of it including itself. 

1. First, use `wiki-cirrus-to-title.sh` script to generate an ordered list of titles of documents from a cirrus wikidump. 
1. Use `slice_category.py` script to slice the dump file and generate a subset ready for indexing into an ES index. Two types of output is supported. 
    1. Write to files (N articles ina file, as configured in the python script): to index with any REST-ful requests
    1. Output as a Python generator: to index with `elasticsearch-py` 


