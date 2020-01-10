import gzip
import json

BULK_SIZE = 1000
def slice(bulk_prefix, slice_titles, dump_file):
    """
    Function to generate bulk-ready files of a slice of wikipeida, given a list of pages to include in the slice and
    file-like object of a cirrus-format wiki dump. When a name of elasticsearch index is given, the slices will be
    directly indexed instead of be output to bulk files.
    """

    print(f"SLICING {len(slice_titles)} articles")

    tot_batch_num = len(slice_titles) // BULK_SIZE
    batch_digits = len(str(tot_batch_num))
    cur_batch_num = 0
    cur_batch = []

    def get_bulk_fname():
        return f'{bulk_prefix}{cur_batch_num:0{batch_digits}}'

    def write_batch_to_bulkfile():
        print(f"WRITING BATCH {cur_batch_num}/{tot_batch_num}")
        with open(get_bulk_fname(), 'wb') as bulk_file:
            for line in cur_batch:
                if isinstance(line, str):
                    line = line.encode()
                bulk_file.write(line)

    def remove_if_has(l, i):
        if i in l:
            l.remove(i)

    if isinstance(dump_file, str):
        if dump_file.endswith('.gz'):
            dump_file = gzip.open(dump_file, 'r')
        elif dump_file.endswith('.json'):
            dump_file = open(dump_file, 'r')

    while len(slice_titles) > 0:
        article_metadata_str = dump_file.readline()
        if article_metadata_str is None or len(article_metadata_str) < 2:
            print(f"{len(slice_titles)} articles left in the subset but the dump file ended early. LEFT: ")
            print(slice_titles)
            break

        article_metadata = json.loads(article_metadata_str)
        pagetype = article_metadata['index']['_type']
        pageid = article_metadata['index']['_id']
        if not pagetype == 'page' or not pageid.isnumeric():
            next(dump_file)
            continue
        article_contents_str = dump_file.readline()
        article_contents = json.loads(article_contents_str)
        if article_contents['namespace'] == 0:
            title = None
            article_title = article_contents.get('title')
            article_aliases = [redirect['title'] for redirect in article_contents['redirect'] if redirect.get('namespace') == 0]
            if article_title in slice_titles:
                title = article_title
            else:
                for alias in article_aliases:
                    if alias in slice_titles:
                        title = alias
                        break
            if title is not None:
                print(f"FOUND: {title}, ", end="", flush=True)
                remove_if_has(slice_titles, title)
                map(lambda x: remove_if_has(slice_titles, x), article_aliases)
                if bulk_prefix is None:
                    article_metadata['index']['_source'] = article_contents
                    yield article_metadata['index']
                else:
                    cur_batch.append(article_metadata_str)
                    cur_batch.append(article_contents_str)
                    if len(cur_batch) >= BULK_SIZE * 2:
                        write_batch_to_bulkfile()
                        cur_batch = []
                        cur_batch_num += 1

    if len(cur_batch) > 0:
        write_batch_to_bulkfile()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )
    parser.add_argument(
        '-d', '--dump',
        required=True,
        action='store',
        nargs='?',
        help='Pass a cirrus-format wiki dump file name to generate files for bulk-index to elasticsearch.'
             'Cirrus dump files are distributed with names of `enwiki-DATE-cirrussearch-content.json.gz`,'
             'but this script can only take `.json` (uncompressed) file. '
             'Also, the dump must be accompanied (in the same directory) by a corresponding `title_index_file`, '
             'and the `title_index_file` must be named as `enwiki-DATE-cirrussearch-titles.txt` '
             '(DATE must be the same).'
    )
    parser.add_argument(
        '-s', '--slice',
        required=True,
        action='store',
        nargs='?',
        help='Specify a wikipedia slice for elasticsearch indexing. '
             'Specification is done by simply listing all article titles in a file, one title at a line. '
    )
    parser.add_argument(
        '-f', '--file',
        action='store_true',
        help='If given, the bulk json files will be saved as files, using the name of slice as prefix.'
             f'Each file will include {BULK_SIZE} articles. '
    )
    args = parser.parse_args()
    dump_name = args.dump
    title_index_name = dump_name.replace('-content.json', '-titles.txt')
    slice(f'{args.slice}-bulk-' if args.file else None,
          [title.strip() for title in open(args.slice)], dump_name)
