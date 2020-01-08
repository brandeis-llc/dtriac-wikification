from slice import slice_category, slice_dump
from elastic import index_wikipedia

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )
    parser.add_argument(
        '-c', '--category',
        action='store',
        nargs='?',
        help='Root category name'
    )
    parser.add_argument(
        '-d', '--dump',
        action='store',
        nargs='?',
        help='Cirrus dump file name'
    )
    parser.add_argument(
        '-i', '--esindex',
        action='store',
        nargs='?',
        help='ES index name'
    )
    args = parser.parse_args()
    index_wikipedia.init_index(args.esindex)
    slice_titles = list(slice_category.get_pages_only(args.category))
    index_wikipedia.index_from_bulkiterator(
        slice_dump.slice(None, slice_titles, args.dump),
        args.esindex
    )

