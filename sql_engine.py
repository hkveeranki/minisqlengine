"""
Main runner file for sql engine
"""

import sys

from util import read_meta

__author__ = 'harry-7'

METAFILE = 'metadata.txt'


def main():
    """ The Main function. Inititates the sql engine functioning"""
    table_data = read_meta(METAFILE)
    queries = str(sys.argv[1]).split(';')
    for query in queries:
        process_query(query, table_data)


def process_query(query, table_data):
    """Processes the given query and prints the output"""
    print query
    print table_data


if __name__ == '__main__':
    main()
