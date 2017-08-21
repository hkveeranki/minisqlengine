"""
Main runner file for sql engine
"""
import sys

from QueryProcessor import QueryProcessor
from utility_functions import read_meta

__author__ = 'harry-7'
METAFILE = 'metadata.txt'


def main():
    """ The Main function. Initiates the sql engine functioning"""
    queries = str(sys.argv[1]).split(';')
    query_processor = QueryProcessor(read_meta(METAFILE))
    for query in queries:
        if query != '':
            query_processor.process_query(query)


if __name__ == '__main__':
    main()
