"""
Main runner file for sql engine
"""

import sys
import re

from util import read_meta

__author__ = 'harry-7'
METAFILE = 'metadata.txt'


def main():
    """ The Main function. Inititates the sql engine functioning"""
    table_info = read_meta(METAFILE)
    queries = str(sys.argv[1]).split(';')
    for query in queries:
        process_query(query, table_info)


def process_query(query, table_info):
    """Processes the given query and prints the output"""
    query = format_string(query)
    lis = []
    if check_for('from', query.split()):
        lis = query.split('from')
    else:
        error_exit("Syntax Error: No Table Selected")
    project = format_string(str(lis[0]))
    condition = format_string(lis[1])

    if not check_for('select', project.split().lower()):
        error_exit("Syntax Error: No Select statement given")


def check_for(string, lis):
    """Checks whether string is in the list"""
    return string in lis


def format_string(string):
    """Returns the query in a formatted manner removing unnecessary spaces"""
    return (re.sub(' +', ' ', string)).strip()


def error_exit(error):
    """Prints the error to Stderr and exits the program"""
    sys.stderr.write(error)
    exit(-1)


if __name__ == '__main__':
    main()
