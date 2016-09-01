"""
Main runner file for sql engine
"""

import sys

from util import read_meta, check_for, \
    error_exit, format_string, read_table_data

__author__ = 'harry-7'
METAFILE = 'metadata.txt'
FUNCTIONS = ['distinct', 'max', 'sum', 'average']


def main():
    """ The Main function. Initiates the sql engine functioning"""
    table_info = read_meta(METAFILE)
    queries = str(sys.argv[1]).split(';')
    for query in queries:
        process_query(query, table_info)


def process_query(query, table_info):
    """Processes the given query and prints the output"""
    query = format_string(query)
    function_process = []

    if not check_for('from', query.split()):
        error_exit("Syntax Error: No Table Selected")

    lis = query.split('from')
    project = format_string(str(lis[0]))
    remaining = format_string(str(lis[1]))

    if len(lis) != 2:
        error_exit("Syntax Error: More than one from statement given")
    if not check_for('select', project.lower().split()):
        error_exit("Syntax Error: No Select statement given")
    clauses = remaining.split('where')
    tables = format_string(clauses[0])
    tables = tables.split(',')
    tables_data = []
    for i in range(0, len(tables)):
        tables[i] = format_string(tables[i])
        if tables[i] not in table_info.keys():
            error_exit('No Such Table \'' + tables[i] + '\' Exists')
        tables_data.append(read_table_data(tables[i]))

    required = project[len('select '):]
    required = format_string(required)
    required = required.split(',')
    columns = []
    process_select(required, function_process, columns)

    if len(clauses) > 1 and len(tables) == 1:
        # Single table where condition
        if len(function_process) != 0:
            error_exit('ERROR:Where Condition can '
                       'only be given to project functions')
        process_where(clauses[1], columns, tables, table_info, tables_data)
    elif len(clauses) > 1 and len(tables) > 1:
        # Join Condition
        process_where_join(clauses[1], columns,
                           tables, table_info, tables_data)
    elif len(function_process) != 0:
        for query in function_process:
            process_function(query[0], query[1],
                             tables, table_info, tables_data)
    elif len(tables) > 1:
        process_join(columns, tables, table_info, tables_data)
    else:
        process_project(columns, tables, table_info, tables_data)


def process_select(required, function_process, columns):
    """Process the select part of the query and return tokens"""
    column_name = ''
    for thing in required:
        taken = False
        thing = format_string(thing)
        for function in FUNCTIONS:
            if function + '(' in thing.lower():
                taken = True
                if ')' not in thing:
                    error_exit('Syntax Error: \')\' expected ')
                else:
                    column_name = thing.strip(')').split('distinct(')[1]
                function_process.append([function, column_name])
                break
        if not taken:
            columns.append(thing.strip('()'))


def process_where(condition, columns, tables, table_info, tables_data):
    """ Process where clause on a single table"""
    condition = format_string(condition)
    # print 'I am in', sys._getframe().f_code.co_name
    print condition
    print columns
    print tables
    print table_info


def process_where_join(condition, columns, tables, table_info, tables_data):
    """Deals with Join type queries"""
    condition = format_string(condition)
    # print 'I am in', sys._getframe().f_code.co_name
    print condition
    print columns
    print tables
    print table_info


def process_join(columns, tables, table_info, tables_data):
    """Deals with join without where"""

    # print 'I am in', sys._getframe().f_code.co_name
    print columns
    print tables
    print table_info


def process_function(function_name, column_name, tables,
                     table_info, tables_data):
    """Deals with aggregate functions and distinct"""
    # print 'I am in', sys._getframe().f_code.co_name
    print function_name
    print column_name
    print tables
    print table_info


def process_project(columns, tables, table_info, tables_data):
    """ Deals with project operation without where condition"""
    # print 'I am in', sys._getframe().f_code.co_name
    print columns
    print tables
    print table_info


if __name__ == '__main__':
    main()
