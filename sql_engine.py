"""
Main runner file for sql engine
"""

import sys

from util import read_meta, check_for, \
    error_exit, format_string, read_table_data, generate_header

__author__ = 'harry-7'
METAFILE = 'metadata.txt'
FUNCTIONS = ['distinct', 'max', 'sum', 'avg', 'min']


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
    distinct_process = []

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
    tables_data = {}
    for i in range(0, len(tables)):
        tables[i] = format_string(tables[i])
        if tables[i] not in table_info.keys():
            error_exit('No Such Table \'' + tables[i] + '\' Exists')
        tables_data[tables[i]] = read_table_data(tables[i])

    required = project[len('select '):]
    required = format_string(required)
    required = required.split(',')
    columns = []
    process_select(required, function_process, distinct_process, columns)

    if len(clauses) > 1 and len(tables) == 1:
        # Single table where condition
        if len(function_process) != 0 or len(distinct_process) != 0:
            error_exit('ERROR:Where Condition can '
                       'only be given to project functions')
        process_where(clauses[1], columns, tables[0], table_info, tables_data[tables[0]])
    elif len(clauses) > 1 and len(tables) > 1:
        if len(function_process) != 0 or len(distinct_process) != 0:
            error_exit('ERROR:Where Condition can '
                       'only be given to project functions')
        process_where_join(clauses[1], columns,
                           tables, table_info, tables_data)
    elif len(function_process) != 0:
        process_aggregate(function_process, tables, table_info, tables_data)
    elif len(distinct_process) != 0:
        pass
    elif len(tables) > 1:
        process_join(columns, tables, table_info, tables_data)
    else:
        process_project(columns, tables[0], table_info, tables_data)


def process_select(required, function_process, distinct_process, columns):
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
                    column_name = thing.strip(')').split(function + '(')[1]
                if function == 'distinct':
                    distinct_process.append([function, column_name])
                else:
                    function_process.append([function, column_name])
                break
        if not taken:
            columns.append(thing.strip('()'))


def generate_evaluator(condition, table, table_info, data):
    """Generates the evaluator string for single table where"""
    condition = condition.split(' ')
    string = ''
    for i in condition:
        if i == '=':
            string += i * 2
        elif i.lower() == 'and' or i.lower == 'or':
            string += ' ' + i.lower() + ' '
        elif i in table_info[table]:
            string += data[table_info[table].index(i)]
        else:
            string += i
    return string


def process_where_join(condition, columns, tables, table_info, tables_data):
    """Deals with Join type queries"""
    condition = format_string(condition)
    # print 'I am in', sys._getframe().f_code.co_name
    print condition
    print columns
    print tables
    print table_info
    print tables_data


def process_join(columns, tables, table_info, tables_data):
    """Deals with Join type queries"""

    columns_in_table = {}

    for column in columns:
        if '.' in column:
            table, column = column.split('.')
            if table not in tables:
                error_exit('No Such table \'' + table + '\' exists')
            columns_in_table[table].append(table_info[table].index(column))
            continue
        cnt = 0
        for table in tables:
            if column in table_info[table]:
                if cnt > 1:
                    error_exit('Abigous column name \'' + column + '\' given')
                columns_in_table[table].append(table_info[table].index(column))
                cnt += 1
        if cnt == 0:
            error_exit('No such column \'' + column + '\' found')

    for table in tables:
        if len(columns_in_table[table]) == 0:
            continue
        print generate_header(table, columns_in_table[table])
        for data in tables_data[table]:
            for column in columns_in_table[table]:
                print data[column],
            print
        print


def process_where(condition, columns, table, table_info, table_data):
    """ Process where clause on a single table"""
    condition = format_string(condition)

    if len(columns) == 1 and columns[0] == '*':
        columns = table_info[table]
    print generate_header(table, columns)

    for row in table_data:
        string = generate_evaluator(condition, table, table_info, row)
        if eval(string):
            for column in columns:
                print row[table_info[table].index(column)],
            print


def process_aggregate(queries, tables,
                      table_info, tables_data):
    """Deals with aggregate functions and distinct"""
    header, result = '', ''
    for query in queries:
        function_name = query[0]
        column_name = query[1]
        table, column = '', ''
        if '.' in column_name:
            table, column = column_name.split('.')
        else:
            cnt = 0
            for tab in tables:
                if column_name in table_info[tab]:
                    if cnt > 1:
                        error_exit('Abigous column name \'' + column_name + '\' given')
                    table = tab
                    column = column_name
                    cnt += 1
            if cnt == 0:
                error_exit('No such column \'' + column_name + '\' found')

        data = []
        if header != '':
            header += ' , '
        header += table + '.' + column
        for row in tables_data[table]:
            data.append(int(row[table_info[table].index(column)]))

        if function_name.lower() == 'max':
            result += str(max(data))
        elif function_name.lower() == 'min':
            result += str(min(data))
        elif function_name.lower() == 'sum':
            result += str(sum(data))
        elif function_name.lower() == 'avg':
            result += str(float(sum(data)) / len(data))
        result += ' '
    print header
    print result


def process_project(columns, table, table_info, tables_data):
    """ Deals with project operation without where condition in a single table"""
    if len(columns) == 1 and columns[0] == '*':
        columns = table_info[table]
    for column in columns:
        if column not in table_info[table]:
            error_exit('No Such column \'' + column +
                       '\' found in the given table \'' + table + '\' ')
    print generate_header(table, columns)

    for data in tables_data[table]:
        for column in columns:
            print data[table_info[table].index(column)],
        print


if __name__ == '__main__':
    main()
