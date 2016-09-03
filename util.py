"""
Contains various utility functions required for the mini sql Shell
"""
import re
import sys
import csv

__author__ = 'harry-7'


def read_meta(file_name):
    """ Reads the Metadata of the file
    returns a dictionary containing the info about table
    """
    try:
        meta_file = open(file_name, 'r')
        start = False
        table_name = ""
        table_info = {}
        for line in meta_file:
            line = line.strip()
            if line == '<begin_table>':
                start = True
            elif start:
                table_name = line
                table_info[table_name] = []
                start = False
            elif line != '<end_table>':
                table_info[table_name].append(line)
        return table_info
    except IOError:
        error_exit('No metadata file \'' + file_name + '\' found')


def read_table_data(table_name):
    """ Reads the csv file data and returns it as a list"""
    data = []
    file_name = table_name + '.csv'
    try:
        data_file = open(file_name, 'rb')
        reader = csv.reader(data_file)
        for row in reader:
            data.append(row)
        data_file.close()
    except IOError:
        error_exit('No file for given table: \'' + table_name + '\' found')
    return data


def check_for(string, lis):
    """Checks whether string is in the list"""
    return string in lis


def format_string(string):
    """Returns the query in a formatted manner removing unnecessary spaces"""
    return (re.sub(' +', ' ', string)).strip()


def generate_header(table_name, columns):
    """Prints the header of the columns needed"""
    string = ''
    for column in columns:
        if string != '':
            string += ', '
        string += table_name + '.' + column

    return string


def display_output(tables_needed, columns_in_table, table_info, tables_data):
    """ Displays the output for a join operation without `where` clause"""
    if len(columns_in_table) == 2:
        table1 = tables_needed[0]
        table2 = tables_needed[1]
        header1 = generate_header(table1, columns_in_table[table1])
        header2 = generate_header(table2, columns_in_table[table2])
        print header1 + ', ' + header2
        for item in tables_data:
            for column in columns_in_table[table1]:
                print item[table_info[table1].index(column)],
            for column in columns_in_table[table2]:
                print item[table_info[table2].index(column) +
                           len(table_info[table1])],
            print

    else:
        for table in tables_needed:
            print generate_header(table, columns_in_table[table])
            for data in tables_data[table]:
                for column in columns_in_table[table]:
                    print data[table_info[table].index(column)],
                print
            print


def error_exit(error):
    """Prints the error to Stderr and exits the program"""
    sys.stderr.write(error + '\n')
    exit(-1)


def get_tables_columns(columns, tables, table_info):
    """ Selects required tables and columns in it"""
    columns_in_table = {}
    tables_needed = []

    for column in columns:
        if '.' in column:
            table, column = column.split('.')
            table = format_string(table)
            column = format_string(column)
            if table not in tables:
                error_exit('No Such table \'' + table + '\' exists')
            if table not in columns_in_table.keys():
                columns_in_table[table] = []
                tables_needed.append(table)
            columns_in_table[table].append(column)
            continue
        cnt = 0
        for table in tables:
            if column in table_info[table]:
                if cnt > 1:
                    error_exit('Abigous column name \'' + column + '\' given')
                if table not in columns_in_table.keys():
                    columns_in_table[table] = []
                    tables_needed.append(table)
                columns_in_table[table].append(column)
                cnt += 1
        if cnt == 0:
            error_exit('No such column \'' + column + '\' found')
    return columns_in_table, tables_needed
