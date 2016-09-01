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


def print_header(table_name, columns, table_info):
    """Prints the header of the columns needed"""
    string = ''
    for column in columns:
        if string != '':
            string += ', '
        string += table_name + '.' + table_info[table_name][column]

    print string


def error_exit(error):
    """Prints the error to Stderr and exits the program"""
    sys.stderr.write(error + '\n')
    exit(-1)
