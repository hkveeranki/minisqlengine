"""
Contains various utility functions required for the mini Bash Shell
"""
__author__ = 'harry-7'


def read_meta(file_name):
    """ Reads the Metadata of the file and returns a dictionary containing the info about table"""
    meta_file = open(file_name, 'r')
    start = False
    table_name = ""
    table_data = {}
    for line in meta_file:
        line = line.strip()
        if line == '<begin_table>':
            start = True
        elif start:
            table_name = line
            table_data[table_name] = []
            start = False
        elif line != '<end_table>':
            table_data[table_name].append(line)
    return table_data
