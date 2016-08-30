"""
Contains various utility functions required for the mini Bash Shell
"""
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
