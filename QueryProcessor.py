"""
Code for QueryProcessor class and functions to assist.
"""

from utility_functions import check_for_string, error_exit, format_string, \
    read_table_data, generate_header, display_output, join_needed_data

__author__ = 'harry-7'
FUNCTIONS = ['distinct', 'max', 'sum', 'avg', 'min']


class QueryProcessor:
    """Class to deal with query processing"""

    def __init__(self, tables_info):
        """
        Default constructor
        :param tables_info: metadata about the tables present 
        """
        self.tables_info = tables_info

    def process_query(self, query):
        """Processes the given query and prints the output"""
        query = format_string(query)

        if not check_for_string('from', query.split()):
            error_exit("Syntax Error: No Table Selected")

        check_errors_in_select(query)
        lis = query.split('from')
        project = format_string(str(lis[0]))
        remaining = format_string(str(lis[1]))
        clauses = remaining.split('where')
        tables = format_string(clauses[0])
        tables = tables.split(',')
        tables_data = {}
        for i in range(0, len(tables)):
            tables[i] = format_string(tables[i])
            if tables[i] not in self.tables_info.keys():
                error_exit('No Such Table \'' + tables[i] + '\' Exists')
            tables_data[tables[i]] = read_table_data(tables[i])

        required = project[len('select '):]
        required = format_string(required)
        required = required.split(',')

        columns, function_process, distinct_process = process_select(required)
        check_errors_in_clauses(clauses, columns, function_process,
                                distinct_process)
        self.execute_query(clauses, tables, tables_data, columns,
                           function_process, distinct_process)

    def execute_query(self, clauses, tables, tables_data, columns,
                      function_process, distinct_process):
        """ Decides the type of query and appropriately processes it"""
        if len(clauses) > 1 and len(tables) == 1:
            # Single table where condition
            self.process_where(clauses[1], columns,
                               tables[0], tables_data[tables[0]])
        elif len(clauses) > 1 and len(tables) > 1:
            self.process_where_join(clauses[1], columns,
                                    tables, tables_data)
        elif len(function_process) != 0:
            self.process_aggregate(function_process, tables, tables_data)
        elif len(distinct_process) != 0:
            self.process_distinct(distinct_process, tables, tables_data)
        elif len(tables) > 1:
            self.process_join(columns, tables, tables_data)
        else:
            self.process_project(columns, tables[0], tables_data)

    def process_project(self, columns, table, tables_data):
        """ Deals with project operation without in a single table"""
        if len(columns) == 1 and columns[0] == '*':
            columns = self.tables_info[table]
        for column in columns:
            if column not in self.tables_info[table]:
                error_exit('No Such column \'' + column +
                           '\' found i  n the given table \'' + table + '\' ')
        print generate_header(table, columns)

        for data in tables_data[table]:
            ans = ''
            for column in columns:
                ans += data[self.tables_info[table].index(column)] + ','
            print ans.strip(',')

    def process_distinct(self, distinct_process, tables, tables_data):
        """ Process the queries with distinct """
        column_data = {}
        max_len = 0
        header = ''
        for column in distinct_process:
            table, column = self.search_column(column, tables)
            header += table + '.' + column + ','
            data = []
            for row in tables_data[table]:
                value = row[self.tables_info[table].index(column)]
                if value not in data:
                    data.append(value)
            column_data[column] = data
            max_len = max(max_len, len(tables_data[table]))
        print header.strip(',')
        for i in range(max_len):
            ans = ''
            for column in column_data:
                if i < len(column_data[column]):
                    ans += column_data[column][i] + ','
                else:
                    ans += ','
            print ans.strip(',')

    def process_join(self, columns, tables, tables_data):
        """Deals with Join type queries"""
        columns_in_table, tables_needed = self.get_tables_columns(
            columns, tables)
        join_data = []

        if len(tables_needed) == 2:
            table1 = tables_needed[0]
            table2 = tables_needed[1]
            for item1 in tables_data[table1]:
                for item2 in tables_data[table2]:
                    join_data.append(item1 + item2)
            display_output(tables_needed, columns_in_table, self.tables_info,
                           join_data, join=True)
        else:
            display_output(tables_needed, columns_in_table, self.tables_info,
                           tables_data, join=False)

    def process_where(self, condition, columns, table, table_data):
        """ Process where clause on a single table"""
        condition = format_string(condition)
        if len(columns) == 1 and columns[0] == '*':
            columns = self.tables_info[table]
        print generate_header(table, columns)
        for row in table_data:
            evaluator = self.generate_evaluator(condition, table,
                                                row)
            ans = ''
            if eval(evaluator):
                for column in columns:
                    ans += row[self.tables_info[table].index(column)] + ','
                print ans.strip(',')

    def process_where_join(self, condition, columns, tables, tables_data):
        """ Deals with Join type queries with where """
        condition = format_string(condition)
        sentence = condition
        operators = ['<', '>', '=']
        oper = ''
        if 'and' in condition:
            condition = condition.split('and')
            oper = 'and'
        elif 'or' in condition:
            condition = condition.split('or')
            oper = 'or'
        else:
            condition = [condition]
        if len(condition) > 2:
            error_exit('Maximum one AND clause can be given')
        condition1 = condition[0]
        for operator in operators:
            if operator in condition1:
                condition1 = condition1.split(operator)
        if len(condition1) == 2 and '.' in condition1[1]:
            self.process_where_normal_join([condition, oper], columns,
                                           tables, tables_data)
            return
        self.process_where_special_join(sentence, columns, tables,
                                        tables_data)

    def process_where_special_join(self, sentence, columns, tables,
                                   tables_data):
        """Process the special case of where"""
        oper = ''
        if 'and' in sentence.lower().split():
            oper = 'and'
            condition = sentence.split('and')
        elif 'or' in sentence.lower():
            oper = 'or'
            condition = sentence.split('or')
        else:
            condition = [sentence]
        needed_data = self.get_needed_data(condition, tables, tables_data)
        columns_in_table, tables_needed = self.get_tables_columns(columns,
                                                                  tables)
        join_data = join_needed_data(oper, tables_needed, needed_data,
                                     tables_data)
        display_output(tables_needed, columns_in_table, self.tables_info,
                       join_data, True)

    def process_where_normal_join(self, clauses, columns, tables,
                                  tables_data):
        """ Processes the where clause with join condition"""
        needed_data = {}
        failed_data = {}
        operators = ['<', '>', '=']
        for condition in clauses[0]:
            needed = []
            oper = ''
            condition = format_string(condition)
            for operator in operators:
                if operator in condition:
                    needed = condition.split(operator)
                    oper = operator
                    if oper == '=':
                        oper *= 2
                    break
            if len(needed) > 2:
                error_exit('Error in where clause')
            columns_condition, tables_condition = self.get_tables_columns(
                needed, tables)
            table1 = tables[0]
            table2 = tables[1]

            check_errors_for_column(columns_condition[table1][0],
                                    self.tables_info[table1], table1)
            check_errors_for_column(columns_condition[table2][0],
                                    self.tables_info[table2], table2)
            column1 = self.tables_info[table1].index(
                columns_condition[table1][0])
            column2 = self.tables_info[table2].index(
                columns_condition[table2][0])

            failed_data[condition] = []
            needed_data[condition] = []
            for data in tables_data[table1]:
                for row in tables_data[table2]:
                    evaluator = data[column1] + oper + row[column2]
                    if eval(evaluator):
                        needed_data[condition].append(data + row)
                    else:
                        failed_data[condition].append(data + row)
        if clauses[1] != '':
            join_data = join_needed_data(clauses[1],
                                         clauses[0], needed_data, failed_data)
        else:
            join_data = []
            for key in needed_data.keys():
                for data in needed_data[key]:
                    join_data.append(data)
        columns, tables = self.get_tables_columns(columns, tables)
        display_output(tables, columns, self.tables_info, join_data, True)

    def process_aggregate(self, queries, tables, tables_data):
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
                    if column_name in self.tables_info[tab]:
                        table = tab
                        column = column_name
                        cnt += 1
                if cnt == 0:
                    error_exit('No such column \'' + column_name + '\' found')
                elif cnt > 1:
                    error_exit('Ambiguous column name \'' +
                               column_name + '\' given')
            data = []
            header += table + '.' + column + ','
            for row in tables_data[table]:
                data.append(int(row[self.tables_info[table].index(column)]))

            if function_name.lower() == 'max':
                result += str(max(data))
            elif function_name.lower() == 'min':
                result += str(min(data))
            elif function_name.lower() == 'sum':
                result += str(sum(data))
            elif function_name.lower() == 'avg':
                result += str(float(sum(data)) / len(data))
            result += ','
        header.strip(',')
        print header
        print result

    def get_tables_columns(self, columns, tables):
        """ Selects required tables and columns in it"""
        columns_in_table = {}
        tables_needed = []
        if len(columns) == 1 and columns[0] == '*':
            for table in tables:
                columns_in_table[table] = []
                for column in self.tables_info[table]:
                    columns_in_table[table].append(column)
            return columns_in_table, tables

        for column in columns:
            table, column = self.search_column(column, tables)
            if table not in columns_in_table.keys():
                columns_in_table[table] = []
                tables_needed.append(table)
            columns_in_table[table].append(column)
        return columns_in_table, tables_needed

    def generate_evaluator(self, condition, table, data):
        """Generates the evaluator string for single table where"""
        condition = condition.split(' ')
        evaluator = ''
        for i in condition:
            i = format_string(i)
            if i == '=':
                evaluator += i * 2
            elif i.lower() == 'and' or i.lower == 'or':
                evaluator += ' ' + i.lower() + ' '
            elif '.' in i:
                table_here, column = self.search_column(i, [table])
                check_errors_in_condition(column, table, table_here,
                                          self.tables_info[table])
                evaluator += data[self.tables_info[table_here].index(column)]
            elif i in self.tables_info[table]:
                evaluator += data[self.tables_info[table].index(i)]
            else:
                evaluator += i
        return evaluator

    def get_needed_data(self, condition, tables, tables_data):
        """ Gets needed data for where clause"""
        operators = ['<', '>', '=']
        needed_data = {}
        for query in condition:
            needed = []
            for operator in operators:
                if operator in query:
                    needed = query.split(operator)
                    break
            check_error_in_where_clause(needed)
            table, column = self.search_column(format_string(needed[0]), tables)
            needed_data[table] = []
            query = query.replace(needed[0], ' ' + column + ' ')
            for data in tables_data[table]:
                evaluator = self.generate_evaluator(query, table, data)
                try:
                    eval(evaluator)
                    needed_data[table].append(data)
                except NameError:
                    error_exit('AND clause cannot be used in join queries')
        return needed_data

    def search_column(self, column, tables):
        """Searches for column in list of tables"""
        if '.' in column:
            table, column = column.split('.')
            table = format_string(table)
            column = format_string(column)
            if table not in tables:
                error_exit('No Such table \'' + table + '\' exists')
            return table, column
        cnt = 0
        table_needed = ''
        for table in tables:
            if column in self.tables_info[table]:
                cnt += 1
                table_needed = table
        if cnt > 1:
            error_exit('Abigous column name \'' + column + '\' given')
        elif cnt == 0:
            error_exit('No Such Column \'' + column + '\' found')
        return table_needed, column


def process_select(required):
    """Process the select part of the query and return tokens"""
    columns = []
    function_process = []
    distinct_process = []
    for item in required:
        taken = False
        item = format_string(item)
        for func in FUNCTIONS:
            if func + '(' in item.lower():
                if ')' not in item:
                    error_exit('Syntax Error: \')\' expected ')
                taken = True
                column_name = item.strip(')').split(func + '(')[1]
                if func == 'distinct':
                    distinct_process.append(column_name)
                else:
                    function_process.append([func, column_name])
                break
        if not taken:
            item = format_string(item)
            if item != '':
                columns.append(item.strip('()'))
    return columns, function_process, distinct_process


def check_errors_in_select(query):
    """ Check for errors in `select` part of the query """
    lis = query.split('from')
    if len(lis) > 2:
        error_exit('Syntax Error: More than one \"from\" statement given')
    if not check_for_string('select',
                            format_string(str(lis[0])).lower().split()):
        error_exit('Syntax Error: No Select statement given')
    elif query.lower().count('select') > 1:
        error_exit('More than one select statement given')


def check_errors_in_clauses(clauses, columns, function_process,
                            distinct_process):
    """ Check for errors in where clauses"""
    if len(columns) + len(function_process) + len(distinct_process) < 1:
        error_exit('Nothing given to select')
    if len(clauses) > 1 and \
            (len(function_process) != 0 or len(distinct_process) != 0):
        error_exit('ERROR:Where Condition can '
                   'only be given to project columns')
    elif len(distinct_process) != 0 and len(function_process) != 0:
        error_exit('distinct and aggregate functions cannot'
                   ' be given at a time')


def check_errors_in_condition(column, table_here, table, column_list):
    """ Check for errors in where condition"""
    if table_here != table:
        error_exit('Unknown table \'' + table_here + '\' given')
    elif column not in column_list:
        error_exit('No Such column \'' + column + '\' found in \'' +
                   table_here + '\' given')


def check_error_in_where_clause(needed):
    """ Check for errors in where clause"""
    if len(needed) != 2:
        error_exit('Syntax error in where clause')


def check_errors_for_column(column, column_list, table_name):
    """ Check for columns in the table_name """
    if column not in column_list:
        error_exit(
            'No Such column \'' + column + '\' in table \'' + table_name + '\'')
