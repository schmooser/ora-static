#!/usr/bin/python
# encoding: utf-8

import datetime
from decimal import Decimal

import cx_Oracle

__author__ = 'Pavel Popov'
__email__ = 'schmooser@gmail.com'
__version__ = '0.2.0'


def decimal_numbers(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        return cursor.var(str, 100, cursor.arraysize, outconverter=Decimal)


class OraStatic(object):
    NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    ALTER_SESSION = "ALTER SESSION SET NLS_DATE_FORMAT='%s'" % NLS_DATE_FORMAT

    def __init__(self, connection_string, file_query='query.sql',
                 file_yield='yield.sql', file_result='result.sql'):

        self.file_query = file_query
        self.file_yield = file_yield
        self.file_result = file_result
        self.connection_string = connection_string

    def load_data(self):

        db = cx_Oracle.connect(self.connection_string)
        db.outputtypehandler = decimal_numbers
        cursor = db.cursor()

        cursor.execute(self.ALTER_SESSION)
        cursor.execute(open(self.file_query, 'r').read())

        titles = [x[0] for x in cursor.description]

        def item_to_str(x):
            ii = isinstance
            if x is None:
                return 'NULL'
            if ii(x, Decimal):
                return str(x)
            if ii(x, int) or ii(x, float):
                return str(x)
            if ii(x, datetime.datetime):
                return "DATE'%s'" % str(x)[:10]
            return "'%s'" % x

        wrap = lambda x: '%s %s' % (item_to_str(x[0]), x[1])

        out = ["select %s from dual" % ', '.join(map(wrap, zip(row, titles)))
               for row in cursor]

        with_stmt = ' union all\n'.join(out)
        return with_stmt

    def process(self):
        with_stmt = self.load_data()
        sql_stmt = open(self.file_yield, 'r').readlines()
        output = open(self.file_result, 'w')

        output.write('set define off;\n%s;\n' % self.ALTER_SESSION)

        for line in sql_stmt:
            if '/*WITH*/' in line:
                line = with_stmt
            output.write(line)

        output.write('commit;\nexit;\n')
        output.close()
        print('File %s processed' % self.file_query)
