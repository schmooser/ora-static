#!/usr/bin/python
# encoding: utf-8

from os.path import join
from os.path import splitext
import ora_static as ora

__author__ = 'Pavel Popov'
__email__ = 'schmooser@gmail.com'
__version__ = '1.0'


def process(conn, files):

    for f in files:
        if not f:
            continue

        filename = dict(zip(('f', 'ext'), splitext(f)))
        o = ora.OraStatic(connection_string=conn,
                file_query=join('query', f),
                file_yield=join('yield', f),
                file_result=join('out', 'script_{f}{ext}'.format(filename)))
        o.process()


if __name__ == '__main__':
    conn = 'user/pass@tns1'
    files = ('file1.sql',
             'file2.sql',
    )
    process(conn, files)

    conn = 'user/pass@tns2'
    files = ('file3.sql',
             'file4.sql'
    )
    process(conn, files)
