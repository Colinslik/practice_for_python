# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (c) 2013-2018 ProphetStor Data Services, Inc.
# All Rights Reserved.
#

import os
import sqlite3


class SqlDB(object):
    """
    SQLite Database Query
    """

    def __init__(self, db_file, tb_name, tb_schema):
        self.conn = None
        self.new_database = False

        self.db_file = db_file
        self.tb_name = tb_name
        self.tb_schema = tb_schema
        self.tb_columns = [item[0] for item in self.tb_schema]

        self._mkdir()
        self._get_connection()
        self._create_table()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def _mkdir(self):
        # create directory for database file if not exists
        directory = os.path.dirname(self.db_file)
        if not os.path.exists(directory):
            os.makedirs(directory)
            self.new_database = True

    def _get_connection(self):
        self.conn = sqlite3.connect(self.db_file)

    def _create_table(self):
        cmd = "CREATE TABLE IF NOT EXISTS %s (" % self.tb_name \
              + ", ".join([("%s %s" % item) for item in self.tb_schema]) + ")"
        cursor = self.conn.cursor()
        cursor.execute(cmd)
        self.conn.commit()

    def insert(self, row):
        columns, values = [], []
        for key, value in row.iteritems():
            columns.append(key)
            values.append(value)

        cmd = "INSERT INTO %s (" % self.tb_name + ", ".join(columns) \
              + ") VALUES(" + ", ".join([repr(x) for x in values]) + ")"
        cursor = self.conn.cursor()
        cursor.execute(cmd)
        self.conn.commit()

        if 'id' in row:
            id = row['id']
        else:
            id = cursor.lastrowid
        # return cursor.lastrowid
        return self.get(id)

    def update(self, id, row):
        cmd = "UPDATE %s SET " % self.tb_name \
              + ", ".join(["%s = %s" % (k, repr(v)) for k, v in row.iteritems()]) \
              + " WHERE id = '%s'" % id
        cursor = self.conn.cursor()
        cursor.execute(cmd)
        self.conn.commit()

        return self.get(id)

    def delete(self, id):
        cmd = "DELETE FROM %s " % self.tb_name \
              + " WHERE id = '%s'" % id
        cursor = self.conn.cursor()
        cursor.execute(cmd)
        self.conn.commit()

    def get(self, id):
        cmd = "SELECT " + ", ".join(self.tb_columns) + \
            " FROM %s" % self.tb_name
        if isinstance(id, int):
            cmd += " WHERE id=%s" % id
        else:
            cmd += " WHERE id='%s'" % id
        cursor = self.conn.cursor()
        cursor.execute(cmd)

        v = cursor.fetchone()
        if not v:
            return None

        row = {item[0]: item[1] for item in zip(self.tb_columns, v)}
        return row

    def list(self, condition=None, sort=[], limit=None, offset=0, count=False):
        # TODO: implement offset
        cmd = "SELECT " + ", ".join(self.tb_columns) + \
            " FROM %s" % self.tb_name
        if condition:
            cmd += " WHERE %s" % condition
        if sort:
            cmd += " ORDER BY " + ", ".join(["%s %s" % i for i in sort])
        if limit:
            cmd += " LIMIT %s" % limit
        cursor = self.conn.cursor()
        cursor.execute(cmd)

        rows = []
        for v in cursor.fetchall():
            rows.append({item[0]: item[1] for item in zip(self.tb_columns, v)})

        if count:
            cmd2 = "SELECT count(*) FROM %s" % self.tb_name
            if condition:
                cmd2 += " WHERE " + condition
            cursor.execute(cmd2)
            (number_of_rows,) = cursor.fetchone()

            return rows, number_of_rows
        else:
            return rows


def printTable(myDict, colList=None):
    """ Pretty print a list of dictionaries (myDict) as a dynamically sized table.
    If column names (colList) aren't specified, they will show in random order.
    Author: Thierry Husson - Use it as you want but don't blame me.
    """
    if not colList:
        colList = list(myDict[0].keys() if myDict else [])

    myList = [colList]  # 1st row = header
    for item in myDict:
        myList.append([str(item[col] or '') for col in colList])

    colSize = [max(map(len, col)) for col in zip(*myList)]
    formatStr = ' | '.join(["{{:<{}}}".format(i) for i in colSize])
    myList.insert(1, ['-' * i for i in colSize])  # Seperating line

    for item in myList:
        print(formatStr.format(*item))
