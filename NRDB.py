#!/usr/bin/env python3
# NRDB.py

# module version
__version__ = "1.0"
# TODO: Import CSV to Sqlite/mysql database
# TODO: Joins and window functions
# TODO: Visualisation
# TODO: PostgreSQL support


# import sqlite3
try:
    import sqlite3

    have_sqlite3 = True
except ImportError:
    sqlite3 = None
    have_sqlite3 = False

# import mysql
try:
    import mysql.connector as mysql

    have_mysql = True
except ImportError:
    mysql = None
    have_mysql = False


class NRErr(Exception):
    """Error class"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class NRDB:
    def __init__(self, **kwargs):
        self._db = None
        self._cur = None
        self._dbms = None
        self._database = None
        self._table = None
        self._column_names = None

        # populate parameters
        if 'user' in kwargs:
            self._user = kwargs['user']
        else:
            self._user = None

        if 'password' in kwargs:
            self._password = kwargs['password']
        else:
            self._host = None

        if 'host' in kwargs:
            self._host = kwargs['host']
        else:
            self._host = None

        # populate properties
        if 'dbms' in kwargs:
            self.dbms = kwargs['dbms']

        if 'database' in kwargs:
            self.database = kwargs['database']

    # property setters/getters
    def get_dbms(self):
        return self._dbms

    def set_dbms(self, dbms_str):
        if dbms_str == 'sqlite':
            self._dbms = dbms_str
        else:
            raise NRErr('sqlite not available')

    def get_database(self):
        return self._database

    def set_database(self, database):
        self._database = database
        if self._cur:
            self._cur.close()
        if self._db:
            self._db.close()

        self._database = database
        if self._dbms == 'sqlite':
            self._db = sqlite3.connect(self._database)
            if self._db is None:
                raise NRErr('set_database: failed to open sqlite database')
            else:
                self._cur = self._db.cursor()
        elif self._dbms == 'mysql':
            self._db = mysql.connect(user=self._user, password=self._password,
                                     host=self._host, database=self._database)
            if self._db is None:
                raise NRErr('set_database: failed to connect to mysql')
            else:
                self._cur = self._db.cursor(prepared=True)
        else:
            raise NRErr('set_database: unknown _dbms')

    def get_cursor(self):
        return self._cur

    def set_table(self, table):
        print("set table method called")
        self._table = self.sanitize_string(table)
        self.column_names()

    def get_table(self):
        return self._table

    # properties ====
    dbms = property(fget=get_dbms, fset=set_dbms)
    database = property(fget=get_database, fset=set_database)
    table = property(fget=get_table, fset=set_table)
    cursor = property(fget=get_cursor)

    # sql methods ===
    def sql_do_nocommit(self, sql, parms=()):
        """Execute a SQL statement"""
        self._cur.execute(sql, parms)
        return self._cur.rowcount

    def sql_do(self, sql, parms=()):
        """Execute a SQL statement"""
        self._cur.execute(sql, parms)
        self.commit()
        return self._cur.rowcount

    def sql_query(self, sql, parms=()):
        self._cur.execute(sql, parms)
        for row in self._cur:
            yield row

    def sql_query_row(self, sql, parms=()):
        self._cur.execute(sql, parms)
        row = self._cur.fetchone()
        self._cur.fetchall()
        return row

    def sql_query_value(self, sql, parms=()):
        return self.sql_query_row(sql, parms)[0]

    def sql_query_id(self, sql, parms=()):
        pass

    def add_row_nocommit(self, parms=()):
        colnames = self.column_names()
        numnames = len(colnames)
        if 'id' in colnames:
            numnames -= 1
        names_str = self.sql_colnames_string(colnames)
        values_str = self.sql_values_string(numnames)
        sql = f"INSERT INTO {self._table} ({names_str}) VALUES ({values_str})"
        return self.sql_do_nocommit(sql, parms)

    def add_row(self, parms=()):
        r = self.add_row_nocommit(parms)
        print(f'r is {r}')
        self.commit()
        return r

    # crud methods ====

    def column_names(self):
        """ Get column names """
        if self._column_names is not None:
            return self._column_names

        if self._dbms == 'sqlite':
            rows = self.sql_query(f"PRAGMA table_info ({self._table});")
            self._column_names = tuple(r[1] for r in rows)
        elif self._dbms == 'mysql':
            self._cur.execute(f"SELECT * FROM {self._table} LIMIT 1")
            self._cur.fetchall()
            self._column_names = self._cur.column_names
        else:
            raise NRErr("column_names: unknown _dbms")

    def get_row(self, row_id):
        """ Get row from table – returns cursor """
        return self.sql_query_row(f"SELECT * FROM {self._table} WHERE id = ?", (row_id,))

    def get_rows(self):
        """Returns iterator"""
        return self.sql_query(f"SELECT * FROM {self._table}")

    def count_rows(self):
        """ Returns number of rows in table """
        return self.sql_query_value(f'SELECT COUNT(*) FROM {self._table}')

    def find_row(self, colname, value):
        """ Simple wrapper. Find the first match and returns id or None """
        colname = self.sanitize_string(colname)  # sanitize params
        sql = f"SELECT * FROM {self._table} WHERE {colname} LIKE ?"
        row = self.sql_query_row(sql, (value,))
        if row:
            return row[0]
        else:
            return None

    def find_rows(self, colname, value):
        """Find the first match and returns id or empty list"""
        colname = self.sanitize_string(colname)  # sanitise params
        sql = f"SELECT * FROM {self._table} WHERE {colname} LIKE ?"
        row_ids = []
        for row in self.sql_query(sql, (value,)):
            row_ids.append(row[0])
        return row_ids

    def update_row_nocommit(self, row_id, dict_rec):
        """ Update row id with data in dict """
        if "id" in dict_rec.keys():  # don't update id column
            del dict_rec['id']

        keys = sorted(dict_rec.keys())  # get keys and values
        values = [dict_rec[v] for v in keys]
        update_string = self.sql_update_string(keys)
        print(f'Update string is')
        sql = f"UPDATE {self._table} SET {update_string} WHERE id = ?"
        values.append(row_id)
        return self.sql_do_nocommit(sql, values)

    def update_row(self, row_id, dict_rec):
        rowcount = self.update_row_nocommit(row_id, dict_rec)
        self.commit()
        return rowcount

    def del_row_nocommit(self, row_id):
        """Simple wrapper"""
        return self.sql_do_nocommit(f"DELETE FROM {self._table} WHERE id = ?", (row_id,))

    def del_row(self, row_id):
        rowcount = self.del_row_nocommit(row_id)
        print(f'rowcount is {rowcount}')
        self.commit()
        return rowcount

    # utility functions ====
    @staticmethod
    def sanitize_string(s):
        """ Remove nefarious characters from a string """
        charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-.% "
        san_string = ""
        for i in range(0, len(s)):
            if s[i] in charset:
                san_string += s[i]
            else:
                san_string += '_'
        return san_string

    @staticmethod
    def sql_colnames_string(colnames):
        names_str = ","
        if colnames[0] == "id":
            colnames = colnames[1:]
        return names_str.join(colnames)

    @staticmethod
    def sql_values_string(num):
        s = "?," * num
        return s[0:-1]

    @staticmethod
    def sql_update_string(colnames):
        update_string = ","
        for i in range(len(colnames)):
            colnames[i] += "=?"
        return update_string.join(colnames)

    @staticmethod
    def version():
        return __version__

    def commit(self):
        if self.have_db():
            self._db.commit()

    def have_db(self):
        """Checks if DB is setup"""
        if self._db is None:
            return False
        else:
            return True

    def have_cursor(self):
        if self._db is None:
            return False
        else:
            return True

    def have_table(self, table_name=None):
        """Can pass table, queries master DB and checks if table exists"""
        if table_name is None:
            table_name = self._table
        if table_name is None:
            return False
        if self._dbms == 'sqlite':
            rc = self.sql_query_value("SELECT COUNT(*) FROM sqlite_master WHERE type=? AND name=?",
                                      ('table', table_name,))
            print(f'row count is {rc}')
            if rc > 0:
                return True
        return False

    def begin_transaction(self):
        if self.have_db():
            if self._database == 'sqlite':
                self.sql_do("BEGIN TRANSACTION")
            elif self._database == 'mysql':
                self.sql_do("START TRANSACTION")

    def disconnect(self):
        if self.have_cursor():
            self._cur.close()
        if self.have_db():
            self._db.close()
        self._cur = None
        self._db = None
        self._column_names = None

    def lastrowid(self):
        return self._cur.lastrowid

    # destructor
    def __del__(self):
        self.disconnect()
