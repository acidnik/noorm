from urllib.parse import urlparse
from ..query import QueryBuilder

import logging


logger = logging.getLogger('noorm')


class NoormDB:
    """
        base db class
    """
    def __init__(self, **kwargs):
        dsn = kwargs.pop('dsn', None)
        if dsn is not None:
            kwargs.update(self.parse_dsn(dsn))
        self.conn = self.connect(**kwargs)

    def query_builder(self):
        return QueryBuilder()
    
    @classmethod
    def parse_dsn(cls, dsn):
        r = urlparse(dsn)
        result = {}
        if r.hostname:
            result['host'] = r.hostname
        if r.username:
            result['user'] = r.username
        if r.password:
            result['passwd'] = r.password
        if len(r.path) > 1:
            result['db'] = r.path[1:] # trim /
        if r.port:
            result['port'] = r.port
        # TODO allow connection params via r.query
        return result

    def connect(self, **kwargs):
        raise NotImplementedError

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        return self.conn.commit()

    def execute_sql(self, sql, binds=None, commit=True, fetch=True):
        if binds is None:
            binds = []
        cursor = self.cursor()
        logger.debug(f"{sql} % {binds}")
        res = cursor.execute(sql, binds)
        if fetch:
            res = cursor.fetchall()
        if commit:
            self.commit()
        return res

    def execute(self, *args, **kwargs):
        q = self.query_builder().build(*args, **kwargs)
        return self.execute_sql(q.sql, q.binds)

    def fetch(self, *args, **kwargs):
        """
            rows = db.fetch(*args, **kwargs)
            returns all rows from query as list of dicts
        """
        q = self.query_builder().build(*args, **kwargs)
        cursor = self.cursor()
        cursor.execute(q.sql, q.binds)
        keys = [ x[0] for x in cursor.description ]
        res = cursor.fetchall()
        self.commit()
        return [ dict(zip(keys, row)) for row in res ]

    def fetchrow(self, *args, **kwargs):
        q = self.query_builder().build(*args, **kwargs)
        cursor = self.cursor()
        cursor.execute(q.sql, q.binds)
        keys = [ x[0] for x in cursor.description ]
        row = cursor.fetchone()
        res = dict(zip(keys, row))
        self.commit()
        return res

    def fetchcol(self, *args, **kwargs):
        q = self.query_builder().build(*args, **kwargs)
        cursor = self.cursor()
        cursor.execute(q.sql, q.binds)
        res = cursor.fetchall()
        self.commit()
        return [ x[0] for x in res ]

    def fetchval(self, *args, **kwargs):
        q = self.query_builder().build(*args, **kwargs)
        cursor = self.cursor()
        cursor.execute(q.sql, q.binds)
        row = cursor.fetchone()
        self.commit()
        return row[0]

    def fetchdict(self, *args, **kwargs):
        q = self.query_builder().build(*args, **kwargs)
        cursor = self.cursor()
        cursor.execute(q.sql, q.binds)
        keys = [ x[0] for x in cursor.description ]
        res = cursor.fetchall()
        self.commit()
        return { row[0]: dict(zip(keys, row)) for row in res }

    def fetchkv(self, *args, **kwargs):
        q = self.query_builder().build(*args, **kwargs)
        cursor = self.cursor()
        cursor.execute(q.sql, q.binds)
        keys = [ x[0] for x in cursor.description ]
        assert len(keys) == 2, 'query for fetchkv must return exactly 2 columns'
        res = cursor.fetchall()
        self.commit()
        return { row[0]: row[1] for row in res }

    def insert(self, table, rows, **kwargs):
        if not isinstance(rows, list):
            rows = [rows]
        sql, binds = self.query_builder().insert(table, rows, **kwargs)
        return self.execute_sql(sql, binds)

    def delete(self, table, *, where):
        sql, binds = self.query_builder().delete(table, where=where)
        return self.execute_sql(sql, binds, fetch=False)

    def update(self, table, row, *, where):
        """
            update t set name = $1, updated_dt = now(), count = count + 1
            db.update('t', {'name': "new name", 'updated_dt': {'val': 'now()', 'raw': True}, 'count': {'val': 'count + 1', 'raw': True}}
        """
        sql, binds = self.query_builder().update(table, row, where=where)
        return self.execute_sql(sql, binds, fetch=False)



