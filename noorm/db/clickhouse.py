from clickhouse_driver import Client

from . import NoormDB
from ..query import QueryBuilder

import logging


logger = logging.getLogger('noorm')


class QueryBuilderClickhouse(QueryBuilder):
    @staticmethod
    def paramstyle(_i):
        return f'%s'

    def insert(self, table, rows, bulk=True):
        """

        :param bulk: use clickhouse bulk insert features. Suitable for large inserts, but not for inserts with raw values:
        """
        sql = ['INSERT', 'INTO', table]
        keys = rows[0].keys()
        sql.append('(' + ', '.join(keys) + ')')
        sql.append('VALUES')

        if bulk:
            # yea, clh realy makes it easy
            binds = rows
        else:
            sql_ph, binds = self.insert_values(keys, rows)
            sql.append(sql_ph)

        print(sql, binds)
        return ' '.join(sql), binds


class NoormDBClickhouse(NoormDB):
    
    @classmethod
    def parse_dsn(cls, dsn):
        return {'url': dsn}
    
    def connect(self, **kwargs):
        return Client.from_url(**kwargs)
    
    def execute_sql(self, sql, binds=None, commit=False, fetch=False):
        if binds is None:
            binds = []
        else:
            binds = [binds]
        logger.debug(f"{sql} % {binds}")
        res = self.conn.execute(sql, *binds)
        return res
    
    def cursor(self):
        return self.conn

    def commit(self):
        pass

    def query_builder(self):
        return QueryBuilderClickhouse()

    def insert(self, table, rows, **kwargs):
        if not isinstance(rows, list):
            rows = [rows]
        sql, binds = self.query_builder().insert(table, rows, **kwargs)
        return self.execute_sql(sql, binds)

