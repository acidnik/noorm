import MySQLdb

from . import NoormDB
from ..query import QueryBuilder


class QueryBuilderMySQL(QueryBuilder):
    @staticmethod
    def paramstyle(_i):
        return f'%s'

    def insert(self, table, rows, update=None, delayed=False, ignore=False):
        sql = ['INSERT']
        if delayed:
            sql.append('DELAYED')
        if ignore:
            sql.append('IGNORE')
        sql.extend(['INTO', table])
        keys = rows[0].keys()
        sql.append('(' + ', '.join(keys) + ')')
        sql.append('VALUES')
        
        binds = []
        ph = '(' + ','.join('%s' for _ in keys) + ')'
        sql.append(', '.join(ph for _ in rows))
        for row in rows:
            for k in keys:
                binds.append(row[k])
        if update is not None:
            update_keys = None
            if isinstance(update, str):
                # update all fields, except index
                update_keys = set(keys) - {update}
            elif isinstance(update, set):
                # update only these fields
                update_keys = update
            else:
                raise ValueError("invalid update")
            sql.append('ON DUPLICATE KEY UPDATE')
            sql.append(', '.join( f"{k} = values({k})" for k in update_keys))
        return ' '.join(sql), binds


class NoormDBMySQL(NoormDB):
    def connect(self, **kwargs):
        return MySQLdb.connect(**kwargs)

    def query_builder(self):
        return QueryBuilderMySQL()

    def insert(self, table, rows, **kwargs):
        if not isinstance(rows, list):
            rows = [rows]
        sql, binds = self.query_builder().insert(table, rows, **kwargs)
        self.execute_sql(sql, binds)
        _id = self.fetchval('select last_insert_id()')
        return _id
