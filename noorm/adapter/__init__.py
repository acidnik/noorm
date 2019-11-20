from .. import query

class AdapterBase:
    def __init__(self):
        self.conn = None
        self.operators = {}
        self.paramstyle = '${i}'

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        return self.conn.commit()

    # def query(self, *args, **kwargs):
    #     query_builder = query.QueryBuilder(custom_operators=self.operators)
    #     query_builder.build(*args, **kwargs)
    #     return query_builder
    #
    # def execute(self, *args, **kwargs):
    #     cur = self.cursor()
    #     q = self.query(*args, **kwargs)
    #     result = cur.execute(q.sql, q.binds)
    #     self.commit()
    #     return result

from noorm.adapter.mysql import AdapterMySQL
