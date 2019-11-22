
# from . import adapter
# from . import query

# class NoormDB:
#     def __init__(self, adapter, *args, **kwargs):
#         self.adapter = adapter
#         self.adapter.connect(*args, **kwargs)
#
#     def execute_with(self, binds, sql, commit=True):
#         """
#             execute simple sql query with binds
#         """
#         cursor = self.adapter.cursor()
#         res = cursor.execute(sql, binds)
#         res = cursor.fetchall()
#         if commit:
#             self.adapter.commit()
#         return res
#
#     def _query(self, *args, **kwargs):
#         q = query.QueryBuilder(
#             custom_operators=self.adapter.operators,
#             paramstyle=self.adapter.paramstyle,
#         ).build(*args, **kwargs)
#         return q
#
#     def execute(self, *args, **kwargs):
#         q = self._query(*args, **kwargs)
#         # print(q.sql, q.binds)
#         return self.execute_with(q.binds, q.sql)
#
#     def cursor(self, *args, **kwargs):
#         q = self._query(*args, **kwargs)
#         cursor = self.adapter.cursor()
#         cursor.execute(q.sql, q.binds)
#         return cursor
#
#     def fetchall(self, *args, **kwargs):
#         cursor = self.cursor(*args, **kwargs)
#         keys = [ x[0] for x in cursor.description ]
#         res = cursor.fetchall()
#         self.adapter.commit()
#         return [ dict(zip(keys, row)) for row in res ]
#
#     def fetchrow(self, *args, **kwargs):
#         cursor = self.cursor(*args, **kwargs)
#         keys = [ x[0] for x in cursor.description ]
#         row = cursor.fetchone()
#         res = dict(zip(keys, row))
#         self.adapter.commit()
#         return res
#
#     # def insert(self, table, data, **kwargs):
#
#
#
# def mysql(*args, **kwargs):
#     a = adapter.AdapterMySQL()
#     return NoormDB(a, *args, **kwargs)
from noorm.db.mysql import NoormDBMySQL 

mysql = NoormDBMySQL

"""
Query builder

q = query(*args, **kwargs)
q.sql
q.binds

*args, **kwargs will be turned to string by following rules
str, kwargs.keys() - as is
dicts will be turned into where condition follw the rules
{
    'field': 'condition'
}

if condition is not a dict, then it will be turned into 'field = $1', and 'condition' is added to binds
if condition is a list: 'field in [$1, $2, ...]', list items added to binds
if condition is a dict:
{'operator': 'value', 'raw': bool, 'coerce': Callable, 'not': bool, '__or__': {... nested conditions, joined by or ...}}
operator:
eq, gt, ge, lt, le, in, in, like, rlike,
raw:
do not use $1 / binds, but use value as is
coerce:
convert value to given type
not:
inverse condition (add not (...))

{'price': {'gt': 100}}
'price > $1'
{'created_dt': {'gt': 'now() - interval 1 day', 'raw': 1}}
'created_dt > now() - interval 1 day'

{'is_active': {}}
'is_active'


query execution:
    db = noorm.mysql(dsn)
    # or
    db = await noorm.aio.mysql(dsn)

    rows = db.fetchall('select * from t')
    price = db.fetchone('select price from t', where={'id': product_id})
    product_by_id = db.fetchdict('select id, price, name from t') # {1: {'id': 1, 'price': 100, 'name': 'product_name'}}
    price_by_id = db.fetchkv('select id, price from t') # {1: 100, 2: 200}
    ids = db.fetchlist('select id from t') # [1,2]


"""
