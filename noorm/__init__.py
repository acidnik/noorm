
from . import adapter
from . import query

class NoormDB:
    def __init__(self, adapter, *args, **kwargs):
        self.adapter = adapter
        self.adapter.connect(*args, **kwargs)

    def execute(self, *args, **kwargs):
        q = query.QueryBuilder(
            custom_operators=self.adapter.operators,
            paramstyle=self.adapter.paramstyle,
        ).build(*args, **kwargs)
        # print(q.sql, q.binds)
        cursor = self.adapter.cursor()
        res = cursor.execute(q.sql, q.binds)
        # res = cursor.fetchall()
        # self.adapter.commit()
        return res

    def fetchall(self, *args, **kwargs):
        # query = QueryBuilder(custom_operators=self.adapter.operators)
        # cursor = self.adapter.cursor()
        # cursor.execute(query.sql, query.binds)
        # res = cursor.fetchall()
        self.adapter.commit()
        # return res


def mysql(*args, **kwargs):
    a = adapter.AdapterMySQL()
    return NoormDB(a, *args, **kwargs)

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
