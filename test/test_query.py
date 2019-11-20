import pytest

from noorm.query import query

def test_query():
    q = query('select 1')
    assert q.sql == 'select 1'
    assert q.binds == []

    q = query('select * from t', where={})
    assert q.sql == 'select * from t where '

    q = query({'price': 100})
    assert q.sql == 'price = $1'
    assert q.binds == [100]
    
    q = query({'price': 100, 'year': 1984})
    assert q.sql == 'price = $1 AND year = $2'
    assert q.binds == [100, 1984]
    
    q = query({'price': 100, 'd': {'eq': 'now()', 'raw': True}})
    assert q.sql == 'price = $1 AND d = now()'
    assert q.binds == [100]

    q = query({'is_deleted': {'not': True}})
    assert q.sql == 'NOT (is_deleted)'

    q = query({'is_active': {}})
    assert q.sql == 'is_active'

    q = query({'price': {'eq': '100.1', 'coerce': float}})
    assert q.sql == 'price = $1'
    assert q.binds == [100.1]

    q = query({'__or__': {'is_active': {}, 'is_deleted': {'not': True}}})
    assert q.sql == '(is_active OR NOT (is_deleted))'

    q = query({'__or__': {'a': {}, 'b': {}, '__and__': {'c': {}, 'd': {}}}})
    assert q.sql == '(a OR b OR (c AND d))'

    q = query({'price': 1, 'id': [1,2,3]})
    assert q.sql == 'price = $1 AND id IN ($2, $3, $4)'
    assert q.binds == [1, 1, 2, 3]

    q = query({'dt': {'in': ['now()', 'now()+1'], 'raw': True}})
    assert q.sql == 'dt IN (now(), now()+1)'

    q = query({"json_data#>>'{price,currency}'": 'rub'})
    #q = query({})
