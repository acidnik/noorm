import pytest
import noorm
import os
import MySQLdb

from datetime import datetime

MYSQL_DSN = os.environ.get('NOORM_TEST_MYSQL_DSN')

def with_dsn(f):
    def inner(*args, **kwargs):
        if MYSQL_DSN is None:
            return
        return f(*args, **kwargs)
    return inner


@with_dsn
def test_mysql():
    db = noorm.mysql(dsn=MYSQL_DSN)
    db.execute_sql("""
        drop table if exists test_noorm;
        create table test_noorm (
            id serial,
            dt datetime not null default now(),
            name varchar(32) UNIQUE,
            price float,
            unique (name)
        )
    """, commit=False)
    db.execute_sql('insert into test_noorm (name, price) values (%s, %s)', ['test', 12.34])
    row = db.fetchrow('select id, name, price from test_noorm', where={'id': 1})
    assert row == {'id': 1, 'name': 'test', 'price': 12.34}
    rows = db.fetch('select id, name, price from test_noorm', where={'id': 1})
    assert rows == [{'id': 1, 'name': 'test', 'price': 12.34}]

    # def fetchcol(self, *args, **kwargs):
    col = db.fetchcol('select id from test_noorm')
    assert col == [1]

    # def fetchval(self, *args, **kwargs):
    tid = db.fetchval('select id from test_noorm')
    assert tid == 1

    # def fetchdict(self, *args, **kwargs):
    rows = db.fetchdict('select id, name, price from test_noorm')
    assert rows == {1: {'id': 1, 'name': 'test', 'price': 12.34}}

    # def fetchkv(self, *args, **kwargs):
    rows = db.fetchkv('select id, price from test_noorm')
    assert rows == {1: 12.34}

    tid = db.insert('test_noorm', {'name': 'foo', 'price': 22})
    assert tid == 2
    
    tid = db.insert('test_noorm', [{'name': 'foo1', 'price': 22}, {'name': 'foo2', 'price': 23}])
    assert db.fetchval('select count(*) from test_noorm') == 4

    with pytest.raises(MySQLdb._exceptions.IntegrityError):
        tid = db.insert('test_noorm', {'name': 'foo', 'price': 22})

    tid = db.insert('test_noorm', {'name': 'foo', 'price': 33}, update='name')
    assert db.fetchval('select price from test_noorm', where={'name': 'foo'}) == 33


    # print(db.fetchcol('select price from test_noorm order by 1'))
    res = db.delete('test_noorm', where={'price': {'gt': 22}})
    assert res == 2
    
    db.execute('truncate table test_noorm')
    tid = db.insert('test_noorm', {'name': 'foo', 'price': 22})
    
    row = db.fetchrow('select name, price from test_noorm', where={'id': tid})
    db.update('test_noorm', {'name': 'new name', 'price': {'val': 'price + 1', 'raw': True}}, where={'id': tid})
    row2 = db.fetchrow('select name, price from test_noorm', where={'id': tid})
    assert row['price'] == row2['price'] - 1
    assert row2['name'] == 'new name'

    db.insert('test_noorm', {'name': 'test3', 'dt': {'val': 'now()', 'raw': True}})
    db.insert('test_noorm', [{'name': 'test4', 'dt': {'val': 'now()', 'raw': True}}, {'name': 'test5', 'dt': datetime.now()}])

