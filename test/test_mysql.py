
# from noorm.adapter.mysql import AdapterMySQL as m
import noorm
import os

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
            name varchar(32),
            price float,
            unique (name)
        )
    """, commit=False)
    db.execute_sql('insert into test_noorm (name, price) values (%s, %s)', ['test', 12.34])
    row = db.fetchrow('select id, name, price from test_noorm', where={'id': 1})
    assert dict(row) == {'id': 1, 'name': 'test', 'price': 12.34}
    rows = db.fetch('select id, name, price from test_noorm', where={'id': 1})
    assert rows == [{'id': 1, 'name': 'test', 'price': 12.34}]
