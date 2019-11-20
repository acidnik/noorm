
from noorm.adapter.mysql import AdapterMySQL as m
import noorm
import os

MYSQL_DSN = os.environ.get('NOORM_TEST_MYSQL_DSN')

def with_dsn(f):
    def inner(*args, **kwargs):
        if MYSQL_DSN is None:
            return
        return f(*args, **kwargs)
    return inner

def test_mysql_parse_dsn():
    dsn = 'mysql://user:pass@localhost:3306/test'
    params = m.parse_dsn(dsn)
    assert params['host'] == 'localhost'
    # TODO


@with_dsn
def test_mysql():
    db = noorm.mysql(dsn=MYSQL_DSN)
    db.execute("""
        drop table if exists test_noorm;
        create table test_noorm (
            id serial,
            dt datetime not null default now(),
            name varchar(32),
            price float,
            unique (name)
        )
    """)
    db.execute('select * from test_noorm', where={'id': 1})
