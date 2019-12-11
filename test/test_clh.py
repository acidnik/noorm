import os
import noorm
from datetime import datetime


CLH_DSN = os.environ.get('NOORM_TEST_CLH_DSN')

def with_dsn(f):
    def inner(*args, **kwargs):
        if CLH_DSN is None:
            return
        return f(*args, **kwargs)
    return inner

@with_dsn
def test_clh():
    db = noorm.clh(dsn=CLH_DSN)
    db.execute_sql('drop table if exists test_noorm')
    db.execute_sql("""
        create table test_noorm (
            id UInt32,
            name String,
            created_dt Date
        )
        ENGINE MergeTree() PARTITION BY toYYYYMM(created_dt) ORDER BY (id, created_dt) SETTINGS index_granularity=8192
    """)

    db.insert('test_noorm', [{'id': 1, 'name': 'name1', 'created_dt': datetime.now()}], bulk=True)

    # db.insert('test_noorm', {'id': '2', 'name': 'name2', 'created_dt': {'val': 'now()', 'raw': True}}, bulk=False)
    # db.insert('test_noorm', {'id': 2, 'name': 'name2'})
