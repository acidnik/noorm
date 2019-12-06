Synopsis
========
```
db = noorm.mysql.connect('mysql://user:pass@localhost/test')

# execute sql
db.execute_sql("""
    create table users (
        id serial,
        name varchar(255) UNIQUE,
        password: varchar(40)
        -- etc
    )
""")

# execute sql with bind values
db.execute_sql("delete from users where name = %s", ['Oleg'])

# insert
user_id = db.insert('users', {'name': 'Ale', 'password': 'xxx'})

# insert multiple
db.insert('users', [{'name': 'name1'}, {'name': 'name2'}, {'name': 'name3'}])

# insert ... on duplicate key update (all provided fields except 'name', 'password' in this case)
# assuming that 'name' caused the conflict
db.insert('users', {'name': 'Ale', 'password': 'xxx2'}, update='name')

# insert ... on duplicate key update (only given fields)
db.insert('users', {...}, update={'password'})

# update; use raw sql
rows_affected = db.update('users', {'password': 'xxx3', 'name': {'val': "concat(name, '_new')", 'raw': True}}, where={'id': user_id})
# update users set password = %s, name = concat(name, '_new') where id = %s

# delete
db.delete('users', where={'id': user_id})

# query
## list of dicts
rows = db.fetch('select * from users', where={'name': 'Ale'}, 'order by 1')

## list of values
names = db.fechcol('select name from users')

## single value
cnt = db.fetchval('select count(*) from users')

## dict { id: row }
users_by_id = db.fetchdict('select id, name, password from users')

## dict { id: name }
names_by_id = db.fetchkv('select id, name from users')

# queries
db.fetch('select', '*', 'from', 'users')
# same as
db.fetch('select * from users')

# gt, ge, lt, le, eq, like, in
db.fetch('select * from users', where={'name': {'gt': some_name,}})

# where is not a special keyword, it's just kwargs. keys go as is, dicts are processed into sql conditions
db.fetch('select * from users where', {'name': some_name}, 'order by name')

# implicit IN
user_ids = [1,2,3]
db.fetch('select * from users', where={'id': user_ids})
# same as
db.fetch('select * from users', where={'id': {'in': user_ids}})

# expressions in query
db.fetch('select * from event_log', where={'created_dt': {'gt': 'now() - interval 1 day', 'raw': True}})

# nested conditions
db.fetch('select * from users', where={'__or__': {'name': some_name, 'id': some_id}})
# where (name = %s or id = %s)

db.fetch('select * from users', where={'__and__': {'name': some_name, '__text__': '... some tricky sql here ...'}})
# where (name = %s AND ... some tricky sql here ...)

```

TODO
====
* async
* different DBMS (pg, sqlite, clickhouse)
* documentation
