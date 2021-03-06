from typing import Callable


def _in(val, start, raw, paramstyle):
    if raw:
        values = ', '.join([ str(x) for x in val ])
        binds = []
    else:
        values = ', '.join([ paramstyle(i) for i in range(start, start + len(val)) ])
        start += len(values)
        binds = val
    result = '(' + values + ')'
    return binds, start, 'IN', result

def _between(val, start, raw, paramstyle):
    if raw:
        v1, v2 = val
        binds = []
    else:
        binds = val
        v1, v2 = paramstyle(start), paramstyle(start+1)
        start += 2
    result = v1 + ' AND ' + v2
    return binds, start, 'BETWEEN', result

def _op(op):
    def inner(val, start, raw, paramstyle):
        if raw:
            return [], start, op, str(val)
        else:
            return [val], start + 1, op, paramstyle(start)
    return inner

operators = {
    'val': _op('='), # for update statement
    'eq': _op('='),
    'gt': _op('>'),
    'ge': _op('>='),
    'lt': _op('<'),
    'le': _op('<='),
    'like': _op('LIKE'),
    'in': _in,
    'between': _between,
}

class QueryBuilder:
    def __init__(self, placeholder=1, binds=None):
        self.sql = None
        self.binds = binds or []
        self.operators = operators
        self.placeholder = placeholder

    @staticmethod
    def paramstyle(i):
        return f'${i}'

    def __str__(self):
        return f"{self.sql} % {self.binds}"

    def build(self, *args, **kwargs):
        sql_parts = []
        for part in args:
            if isinstance(part, dict):
                sql_parts.append(self.condition(part))
            else:
                sql_parts.append(part)
        for k, v in kwargs.items():
            sql_parts.append(k)
            if isinstance(v, dict):
                sql_parts.append(self.condition(v))
            else:
                sql_parts.append(v)
        self.sql = ' '.join(sql_parts)
        return self

    def condition(self, cond, logic='AND'):
        result = []
        for k, v in cond.items():
            result.append(self._build_cond(k, v))
        return (' ' + logic + ' ').join(result)

    def _build_cond(self, field, cond, val_only=False):
        if field == '__or__':
            return '(' + self.condition(cond, logic='OR') + ')'
        if field == '__text__':
            return cond
        if field == '__and__':
            return '(' + self.condition(cond, logic='AND') + ')'

        if isinstance(cond, list):
            cond = {'in': cond}

        if not isinstance(cond, dict):
            # {'field': 'value'}
            param = self.paramstyle(self.placeholder)
            result = f"{field} = {param}"
            self.binds.append(cond)
            self.placeholder += 1
            if val_only:
                return cond
            return result
        
        negate = cond.pop('not', False)

        if len(cond) == 0:
            # {'bool_field': {}} or {'bool_field': {'not': True}}
            result = field
        else:
            cond_val = None
            cond_op = None
            negate = False
            for op_name, op_val in self.operators.items():
                if op_name in cond:
                    if cond_op is not None:
                        raise ValueError(f"multiple operators in condition {cond}: {op_name}, {cond_op}")
                    val = cond[op_name]
                    if to_type := cond.get('coerce'):
                        val = self._coerce(val, to_type)
                    binds, self.placeholder, cond_op, cond_val = op_val(val, self.placeholder, cond.get('raw'), self.paramstyle)
                    self.binds.extend(binds)

            if not cond_op:
                raise ValueError(f"operator missing in condition {cond}")
            result = ' '.join([field, cond_op, cond_val])
        if val_only:
            return cond_val
        if negate:
            return f"NOT ({result})"
        return result


    @staticmethod
    def _coerce(val, t):
        if isinstance(val, list):
            return [ t(x) for x in val ]
        return t(val)

    def insert(self, table, rows, **kwargs):
        raise NotImplementedError

    def insert_values(self, keys, rows):
        binds = []
        sql = []
        for row in rows:
            # not using row.items() to preserve same order across rows
            sql_row = []
            for k in keys:
                v = row[k]
                if isinstance(v, dict) and v.get('raw'):
                    val = v['val']
                    sql_row.append(val)
                else:
                    sql_row.append('%s')
                    binds.append(v)
            sql.append('(' + ', '.join(sql_row) + ')')
        return ', '.join(sql), binds

    def delete(self, table, *, where):
        sql = ['DELETE FROM', table, 'WHERE']
        self.build(where)
        where, binds = self.sql, self.binds
        sql.append(where)
        return ' '.join(sql), binds
            
    def update(self, table, row, *, where):
        # db.update('t', {'name': "new name", 'updated_dt': {'val': 'now()', 'raw': True}, 'count': {'val': 'count + 1', 'raw': True}}
        sql = ['UPDATE', table, 'SET']
        set_sql = []
        for k, v in row.items():
            # TODO: check for valid values here? {'key': {'gt': 0}} not allowed in update
            set_sql.append(self._build_cond(k, v))
        sql.append(', '.join(set_sql))
        sql.append('WHERE')
        sql.append(self.condition(where))
        return ' '.join(sql), self.binds



def query(*args, **kwargs):
    return QueryBuilder().build(*args, **kwargs)
