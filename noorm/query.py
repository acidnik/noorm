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

def _op(op):
    def inner(val, start, raw, paramstyle):
        if raw:
            return [], start, op, str(val)
        else:
            return [val], start + 1, op, paramstyle(start)
    return inner

operators = {
    'eq': _op('='),
    'gt': _op('>'),
    'ge': _op('>='),
    'lt': _op('<'),
    'le': _op('<='),
    'like': _op('LIKE'),
    'in': _in,
}

class QueryBuilder:
    def __init__(self, placeholder=1, binds=None, custom_operators=None, paramstyle=lambda i: f'${i}'):
        self.sql = None
        self.binds = binds or []
        self.placeholder = placeholder
        self.operators = operators
        if custom_operators:
            self.operators.update(custom_operators)
        self.paramstyle = paramstyle

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

    def _build_cond(self, field, cond):
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
        if negate:
            return f"NOT ({result})"
        return result

    @staticmethod
    def _coerce(val, t):
        if isinstance(val, list):
            return [ t(x) for x in val ]
        return t(val)

def query(*args, **kwargs):
    return QueryBuilder().build(*args, **kwargs)
