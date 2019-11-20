import MySQLdb

from urllib.parse import urlparse
from . import AdapterBase

class AdapterMySQL(AdapterBase):
    def __init__(self):
        super().__init__()
        self.paramstyle = lambda _: '%s'
    
    @classmethod
    def parse_dsn(cls, dsn):
        r = urlparse(dsn)
        result = {}
        if r.hostname:
            result['host'] = r.hostname
        if r.username:
            result['user'] = r.username
        if r.password:
            result['passwd'] = r.password
        if len(r.path) > 1:
            result['db'] = r.path[1:] # trim /
        if r.port:
            result['port'] = r.port
        # TODO allow connection params via r.query
        return result

    def connect(self, dsn=None, **kwargs):
        if dsn is not None:
            kwargs.update(self.parse_dsn(dsn))
        self.conn = MySQLdb.connect(**kwargs)

