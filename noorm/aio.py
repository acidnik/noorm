
class NoormAsyncDB:
    def __init__(self, adapter):
        self.adapter = adapter

async def mysql(*args, **kwargs):
    db = NoormAsyncDB(adapter=noorm.aio.adapter.mysql)
    await db.adapter.connect(*args, **kwargs)
    return db
