import contextlib
import MySQLdb
import MySQLdb.cursors
import config

# db connection
__={}


def cursor():
    return contextlib.closing(get_db().cursor())

def execute(sql, *args):
    sql = sql.strip()
    with cursor() as c:
        c.execute(sql, *args)
        commit()
        return c.fetchall()

def insert(sql, *args):
    _, id2 = insertMany(sql, *args)
    return id2

def insertMany(sql, *args):
    sql = sql.strip()
    with cursor() as c:
        c.execute(sql, *args)
        
        first_id = c.lastrowid
        last_id = first_id + c.rowcount - 1
        
        commit()
        
        return first_id, last_id


def query(sql, *args):
    sql = sql.strip()
    with cursor() as c:
        c.execute(sql, *args)
        return c.fetchall()


def queryOne(sql, *args):
    sql = sql.strip()
    with cursor() as c:
        c.execute(sql, *args)
        return c.fetchone()

def queryGen(sql, *args):
    sql = sql.strip()
    with cursor() as c:
        c.execute(sql, *args)
        for r in c:
            yield r

def commit():
    get_db().commit()
    

def get_db():
    """Returns a database connection instance."""
    if 'connection' in __:
        return __['connection']
        
    cfg = config.get_config()
        
    __['connection'] = MySQLdb.connect(
        host=cfg.dbConfig['host'], user=cfg.dbConfig['user'], 
        passwd=cfg.dbConfig['password'], db=cfg.dbConfig['database'],
        cursorclass=MySQLdb.cursors.DictCursor
    )
    
    return __['connection']

def close():
    if 'connection' in __:
        __['connection'].close()
        __['connection'] = None
