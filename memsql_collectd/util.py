from datetime import datetime
import hashlib

def hash_64_bit(*values):
    result = hashlib.sha1('.'.join(values))
    # truncate into a 64-bit int
    return int(result.hexdigest()[:16], 16)

def sql_utcnow():
    """ returns a SQL compliant UTC timestamp string """
    return to_sql(datetime.utcnow())

def to_sql(dt):
    """ transforms a datetime into a SQL compliant datetime string """
    return dt.strftime('%Y-%m-%d %H:%M:%S')
