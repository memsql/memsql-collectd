from datetime import datetime
import threading
import math
from wraptor.decorators import throttle
import calendar
from itertools import repeat, chain, islice, tee, imap, ifilter
from memsql_collectd import util

CLASSIFIER_MIN_HASH_LEN = 6
CLASSIFIERS = [ 'instance_id', 'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota', 'kappa', 'lambda', 'mu', 'nu', 'xi', 'omicron' ]
CLASSIFIER_COLUMNS = CLASSIFIERS + [ "classifier", "id", "last_updated" ]

ANALYTICS_COLUMNS = [ "classifier_id", "created", "value" ]
ANALYTICS_TEMPLATE = ["(%s, %s, %s)"]

def partition(iterator, cb):
    """ partitions iterator into two lists by running callback on each value
        * callback is only executed once per value
        * iterator must be finite, as this is a non-lazy partition function
    """
    trues, falses = tee(imap(lambda x: (cb(x), x), iterator), 2)
    return [v for r, v in trues if r], [v for r, v in falses if not r]

class AnalyticsRow(object):
    def __init__(self, created, raw_value, *classifier):
        self.created = created
        self.raw_value = raw_value

        self.value = None
        self._classifier_id = None

        self.classifier = tuple(islice(chain(ifilter(None, classifier), repeat('')), len(CLASSIFIERS)))

    @property
    def classifier_id(self):
        if self._classifier_id is None:
            parts = [x for x in self.classifier if x != '']
            if len(parts) < CLASSIFIER_MIN_HASH_LEN:
                diff = CLASSIFIER_MIN_HASH_LEN - len(parts)
                parts = parts + ([''] * diff)
            self._classifier_id = util.hash_64_bit(*parts)
        return self._classifier_id

    @property
    def joined_classifier(self):
        return '.'.join([x for x in self.classifier if x != ''])

    def classifier_values(self):
        """ Returns the entire row as a tuple for the classifiers table. """
        return self.classifier + (self.joined_classifier, self.classifier_id)

    def analytics_values(self):
        """ Returns a tuple for inserting into the analytics table. """
        return (self.classifier_id, int(self.created), self.value)

    def valid(self):
        """ Validates that the value of this row is a valid value (ie. not None and not NaN) """
        return not (self.value is None or math.isnan(self.value))

class AnalyticsCache(object):
    """ A thread safe cache for analytics rows.  Can efficiently flush
    all values in the cache using a multi-insert.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._pending = []
        self._seen_classifiers = []
        self._previous_rows = {}

    def swap_row(self, new_row):
        """ Stores new_row in the cache and returns the previous row
        with the same hash or None.
        """
        classifier = new_row.classifier
        with self._lock:
            self._pending.append(new_row)
            previous_row, self._previous_rows[classifier] = self._previous_rows.get(classifier, None), new_row
        return previous_row

    def flush(self, agg_pool):
        """ Generates a multi-insert statement and executes it against the analytics table. """

        # garbage collection routines
        self.garbage_collect_pending()
        self.garbage_collect_classifiers()

        # grab all the rows that need to be flushed
        with self._lock:
            flushing, self._pending = partition(self._pending, lambda r: r.valid())

        if len(flushing) > 0:
            self.record_classifiers(agg_pool, flushing)

            with agg_pool.connect() as conn:
                columns = ','.join(ANALYTICS_COLUMNS)
                while len(flushing):
                    batch, flushing = flushing[:128], flushing[128:]

                    row_values = sorted((row.analytics_values() for row in batch), key=lambda x: ('%s%s' % (x[0], x[1])))
                    query_params = sum(row_values, ())
                    values = ','.join(ANALYTICS_TEMPLATE * len(row_values))

                    conn.execute('''
                        INSERT INTO analytics (%s) VALUES %s
                        ON DUPLICATE KEY UPDATE value=value
                    ''' % (columns, values), *query_params)

    @throttle(60)
    def garbage_collect_pending(self):
        """ Check the pending array for any old rows, and then delete them """

        now = calendar.timegm(datetime.now().utctimetuple())
        with self._lock:
            self._pending = [r for r in self._pending if r.created > (now - 360)]

    def record_classifiers(self, agg_pool, analytic_rows):
        now = (util.sql_utcnow(),)  # 1-element tuple!
        value_template = ['(' + ','.join(['%s'] * len(CLASSIFIER_COLUMNS)) + ')']
        columns = ','.join(CLASSIFIER_COLUMNS)

        pending = []
        for row in analytic_rows:
            if row.classifier not in self._seen_classifiers:
                pending.append(row)
                self._seen_classifiers.append(row.classifier)

        if len(pending) > 0:
            with agg_pool.connect_master() as master_conn:
                while len(pending):
                    batch, pending = pending[:50], pending[50:]
                    row_values = [tuple(row.classifier_values()) + now for row in batch]
                    query_params = sum(row_values, ()) + now
                    values = ','.join(value_template * len(row_values))
                    sql = "INSERT INTO classifiers (%s) VALUES %s ON DUPLICATE KEY UPDATE last_updated=%%s" % (columns, values)
                    master_conn.execute(sql, *query_params)

    @throttle(60 * 5)
    def garbage_collect_classifiers(self):
        """
            The idea is to force the next record_classifiers to insert
            all of the classifiers that we are missing.  This ensures
            that we 'fix' any missing rows caused by garbage collection
            once every 5 minutes.
        """
        self._seen_classifiers = []
