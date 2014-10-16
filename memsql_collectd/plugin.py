try:
    import collectd
except:
    pass

from memsql_collectd import __version__
from memsql.common.random_aggregator_pool import RandomAggregatorPool
from memsql.common.connection_pool import PoolConnectionException
from memsql_collectd import cluster, util
from memsql_collectd.analytics import AnalyticsCache, AnalyticsRow
from wraptor.decorators import throttle
import math
import re
import threading
import time
import os
import subprocess
import copy
import signal
import traceback

STATS_REPORT_INTERVAL = 5

# This is a data structure shared by every callback. We store all the
# configuration and globals in it.
class _AttrDict(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, val):
        self[name] = val

MEMSQL_DATA = _AttrDict(
    typesdb={},
    values_cache=AnalyticsCache(),
    node=None,
    counters=_AttrDict(stats=0, blacklisted=0, whitelisted=0, last_report=0),
    exiting=False
)

MEMSQL_DATA.config = _AttrDict(
    host=None,
    port=3306,
    user='root',
    password='',
    database='dashboard',
    memsqlnode=None,
    prefix=None,
    blacklist=set(),
    whitelist=set(),
    dfblacklist=True,
    derive=True,
    clusterhostname=None,
    skipdiskusage=False,
    skipcolumnardiskusage=False,
)

###################################
# Lifecycle functions

def memsql_config(config, data):
    """ Handle configuring collectd-memsql. """
    parsed_config = {
        'blacklist': set(),
        'whitelist': set()
    }
    for child in config.children:
        key = child.key.lower()
        val = child.values[0]
        if key in ('blacklist', 'whitelist'):
            parsed_config[key].add(val)
        else:
            if isinstance(val, (str, unicode)) and val.lower() in 'yes,true,on'.split(','):
                val = True
            elif isinstance(val, (str, unicode)) and val.lower() in 'no,false,off'.split(','):
                val = False
            parsed_config[key] = val

    for config_key in data.config:
        if config_key in ('blacklist', 'whitelist'):
            data.config[config_key] |= parsed_config[config_key]
        elif config_key in parsed_config:
            data.config[config_key] = parsed_config[config_key]

    if 'typesdb' in parsed_config:
        memsql_parse_types_file(parsed_config['typesdb'], data)

def memsql_init(data):
    """ Handles initializing collectd-memsql. """

    # handle SIGCHLD normally for subprocesses
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    # verify we have required params
    assert len(data.typesdb) > 0, 'At least 1 TypesDB file path must be specified'
    assert data.config.host is not None, 'MemSQL host is not defined'
    assert data.config.port is not None, 'MemSQL port is not defined'
    if data.config.dfblacklist:
        data.config.blacklist |= set(['df.dev', 'df.dev-shm', 'df.run', 'df.run-lock', 'df.run-shm', 'df.proc'])
    for banned in data.config.blacklist:
        assert ' ' not in banned

    collectd.info('Initializing collectd-memsql %s with %s:%s' % (__version__, data.config.host, data.config.port))

    # initialize the aggregator pool
    data.pool = RandomAggregatorPool(
        host=data.config.host,
        port=int(data.config.port),
        user=data.config.user,
        password=data.config.password,
        database=data.config.database
    )

    data.config.blacklist = _compile_filter_re(data.config.blacklist)
    data.config.whitelist = _compile_filter_re(data.config.whitelist)

    try:
        # initialize a memsql node here if we can
        if data.config.memsqlnode or data.config.memsqlnode is None:
            throttled_find_node(data)
            if data.node is not None:
                data.node.connect().close()

        # initialize a pool connection
        data.pool.connect().close()
    except PoolConnectionException:
        pass

    # initialize the flushing thread
    data.flusher = FlushWorker(data)
    data.flusher.start()

    # initialize disk usage worker
    data.disk_crawler = DiskUsageWorker(data)
    data.disk_crawler.start()

def memsql_shutdown(data):
    """ Handles terminating collectd-memsql. """
    collectd.info('Terminating collectd-memsql')
    data.exiting = True

    collectd.info('Closing MemSQL connections...')
    data.pool.close()

    collectd.info('Terminating threads...')
    data.flusher.terminate()
    data.disk_crawler.terminate()

    collectd.info('Waiting for data flusher to exit...')
    data.flusher.join()
    collectd.info('Waiting for disk crawler to exit...')
    data.disk_crawler.join()

#########################
# Read/Write functions

def memsql_read(data):
    if data.exiting:
        return

    # update our node reference at least once every 60 seconds incase things change
    if data.config.memsqlnode or data.config.memsqlnode is None:
        throttled_find_node(data)

    if data.node is not None:
        memsql_status = collectd.Values(plugin="memsql", plugin_instance="status", type="gauge")
        for name, value in data.node.status():
            memsql_status.dispatch(type_instance=name, values=[value])

            if name in cluster.COUNTER_STATUS_VARIABLES:
                memsql_status.dispatch(type="counter", type_instance=name, values=[value])

        memsql_read_facts(data)
        memsql_read_disk_info(data, memsql_status)

@throttle(60)
def memsql_read_facts(data):
    if data.node.alias is not None:
        now = util.sql_utcnow()
        variables = list(data.node.variables())
        num_rows = len(variables)
        variables = sum([
            [now, data.node.alias, "memsql", "variables", gamma, value]
            for gamma, value in variables
        ], [])
        variables.append(now)

        if num_rows > 0:
            tmpl = ["(%s, %s, %s, %s, %s, %s)"]
            with data.pool.connect() as conn:
                sql = '''
                    INSERT INTO facts (last_updated, instance_id, alpha, beta, gamma, value) VALUES %s
                    ON DUPLICATE KEY UPDATE last_updated=%%s, value=VALUES(value)
                ''' % ",".join(tmpl * num_rows)
                conn.execute(sql, *variables)

@throttle(60)
def memsql_read_disk_info(data, memsql_status):
    memsql_dirs = data.node.directories()

    data_dir = memsql_dirs.get('Data_directory', '')
    dirs = {
        'data': data_dir,
        'segments': memsql_dirs.get('Segments_directory', os.path.join(data_dir, 'columns')),
        'logs': memsql_dirs.get('Logs_directory', os.path.join(data_dir, 'logs')),
        'snapshots': memsql_dirs.get('Snapshots_directory', os.path.join(data_dir, 'snapshots')),
        'plancache': memsql_dirs.get('Plancache_directory', os.path.join(data_dir, 'plancache'))
    }

    for label in dirs.keys():
        path = dirs[label]
        if not path or not os.path.exists(path) or not os.path.isdir(path):
            del dirs[label]

    if 'data' not in dirs or 'plancache' not in dirs:
        collectd.info("Can't find data or plancache directory.")
        return

    mounts = set()
    total_used = 0
    total_avail = 0
    for label, path in dirs.iteritems():
        paths = [path]
        try:
            # get columnar subdirs
            if label == 'segments':
                paths += os.listdir(path)

            with open(os.devnull, 'w') as devnull:
                p = subprocess.Popen(['df', '-k', '-P'] + paths, stderr=devnull, stdout=subprocess.PIPE)
                out = p.communicate()[0]

            for line in out.strip().split('\n')[1:]:
                fs, blocks, used, avail, percent, mount = line.strip().split(None, 5)
                if mount not in mounts:
                    mounts.add(mount)
                    total_used += int(used) * 1024
                    total_avail += int(avail) * 1024

        except (OSError, IndexError, ValueError) as e:
            collectd.info('Error in memsql_read_disk_info: ' + str(e))
            collectd.info(traceback.format_exc())

    memsql_status.dispatch(type_instance='used_storage_bytes', values=[total_used])
    memsql_status.dispatch(type_instance='available_storage_bytes', values=[total_avail])

    data.disk_crawler.update_dirs(dirs)
    for label, size in data.disk_crawler.disk_usage().iteritems():
        memsql_status.dispatch(type_instance=label + '_storage_bytes', values=[size])

def memsql_write(collectd_sample, data):
    """ Write handler for collectd.
    This function is called per sample taken from every plugin.  It is
    parallelized among multiple threads by collectd.
    """
    if data.exiting:
        return

    if data.node is not None:
        throttled_update_alias(data, collectd_sample)

    # get the value types for this sample
    types = data.typesdb
    if collectd_sample.type not in types:
        collectd.info('memsql_writer: do not know how to handle type %s. do you have all your types.db files configured?' % collectd_sample.type)
        return

    value_types = types[collectd_sample.type]

    if len(value_types) != len(collectd_sample.values):
        collectd.info('memsql_writer: differing number of values for type %s' % collectd_sample.type)
        return

    # for each value in this sample, insert it into the cache
    for (value_type, value) in zip(value_types, collectd_sample.values):
        cache_value(value, value_type[0], value_type[1], collectd_sample, data)

#########################
# Disk Usage thread

class DiskUsageWorker(threading.Thread):
    def __init__(self, data):
        super(DiskUsageWorker, self).__init__()
        self.data = data
        self._stop = threading.Event()
        self._disk_usage = {}
        self._disk_usage_lock = threading.RLock()

    def update_dirs(self, dirs):
        with self._disk_usage_lock:
            for label, path in dirs.iteritems():
                if label in self._disk_usage:
                    self._disk_usage[label]['path'] = path
                else:
                    self._disk_usage[label] = { 'path': path, 'bytes': None }
            for label in self._disk_usage:
                if label not in dirs:
                    del self._disk_usage[label]

    def disk_usage(self):
        out = {}
        with self._disk_usage_lock:
            for label, info in self._disk_usage.iteritems():
                if info['bytes'] is not None:
                    out[label] = info['bytes']
        return out

    def run(self):
        if self.data.config.skipdiskusage:
            return

        while not self._stop.isSet():
            time.sleep(5)

            if self.data.node is None:
                continue

            with self._disk_usage_lock:
                disk_usage = copy.deepcopy(self._disk_usage)

            for label, info in disk_usage.iteritems():
                if self.data.config.skipcolumnardiskusage and label == "segments":
                    continue

                try:
                    with open(os.devnull, 'w') as devnull:
                        p = subprocess.Popen(['du', '-k', '-s', info['path']], stderr=devnull, stdout=subprocess.PIPE)
                        out = p.communicate()[0]

                    size, _ = out.strip().split(None, 1)
                    size = int(size) * 1024

                    with self._disk_usage_lock:
                        if label in self._disk_usage:
                            self._disk_usage[label]['bytes'] = size
                except (OSError, IndexError, ValueError) as e:
                    collectd.info('Error in DiskUsageWorker: ' + str(e))
                    collectd.info(traceback.format_exc())
                    with self._disk_usage_lock:
                        if label in self._disk_usage:
                            self._disk_usage[label]['bytes'] = None

    def terminate(self):
        self._stop.set()

#########################
# Flushing thread

class FlushWorker(threading.Thread):
    def __init__(self, data):
        super(FlushWorker, self).__init__()
        self.data = data
        self._stop = threading.Event()

    def run(self):
        while not self._stop.isSet():
            start = time.time()
            try:
                self.data.values_cache.flush(self.data.pool)
            except Exception as e:
                collectd.error(str(e))

            # sleep up till the start of the next second
            sleep_time = math.floor(start + 1) - start
            time.sleep(sleep_time)

    def terminate(self):
        self._stop.set()

#########################
# Utility functions

@throttle(60)
def throttled_update_alias(data, collectd_sample):
    host = collectd_sample.host.replace('.', '_')
    data.node.update_alias(data.pool, host)

@throttle(60)
def throttled_find_node(data):
    node = cluster.find_node(data.pool, data.config.clusterhostname, data.config.memsqlnode)
    if data.node is not None and node is not None:
        # merge node details
        data.node.update_from_node(node)
    else:
        data.node = node

    if data.node is not None:
        collectd.info('I am a MemSQL node: %s:%s' % (data.node.host, data.node.port))

def memsql_parse_types_file(path, data):
    """ This function tries to parse a collectd compliant types.db file.
    Basically stolen from collectd-carbon.
    """
    types = data.typesdb

    f = open(path, 'r')

    for line in f:
        fields = line.split()
        if len(fields) < 2:
            continue

        type_name = fields[0]

        if type_name[0] == '#':
            continue

        v = []
        for ds in fields[1:]:
            ds = ds.rstrip(',')
            ds_fields = ds.split(':')

            if len(ds_fields) != 4:
                collectd.info('memsql_writer: cannot parse data source %s on type %s' % ( ds, type_name ))
                continue

            v.append(ds_fields)

        types[type_name] = v

    f.close()

def _compile_filter_re(filter_list):
    """Given a list of classifiers, like ['cpu.*', '*.r-kelly', '*carl*.*carl*'],
       return a regular expression which matches all strings which have prefixes in this
       list (where * is interpreted as [^\.]*?)
       In this case:
       \A(
         cpu\.[^\.]*?
        |[^\.]*?\.r\-kelly
        |[^\.]*?carl[^\.]*?\.[^\.]*?carl[^\.]*?
       )[\.\Z].*
    """
    if not filter_list:
        return None
    return re.compile(r'\A(' +
        '|'.join(re.escape(item).replace(r'\*', r'[^\.]*?') for item in filter_list)
        + r')(\.|\Z)')

def _blacklisted(data, classifier_str):
    return data.config.blacklist and data.config.blacklist.match(classifier_str)

def _whitelisted(data, classifier_str):
    return (not data.config.whitelist) or data.config.whitelist.match(classifier_str)


def cache_value(new_value, data_source_name, data_source_type, collectd_sample, data):
    """ Cache a new value into the analytics cache.  Handles converting
    COUNTER and DERIVE types.
    """

    data.counters.stats += 1
    now = time.time()
    if (now - data.counters.last_report > STATS_REPORT_INTERVAL):
        data.counters.last_report = now
        collectd.info("%d stats collected, %d excluded by whitelist, %d blacklisted" % (
            data.counters.stats,
            data.counters.whitelisted,
            data.counters.blacklisted
        ))

    temp_classifier = [
        data.config.prefix,         # alpha... (None's will be filtered)
        collectd_sample.plugin,
        collectd_sample.plugin_instance,
        collectd_sample.type,
        collectd_sample.type_instance,
        data_source_name,
    ]

    # we do this since '.' could be inside one of the classifier parts (like plugin, plugin_instance, etc)
    classifier_str = '.'.join(filter(None, temp_classifier))
    classifier = classifier_str.split('.')

    if not _whitelisted(data, classifier_str):
        data.counters.whitelisted += 1
        return

    if _blacklisted(data, classifier_str):
        data.counters.blacklisted += 1
        return

    instance_id = collectd_sample.host
    if instance_id is not None:
        instance_id = instance_id.replace('.', '_')

    new_row = AnalyticsRow(
        int(collectd_sample.time),
        new_value,
        instance_id,
        *classifier
    )

    previous_row = data.values_cache.swap_row(new_row)

    if data_source_type not in ['ABSOLUTE', 'COUNTER', 'DERIVE', 'GAUGE']:
        collectd.info("MemSQL: Undefined data source %s" % data_source_type)
        return

    # special case counter and derive since we only care about the
    # *difference* between their values (rather than their actual value)
    if data_source_type in ['ABSOLUTE', 'GAUGE'] or not data.config.derive:
        new_row.value = new_value
    elif data_source_type in ['COUNTER', 'DERIVE']:
        if previous_row is None:
            return

        old_value = previous_row.raw_value

        # don't let the time delta fall below 1
        time_delta = float(max(1, new_row.created - previous_row.created))

        # The following formula handles wrapping COUNTER data types
        # around since COUNTERs should never be negative.
        # Taken from: https://collectd.org/wiki/index.php/Data_source
        if data_source_type == 'COUNTER' and new_value < old_value:
            if old_value < 2 ** 32:
                new_row.value = (2 ** 32 - old_value + new_value) / time_delta
            else:
                new_row.value = (2 ** 64 - old_value + new_value) / time_delta

            if collectd_sample.plugin == 'cpu' and collectd_sample.type == 'cpu' and collectd_sample.type_instance == 'wait':
                # in virtualized environments, iowait sometimes wraps around and then back
                if new_row.value > (2 ** 31):
                    new_row.raw_value = previous_row.raw_value
                    new_row.value = previous_row.value
        else:
            # the default wrap-around formula
            new_row.value = (new_value - old_value) / time_delta

######################
# Register callbacks

try:
    collectd.register_config(memsql_config, MEMSQL_DATA)
    collectd.register_init(memsql_init, MEMSQL_DATA)
    collectd.register_write(memsql_write, MEMSQL_DATA)
    collectd.register_shutdown(memsql_shutdown, MEMSQL_DATA)
    collectd.register_read(memsql_read, 1, MEMSQL_DATA)
except:
    # collectd not available
    pass
