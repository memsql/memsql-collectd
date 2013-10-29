===========
MemSQL Collectd Plugin
===========

This libary implements a MemSQL Ops specific plugin for `collectd`.

Install
=======

.. code:: bash

    pip install memsql-collectd

Then add the following plugin to your collectd configuration:

.. code:: xml

    <LoadPlugin python>
        Globals true
    </LoadPlugin>

    <Plugin python>
        Import "memsql_collectd.plugin"
        <Module "memsql_collectd.plugin">
            TypesDB "/usr/share/collectd/types.db"
            Host "MASTER AGGREGATOR HOSTNAME/IP_ADDRESS"
            Port "3306"
            MemSQLNode True
        </Module>
    </Plugin>
