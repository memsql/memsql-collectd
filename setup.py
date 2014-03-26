#!/usr/bin/env python

from setuptools import setup

# get version
from memsql_collectd import __version__

setup(
    name='memsql-collectd',
    version=__version__,
    author='MemSQL',
    author_email='support@memsql.com',
    url='http://github.com/memsql/memsql-collectd',
    license='LICENSE.txt',
    description='The official MemSQL collectd plugin.',
    long_description=open('README.rst').read(),
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    packages=[ 'memsql_collectd' ],
    zip_safe=False,
    install_requires=['wraptor==0.5.0', 'netifaces==0.8', 'memsql==2.11.0']
)
