#! /home/nfaggian/anaconda/envs/python-devel/bin/python

import pymongo
import xmlrpc.client
import datetime

from pymongo import MongoClient

def json_map(data):
    """
    Forms mongo friendly documents.
    """
    json_data = {}
    for classifier, entries in data.items():
       json_data[classifier.replace('.','_')] = [{'package':p, 'version':v} for p, v in entries]
    return json_data


def collect_pypi_data():
    """
    Connect to pypi and retrieve data.
    """

    rclient = xmlrpc.client.ServerProxy('http://pypi.python.org/pypi')
    python = {'Programming Language :: Python': rclient.browse(['Programming Language :: Python'])}
    python_two = {}
    python_three = {}

    for classifier in [
     'Programming Language :: Python :: 2',
     'Programming Language :: Python :: 2.3',
     'Programming Language :: Python :: 2.4',
     'Programming Language :: Python :: 2.5',
     'Programming Language :: Python :: 2.6',
     'Programming Language :: Python :: 2.7',
     'Programming Language :: Python :: 2 :: Only']:
        python_two[classifier] = rclient.browse([classifier])

    for classifier in [
     'Programming Language :: Python :: 3',
     'Programming Language :: Python :: 3.0',
     'Programming Language :: Python :: 3.1',
     'Programming Language :: Python :: 3.2',
     'Programming Language :: Python :: 3.3',
     'Programming Language :: Python :: 3.4']:
        python_three[classifier] = rclient.browse([classifier])

    return {datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'):
         {'python': json_map(python),
          'python_two': json_map(python_two),
          'python_three': json_map(python_three)}}


def write_pypi_data(document):
    """
    Write data to the mongo database.
    """

    with MongoClient() as mclient:

        db = mclient.pypi
        collection = db.python_stats
        collection.insert(document)


if __name__ == "__main__":

    document = collect_pypi_data()

    write_pypi_data(document)
