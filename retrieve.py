#! /home/nfaggian/work/anaconda/envs/python-devel/bin/python

import xmlrpc.client
import datetime
import json
import os
import gzip

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
         {'python': python,
          'python_two': python_two,
          'python_three': python_three}}


def write_pypi_data(document, prefix='/home/nfaggian/data'):
    """
    Write data to the mongo database.
    """

    filename = 'pypi_{}.json'.format(
    datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    
    # Create the json file
    with open('{}/{}'.format(prefix, filename), 'w') as f:
        json.dump(document, f, ensure_ascii=True)
    
    # Create the compressed json file
    with open('{}/{}'.format(prefix, filename), 'rb') as f: 
        with gzip.open('{}/{}.gz'.format(prefix, filename),'wb') as gzf: 
            gzf.write(f.read())

    os.unlink('{}/{}'.format(prefix, filename))   

if __name__ == "__main__":

    document = collect_pypi_data()

    write_pypi_data(document)
