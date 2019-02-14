#!/usr/bin/python3
# This script show file sizes of all databases in couchdb instance.

'''
    Couchdb database file size script.
    show file sizes of all databases in couchdb instance.

    Author: Egor Pavlov
    e-mail: xpm.pub@gmail.com'''


import json
import logging
import getpass
import argparse
import re
import urllib
import couchdb
import operator
from lockfile import FileLock, LockTimeout, AlreadyLocked
from logging import handlers
from urllib.parse import urlparse

#This is a filter which injects contextual information into the log.
class ContextFilter(logging.Filter):

    def filter(self, record):
        #get user which run this scripts
        record.user = getpass.getuser()
        return True


def main():
    try:
        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=5)
        #dict of databases names and source instances
        databases = {}

        #get all unique database names from all instances and append them to dict
        couch = couchdb.Server(args.instance)
        for dbname in couch:
            size=int((couch[dbname].info())["sizes"]["file"])/1024/1024
            if (size >= args.minsize):
                databases.update({urllib.parse.quote(dbname,safe=''):size})

        databases_sorted = sorted(databases, key=lambda x: databases[x])
        for dbname in databases_sorted:
            print("Name: %s | Size: %i Mb" % (dbname, databases[dbname]))

    except LockTimeout:
        logger.info('Lock not acquired, exiting')
    except AlreadyLocked:
        logger.info('Already locked, exiting')
    except Exception as e:
        logger.info(type(e))
        logger.info('Error: %s' % e)
    finally:

        # Release the lock
        if lock.i_am_locking():
            lock.release()

if __name__ == '__main__':

    # The script must not be executed simultaneously
    lock = FileLock("/tmp/couchdb_db_sizes")

    #create filter
    f = ContextFilter()

    #Logger settings    
    logger = logging.getLogger()
    logger.name = 'CouchdbSizes'
    logger.setLevel(logging.INFO)

    # add handler to the logger
    handler = logging.handlers.SysLogHandler(address='/dev/log')

    # add formatter to the handler
    formatter = logging.Formatter('%(name)s[%(process)d]: (%(user)s) %(levelname)s %(message)s')

    handler.formatter = formatter
    logger.addHandler(handler)
    logger.addFilter(f)

    #Parser settings
    parser = argparse.ArgumentParser(
    usage = "\n\ncouchdb_db_sizes.py [-I <instance URL>]\n\n"
            "Optional arguments:\n\n" \
            "--min-size <int> Minimal database size to show in Mb. Default: 100 Mb\n\n",
        description='Couchdb database sizes script')

    parser.add_argument(
        "-I",
        action = "store",
        type = str,
        dest = "instance",
        required = True,
        help = "Instance with port URL and auth credentials"
    )

    parser.add_argument(
        "--min-size",
        action = "store",
        type = int,
        dest = "minsize",
        required = False,
        default = int(100),
        help = "Minimal size of database in Mb to list. Default: 100 Mb"
    )

    args = parser.parse_args()

    main()
