#!/usr/bin/python3
# This script make simple benchmark tests of couchdb instance.
# 

'''
    Couchdb benchmark test script.

    Author: Egor Pavlov
    e-mail: xpm.pub@gmail.com'''


import json
import logging
import getpass
import argparse
import re
import urllib
import couchdb
import string
import time
import requests
from random import *
from lockfile import FileLock, LockTimeout, AlreadyLocked
from logging import handlers
from urllib.parse import urlparse

#This is a filter which injects contextual information into the log.
class ContextFilter(logging.Filter):

    def filter(self, record):
        #get user which run this scripts
        record.user = getpass.getuser()
        return True

def gen_string(minlen, maxlen):
    try:
        allchar = string.ascii_letters + string.digits
        result = "".join(choice(allchar) for x in range(randint(minlen, maxlen)))
    except Exception as e:
        logger.info(type(e))
        logger.info('Error: %s' % e)
    finally:
        return result

def main():
    try:
        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=5)

        # variables for calculating summary values
        wtimesum = 0
        wtimeaverage = 0
        wdocsnum = 0
        rtimesum = 0
        rtimeaverage = 0
        rdocsnum = 0
        #replication tasks server
        couch = couchdb.Server(args.instance)
        #replication documents database
        if re.match("^[a-z][a-z0-9_\$\(\)+-/]*$", args.dbname):
            if args.dbname in couch:
                print('Database with name %s already exists in instance!' % args.dbname)
            else:

                if args.shardsnum:
                    r = requests.put('%s/%s?q=%i'% (args.instance,urllib.parse.quote(args.dbname,safe=''),args.shardsnum))
                else:
                    r = requests.put('%s/%s'% (args.instance,urllib.parse.quote(args.dbname,safe='')))

                if (r.status_code == 200) or (r.status_code == 201):
                    db = couch[args.dbname]

                    #write perfomance test
                    for docnum in range(args.docsnum):
                        data = {}
                        for fieldnum in range(args.fieldsnum):
                            data['field%i' % fieldnum ] = gen_string(8,12)
                        #calculating time need for writing document in instance
                        if args.wq:
                            ts = time.perf_counter()
                            response = db.save(data,w=args.wq)
                            tf = time.perf_counter()
                        else:
                            ts = time.perf_counter()
                            response = db.save(data)
                            tf = time.perf_counter()
                        t = tf-ts
                        #add spent time to summary if response contains list with two items (_id,_rev)
                        if len(response) == 2:
                            wtimesum += t
                            wdocsnum += 1
                    wtimeaverage = wtimesum/wdocsnum

                    #read perfomance test
                    for docid in db:
                        #calculating time need for reading document from instance
                        ts = time.perf_counter()
                        doc = db.get(docid,r=args.rq)
                        tf = time.perf_counter()
                        t = tf-ts
                        if len(doc) > 0:
                            rtimesum += t
                            rdocsnum += 1
                    rtimeaverage = rtimesum/rdocsnum


                    #read perfomance test
                    print ("\nPerfomance test summary:\n"
                           "========================================================\n\n"
                           "Write perfomance:\n"
                           "-----------------\n\n"
                           "Total number of documents written: %i\n"
                           "Total time duration for creating all documents:%.3fs\n"
                           "Average time duration for creating one document:%.3fs\n\n"

                           "Read perfomance:\n"
                           "-----------------\n\n"
                           "Total number of documents readed: %i\n"
                           "Total time duration for reading all documents:%.3fs\n"
                           "Average time duration for reading one document:%.3fs\n\n"
                           % (wdocsnum,wtimesum,wtimeaverage,rdocsnum,rtimesum,rtimeaverage))
                else:
                    print("Error creating database! Response status code: %i." % r.status_code)
        else:
            print('Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed in database name. Must begin with a letter.')


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
    lock = FileLock("/tmp/couchdb_benchmark")

    #create filter
    f = ContextFilter()

    #Logger settings    
    logger = logging.getLogger()
    logger.name = 'CouchdbBenchmark'
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
    usage = "\n\ncouchdb_benchmark.py [-I <instance URL>] [-D <database name>] [-N <number of documents>]\n\n"
            "Optional arguments:\n\n" \
            "[--fieldsnum <number> ] Optional number of fields in each documents. Default: 10.\n\n" \
            "[--shardsnum <number> ] Optional number of shards for test database.\n\n" \
            "[--wq <number> ] Optional write quorum for documents.\n\n" \
            "[--rq <number> ] Optional read quorum for documents.\n\n",
        description='Couchdb benchmark test')

    parser.add_argument(
        "-I",
        action = "store",
        type = str,
        dest = "instance",
        required = True,
        help = "Instance with port",
    )

    parser.add_argument(
        "-D",
        action = "store",
        type = str,
        dest = "dbname",
        required = True,
        help = "Test database name",
    )

    parser.add_argument(
        "--shardsnum",
        action = "store",
        type = int,
        dest = "shardsnum",
        required = False,
        help = "Optional number of shards for test database.",
    )

    parser.add_argument(
        "--wq",
        action = "store",
        type = int,
        dest = "wq",
        required = False,
        help = "Optional write quorum for documents.",
    )

    parser.add_argument(
        "--rq",
        action = "store",
        type = int,
        dest = "rq",
        required = False,
        help = "Optional read quorum for documents.",
    )

    parser.add_argument(
        "-N",
        action = "store",
        type = int,
        dest = "docsnum",
        required = True,
        help = "Number of documents to create",
    )

    parser.add_argument(
        "--fieldsnum",
        action = "store",
        type = int,
        dest = "fieldsnum",
        required = False,
        default = 10,
        help = "Optional number of fields in every document. Default: 10. ",
    )


    args = parser.parse_args()

    main()
