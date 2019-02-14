#!/usr/bin/python3
# This script compare databases by name on two instances and show uniques.
# 

'''
    Couchdb databases compare script.
    Compare databases by name on two instances and show uniques

    Author: Egor Pavlov
    e-mail: xpm.pub@gmail.com'''


import json
import logging
import getpass
import argparse
import re
import urllib
import couchdb
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
        #lists of databases
        s_databases = []
        t_databases = []
        #created replication tasks counters
        s_count=0
        t_count=0

        #source instance
        source = (urllib.parse.urlparse(args.s_instance).netloc).rsplit('@',1)[1]
        s_couch = couchdb.Server(args.s_instance)
        #target instance
        target = (urllib.parse.urlparse(args.t_instance).netloc).rsplit('@',1)[1]
        t_couch = couchdb.Server(args.t_instance)

        print("\nCheck databases from \"%s\" which not exist on \"%s\" ...\nCreating list....\n" % (source,target))

        for db_name in s_couch:
            if ( not re.match(r'_', db_name) and db_name not in t_couch ):
                s_databases.append(urllib.parse.quote(db_name,safe=''))

        print("\nCheck databases from \"%s\" which not exist on \"%s\" ...\nCreating list....\n" % (target,source))

        for db_name in t_couch:
            if ( not re.match(r'_', db_name) and db_name not in s_couch ):
                t_databases.append(urllib.parse.quote(db_name,safe=''))

        if args.res_files:
            if (len(s_databases) > 0):
                s = open("%s.lst" % source, "w")
            if (len(t_databases) > 0):
                t = open("%s.lst" % target, "w")

        if (len(s_databases) > 0):
            print("\nFollowing databases from \"%s\" not exist on \"%s\" ...\n" % (source,target))
            for db_name in s_databases:
                if args.res_files:
                    s.write(db_name + '\n')
                print(db_name)

        if (len(t_databases) > 0):
            print("\nFollowing databases from \"%s\" not exist on \"%s\" ...\n" % (target,source))
            for db_name in t_databases:
                if args.res_files:
                    t.write(db_name + '\n')
                print(db_name)

        print("\n%i databases from \"%s\" not exist on \"%s\" ...\n" % (len(s_databases),source,target))
        print("\n%i databases from \"%s\" not exist on \"%s\" ...\n" % (len(t_databases),target,source))

        if args.res_files:
            print("\nWriting result files finished ...\n")
            if (len(s_databases) > 0):
                s.close()
            if (len(t_databases) > 0):
                t.close()

        #logger.info()

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
    lock = FileLock("/tmp/couchdb_db_compare")

    #create filter
    f = ContextFilter()

    #Logger settings    
    logger = logging.getLogger()
    logger.name = 'CouchdbDbCompare'
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
    usage = "\n\ncouchdb_db_compare.py [-S <instance URL> (Source instance)] [-T <instance URL> (Target instance)]\n\n"
            "Optional arguments:\n\n" \
            "[--result-to-files ] Optional parameter. Set if you need additionaly have output files.\n\n",
        description='Couchdb databases compare script')

    parser.add_argument(
        "-S",
        action = "store",
        type = str,
        dest = "s_instance",
        required = True,
        help = "Source instance with port",
    )

    parser.add_argument(
        "-T",
        action = "store",
        type = str,
        dest = "t_instance",
        required = True,
        help = "Target instance with port",
    )

    parser.add_argument(
        "--result-to-files",
        action='store_true',
        dest = "res_files",
        required = False,
        help = "Optional parameter. Set if you need additionaly have output files.",
    )

    args = parser.parse_args()

    main()
