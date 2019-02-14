#!/usr/bin/python3
# This script remove databases listed in file  from couchdb instance.
# 

'''
    Couchdb databases remove script.
    Remove databases from instance

    Author: Egor Pavlov
    e-mail: xpm.pub@gmail.com'''


import json
import logging
import getpass
import argparse
import re
import os
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
        databases = []

        #instance
        instance = (urllib.parse.urlparse(args.instance).netloc).rsplit('@',1)[1]
        couch = couchdb.Server(args.instance)

        if not os.path.isfile(args.filename):
            print("\nFile \"%s\" not found... Exiting...\n" % args.filename )
            raise SysExit
        print ("\nReading databases list from file...\n")

        f = open(args.filename, "r")
        print ("\nFollowing databases will be removed from instance \"%s\"...\n" % instance)
        d_count = 0 
        for db_name in f:
            databases.append(urllib.parse.unquote(db_name.strip()))
            d_count += 1
            print("%i: %s" % (d_count,urllib.parse.unquote(db_name.strip())))

        print ("\n%i databases total will be removed from instance \"%s\".\n" % (len(databases),instance))
        submit = input("Are you sure?(y/n)")
        
        if (submit == "Y" or submit =="y" or submit =="Yes" or submit == "yes"):
            print ("\nStarting databases removing...\n")
            d_count = 0 
            for db_name in databases:
                if db_name in couch:
                    d_count += 1
                    couch.delete(db_name)
                    print("%i: Database \"%s\" removed succesfully from instance \"%s\"." % (d_count,db_name,instance))
                else:
                    print("Database \"%s\" not found in instance \"%s\". Skiping ..." % (db_name,instance))
            print ("\n%i databases from list in file \"%s\" removed from instance \"%s\"." % (d_count,args.filename,instance))
            print ("%i databases from list in file \"%s\" was not found in instance \"%s\" and skipped." % ((len(databases) - d_count),args.filename,instance))
        elif (submit == "N" or submit =="n" or submit =="No" or submit == "no"):
            print ("\nRemoving canceled. Exiting ...\n")
        else:
            print ("\nInvalid input. Exiting ...\n")
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
    lock = FileLock("/tmp/couchdb_db_remove")

    #create filter
    f = ContextFilter()

    #Logger settings    
    logger = logging.getLogger()
    logger.name = 'CouchdbDbRemove'
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
    usage = "\n\ncouchdb_db_remove.py [-I <instance URL> ] [-L <databases list file> ]\n\n",
        description='Couchdb databases remove script')

    parser.add_argument(
        "-I",
        action = "store",
        type = str,
        dest = "instance",
        required = True,
        help = "Instance with port",
    )

    parser.add_argument(
        "-L",
        action = "store",
        type = str,
        dest = "filename",
        required = True,
        help = "File with list of databases to remove",
    )

    args = parser.parse_args()

    main()
