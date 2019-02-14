#!/usr/bin/python3
# This script make couchdb databases replication from source instance to target instance.
# 

'''
    Couchdb replication script.
    Make couchdb databases replication from source instance to target instance.

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
        #list of databases for replication
        databases = []
        #created replication tasks counters
        c_count=0
        i_count=0
        t_count=0

        #replication tasks server
        r_couch = couchdb.Server(args.r_instance)
        #replication documents database
        r_db = r_couch['_replicator']
        #source instance
        source = (urllib.parse.urlparse(args.s_instance).netloc).rsplit('@',1)[1]
        s_couch = couchdb.Server(args.s_instance)
        #target instance
        target = (urllib.parse.urlparse(args.t_instance).netloc).rsplit('@',1)[1]
        t_couch = couchdb.Server(args.t_instance)

        if (args.i_filename and not os.path.isfile(args.i_filename)):
            print("\nFile \"%s\" not found... Exiting...\n" % args.i_filename )
            raise SysExit

        if (args.e_filename and not os.path.isfile(args.e_filename)):
            print("\nFile \"%s\" not found... Exiting...\n" % args.e_filename )
            raise SysExit

        #fill databases dict with all unique databases or only nonexistent
        if args.nonexistent:
            #find only databases which not exits on replication tasks instance
            #if include file not set
            if not args.i_filename:
                print("\nReplicate only nonexistent databases from \"%s\" to \"%s\" ...\nCreating list of nonexistent databases....\n" % (source,target))
                for db_name in s_couch:
                    if ( not re.match(r'_', db_name) and db_name not in t_couch ):
                        databases.append(urllib.parse.quote(db_name,safe=''))
                for db_name in databases:
                    print(db_name)
            #if include file set - just check existance of databases from file
            else:
                print("\nInclude list is set...\n")
                print("\nReplicate only nonexistent databases from \"%s\" to \"%s\" included in \"%s\" file ...\nCreating list of nonexistent databases....\n" % (source,target,args.i_filename))
                print("\nOnly following databases will be replicated if not exist:\n")
                f = open(args.i_filename)
                for db_name in f:
                    db_name = db_name.strip()
                    print ("\"%s\"" % urllib.parse.quote(db_name,safe=''))
                    if ( not re.match(r'_', db_name) and db_name not in t_couch ):
                        databases.append(urllib.parse.quote(db_name,safe=''))
                f.close()

            print ("\n%i databases from \"%s\" not exist on \"%s\" and allowed for replication ...\n" % (len(databases),source,target))
        else:
            #if include file not set
            if not args.i_filename:
                print("\nReplicate all databases from \"%s\" to \"%s\" ...\nCreating list of databases....\n" %(source,target))
                for db_name in s_couch:
                    if ( not re.match(r'_', db_name) ):
                        databases.append(urllib.parse.quote(db_name,safe=''))
            #if include file set - just check existance of databases from file
            else:
                print("\nInclude list is set...\n")
                print("\nReplicate databases from \"%s\" to \"%s\" included in \"%s\" file...\nCreating list of databases....\n" %(source,target,args.i_filename))
                print("\nOnly following databases will be replicated:\n")
                f = open(args.i_filename)
                for db_name in f:
                    db_name = db_name.strip()
                    print ("\"%s\"" % urllib.parse.quote(db_name,safe=''))    
                    if ( not re.match(r'_', db_name) ):
                        databases.append(urllib.parse.quote(db_name,safe=''))
                f.close()

            print ("\n%i databases allowed for replication from \"%s\" to \"%s\" ...\n" % (len(databases),source,target))
        #if exclude file is set
        if args.e_filename:
            print ("\nExlude list is set...\nFollowing databases will not be replicated:\n")
            f = open(args.e_filename)
            for db_name in f:
                db_name = db_name.strip()
                print ("\"%s\"" % urllib.parse.quote(db_name,safe=''))
                if ( not re.match(r'_', db_name) and urllib.parse.quote(db_name,safe='') in databases ):
                    databases.remove(urllib.parse.quote(db_name,safe=''))
            f.close()
            print ("\n%i databases allowed for replication from \"%s\" to \"%s\" ...\n" % (len(databases),source,target))

        #get all databases that already have repliction documents in replication database
        print("Check databases that already have active replication from \"%s\" to \"%s\" on replication server...\nCreating list of databases....\n" % (source,target))

        #check active replication documents in _replicator db
        for id in r_db:
            #init temp vars
            tmp_src_url = ""
            tmp_src_db_name = ""
            tmp_dst_url = ""
            tmp_dst_db_name = ""

            #if not service document
            if (not re.match(r'_', id)):
                #grab source,target and database name from replication document
                #if created from web-ui
                if ('url' in r_db[id]['source']):
                    tmp_src_url = (urllib.parse.urlparse(r_db[id]['source']['url'])).netloc
                    tmp_src_db_name = re.sub("/","",(urllib.parse.urlparse(r_db[id]['source']['url'])).path)
                    tmp_trg_url = (urllib.parse.urlparse(r_db[id]['target']['url'])).netloc
                    tmp_trg_db_name = re.sub("/","",(urllib.parse.urlparse(r_db[id]['target']['url'])).path)
                #if created from script
                elif ('@' in r_db[id]['source']):
                        tmp_src_url = ((urllib.parse.urlparse(r_db[id]['source'])).netloc).rsplit('@',1)[1]
                        tmp_src_db_name = re.sub("/","",(urllib.parse.urlparse(r_db[id]['source'])).path)
                        tmp_trg_url = ((urllib.parse.urlparse(r_db[id]['target'])).netloc).rsplit('@',1)[1]
                        tmp_trg_db_name = re.sub("/","",(urllib.parse.urlparse(r_db[id]['target'])).path)
                else:
                    print("Warning: replication document with \"%s\" id have unknown format...\n" % id)
            #if replication document variables not empty
            if (tmp_src_url and tmp_src_db_name and tmp_trg_url and tmp_trg_db_name):

                #if source and target instance match with parameters and source and target database name is equal
                if ((tmp_src_url == source) and (tmp_trg_url == target) and (tmp_src_db_name == tmp_trg_db_name)):
                    #if continuos replication set in document - task is always active
                    if (r_db[id]['continuous']):
                        #if database name from document exists in databases list, then delete it from there
                        if (tmp_src_db_name in databases):
                            databases.remove(tmp_src_db_name)
                            c_count +=1
                            print ("Database \"%s\" have continuous replication running through replication document with id \"%s\". Skipping...\n" % (tmp_src_db_name,id))
                        #if replication state is not completed
                    elif (r_db[id]['_replication_state'] != "completed"):
                        #if database name from document exists in databases list, then delete it from there
                        if (tmp_src_db_name in databases):
                            databases.remove(tmp_src_db_name)
                            i_count +=1
                            print ("Database \"%s\" have incomplete replication running through replication document with id \"%s\". Skipping...\n" % (tmp_src_db_name,id))

        print("%i databases have active continuous replication\n%i databases have incompleted one-time replication\n%i databases not in replication yet\n" % (c_count,i_count,len(databases)))

        print("%i databases from %i selected databases will be replicated..." % ((args.t_count if args.t_count < len(databases) else len(databases)),len(databases)))

        #start creating replication tasks or documents
        print("\nStarting replication tasks from  \"%s\" to \"%s\" ...\n" % (source,target))
        for db_name in databases:
            source_url = '%s/%s' % (args.s_instance,db_name)
            target_url = '%s/%s' % (args.t_instance,db_name)
            if args.continuous:
                r_db.save({'source': source_url, 'target': target_url, 'create_target':True,'continuous':True})
                t_count += 1
                print('%i/%i: Replication document for database \"%s\" created ...' % (t_count,(args.t_count if args.t_count < len(databases) else len(databases)),db_name))
            else:
                r_couch.replicate(source_url, target_url, create_target=True)
                t_count += 1
                print('%i/%i: Replication for database \"%s\" completed...' % (t_count,(args.t_count if args.t_count < len(databases) else len(databases)),db_name))
            if (t_count == args.t_count):
                break
        if args.continuous:
            print ('\n%i replication documents created succesfully.' % t_count)
        else:
            print ('\n%i databases replicated succesfully.' % t_count)

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
    lock = FileLock("/tmp/couchdb_replication")

    #create filter
    f = ContextFilter()

    #Logger settings    
    logger = logging.getLogger()
    logger.name = 'CouchdbReplication'
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
    usage = "\n\ncouchdb_replication.py [-R <instance URL> (Instance with replication tasks)] [-S <instance URL> (Source instance)] [-T <instance URL> (Target instance)]\n\n"
            "Optional arguments:\n\n" \
            "[--tcount <count> ] Optional replication tasks count. Default: 1.\n\n" \
            "[--continuous ]  Optional parameter. Set if you need cotinuous replication tasks.\n\n" \
            "[--nonexistent ]  Optional parameter. Set if you need to replicate only databases which not exists on target instance.\n\n" \
            "[--include-db-file] Optional parameter. Set filename if you need to replicate only databases listed in text file line by line.\n\n" \
            "[--exclude-db-file] Optional parameter. Set filename if you need to exlude from replication some databases listed in text file line by line.\n\n",
        description='Couchdb replication script')

    parser.add_argument(
        "-R",
        action = "store",
        type = str,
        dest = "r_instance",
        required = True,
        help = "Instances with port for replication tasks and documents",
    )

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
        "--tcount",
        action = "store",
        type = int,
        dest = "t_count",
        required = False,
        default = 1,
        help = "Optional number of replication task added. Default: 1. ",
    )

    parser.add_argument(
        "--continuous",
        action='store_true',
        dest = "continuous",
        required = False,
        help = "Optional parameter. Set if you need cotinuous replication tasks.",
    )

    parser.add_argument(
        "--nonexistent",
        action='store_true',
        dest = "nonexistent",
        required = False,
        help = "Optional parameter. Set if you need to replicate only databases which not exists on target instance.",
    )

    parser.add_argument(
        "--include-db-file",
        action='store',
        type = str,
        dest = "i_filename",
        required = False,
        help = "Optional parameter. Set filename if you need to replicate only databases listed in text file line by line.",
    )

    parser.add_argument(
        "--exclude-db-file",
        action='store',
        type = str,
        dest = "e_filename",
        required = False,
        help = "Optional parameter. Set filename if you need to exlude from replication some databases listed in text file line by line.",
    )

    args = parser.parse_args()

    main()
