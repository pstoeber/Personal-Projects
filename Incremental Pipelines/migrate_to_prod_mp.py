"""
python3 migrate_to_prod_mp.py "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/production_insert_statements/primary_queries" "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/production_insert_statements/multithread"
"""

import pymysql
import itertools
import codecs
import sys
import os
import re
import logging
import datetime
from functools import partial
from multiprocessing import Pool
from multiprocessing import set_start_method
from multiprocessing.dummy import Pool as ThreadPool

def gen_timestamp():
    return str(datetime.datetime.now())

def find_files(dir):
    file_list = []
    for root, dir, files in os.walk(dir):
        for file in files:
            if not re.search(r'.DS_Store', file):
                file_list.append(os.path.join(root, file))
    return file_list

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod', autocommit=True)

def extract_file(file):
    with open(file, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_cmd_str(cmd):
    return r''.join([i for i in cmd])

def sql_execute(file, flag):
    conn = gen_db_conn()
    exe = conn.cursor()
    if flag == 1:
        exe.execute('set session sql_mode = "NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER";')
        exe.execute(gen_cmd_str(extract_file(file)))
        exe.execute('set session sql_mode = default;')
    else:
        exe.execute(gen_cmd_str(extract_file(file)))
    exe.close()
    logging.info('SQL executed successfully: {file} at {stamp}'.format(file=file, stamp=gen_timestamp()))
    return

def gen_threads(files, flag):
    pool = Pool()
    results = pool.map(partial(sql_execute, flag=flag), files)
    pool.close()
    pool.join()
    return

def main(arg1, arg2):
    logging.basicConfig(filename='algo_refresh_log.log', filemode='a', level=logging.INFO)
    logging.info('Algo refresh intialized {stamp}'.format(stamp=gen_timestamp()))
    set_start_method('forkserver', force=True)
    primary_files = find_files(arg1)
    for file in primary_files:
        sql_execute(file, 0)

    multi_thread_files = find_files(arg2)
    gen_threads(multi_thread_files, 1)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
