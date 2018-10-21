"""
script used to create a lookup table mapping individual games to seasons within the QDM
"""

import datetime
import logging
import pymysql
import re
from collections import defaultdict

def truncate_table(conn):
    drop = 'truncate table game_date_lookup'
    sql_execute(conn, drop)

def date_gen():
    date_list = []
    start = datetime.datetime.strptime('2018-1-1', '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime('2018-12-31', '%Y-%m-%d').date()
    step = datetime.timedelta(days=1)

    while start <= end_date:
        date = str(start).split('-')
        if date[1:] not in date_list:
            date_list.append(date[1:])
        start += step
    return date_list

def gen_lookup_dict(conn, date_list):
    exe = conn.cursor()
    get_years = 'select year(game_date) from box_score_map group by year(game_date)'
    years = sql_execute(conn, get_years)
    years = [i[0] for i in years]

    for year in years:
        for date in date_list:
            try:
                if int(date[0]) < 6:
                    exe.execute('insert into game_date_lookup values(str_to_date(\'' + '-'.join([str(i) for i in [year] + date]) + '\', \'%Y-%m-%d\'), ' + str(year) + ')')
                else:
                    exe.execute('insert into game_date_lookup values(str_to_date(\'' + '-'.join([str(i) for i in [year] + date]) + '\', \'%Y-%m-%d\'), ' + str(year+1) + ')')
            except:
                logging.info('[INVALID DATE]' + '-'.join([str(i) for i in [year] + date]))
def sql_execute(conn, query):
    exe = conn.cursor()
    exe.execute(query)
    return exe.fetchall()

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning generation of date lookup table {}'.format(str(datetime.datetime.now())))
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit=True)
    truncate_table(myConnection)
    date_list = date_gen()
    gen_lookup_dict(myConnection, date_list)
    logging.info('Date lookup table generated {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
