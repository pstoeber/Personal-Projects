"""
script used to create a lookup table mapping individual games to seasons within the QDM
"""

import datetime
import pymysql
import re
from collections import defaultdict

def date_gen():

    date_list = []
    start_date = '2010-1-01' #2014-8-30
    end_date = '2010-12-31' #2018-6-30
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    step = datetime.timedelta(days=1)

    while start <= end:

        if start.date() < datetime.date(2010, 5, 30) or start.date() > datetime.date(2010, 10, 15):
            date = str(start.date()).split('-')
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
            if int(date[0]) < 6:
                exe.execute('insert into game_date_lookup values(str_to_date(\'' + '-'.join([str(i) for i in [year] + date]) + '\', \'%Y-%m-%d\'), ' + str(year) + ')')
            else:
                exe.execute('insert into game_date_lookup values(str_to_date(\'' + '-'.join([str(i) for i in [year -1] + date]) + '\', \'%Y-%m-%d\'), ' + str(year) + ')')

def sql_execute(conn, query):

    exe = conn.cursor()
    exe.execute(query)
    return exe.fetchall()

### main

myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit=True)

date_list = date_gen()
gen_lookup_dict(myConnection, date_list)
