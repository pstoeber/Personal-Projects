"""
script used to create a lookup table mapping individual games to seasons within the QDM
"""
import numpy as np
import pandas as pd
import datetime
import logging
import pymysql
from functools import partial
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from sqlalchemy import create_engine

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats', autocommit=True)

def date_gen():
    date_list = []
    start = datetime.datetime.strptime('2018-1-1', '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime('2018-12-31', '%Y-%m-%d').date()

    while start <= end_date:
        date_list.append(str(start).split('-')[1:])
        start += datetime.timedelta(days=1)
    return date_list

def sql_execute(sql):
    exe = gen_db_conn().cursor()
    exe.execute(sql)
    return exe.fetchall()

def find_years():
    get_years = 'select year(game_date) from box_score_map group by year(game_date)'
    years = sql_execute(get_years)
    return [i[0] for i in years]

def gen_pools(years, dates):
    pool = Pool()
    results = pool.map(partial(gen_lookup, years=years), dates)
    pool.close()
    pool.join()
    return results

def gen_lookup(date, years):
    date_list = []
    for year in years:
        year_val = 0
        if int(date[0]) < 6:
            year_val = year
        else:
            year_val = year+1
        str_date = '{}-{}'.format(str(year), '-'.join([i for i in date]))
        date_list.append([datetime.datetime.strptime(str_date, '%Y-%m-%d').date(), year_val])
    return pd.DataFrame(np.array(date_list), index=None, columns=['day', 'season'])

def insert_into_database(df):
    engine = create_engine('mysql+pymysql://', creator=gen_db_conn)
    df.to_sql(con=engine, name='game_date_lookup', if_exists='replace', index=False)
    engine.dispose()
    return

def add_pk():
    sql = 'alter table game_date_lookup add primary key(day);'
    sql_execute(sql)
    return

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning generation of date lookup table {}'.format(str(datetime.datetime.now())))

    #conn = gen_db_conn()
    dates = date_gen()
    years = find_years()
    all_dates = gen_pools(years, dates)
    dates_lu = pd.concat(all_dates)
    insert_into_database(dates_lu)
    add_pk()
    logging.info('Date lookup table generated {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
