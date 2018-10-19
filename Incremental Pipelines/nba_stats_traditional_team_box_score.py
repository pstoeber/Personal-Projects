"""
scraper designed to gather team stats from individual games from
stats.nba.com/teams/boxscores-advanced/
"""

import re
import datetime
import itertools
import numpy as np
import pandas as pd
import pymysql
import requests
import sys
import time
import logging
from collections import defaultdict
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from contextlib import closing

####### HOME TEAM WILL BE FIRST COLUMN, AWAY TEAM WILL BE SECOND COLUMN

def find_max_date(conn):
    exe = conn.cursor()
    ##UNCOMMENT AFTER TESTING##
    #exe.execute('select max(game_date) from nba_stats.box_score_map')
    exe.execute('select max(game_date) from nba_stats_backup.box_score_map')
    return exe.fetchall()[0][0]

def stat_scraper(link):

    columns, stats = [],[]
    chromeDriver = '/Users/Philip/Downloads/chromedriver'
    browser = webdriver.Chrome(executable_path=chromeDriver)

    #time.sleep(8)
    while True:
        try:
            browser.get(link)

            browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]').click() ## Change to option 1
            time.sleep(2)

            ########REMOVE TILL NEXT COMMENT LINE AFTER TESTING#############
            #browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]').click()
            #time.sleep(15)
            ################################################################

            browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]').click()
            time.sleep(2)
            break
        except:
            logging.info('[CONNECTION TIME-OUT]: re-trying traditional pipeline')

    table = browser.find_element_by_class_name('nba-stat-table')

    for c, row in enumerate(table.text.split('\n')):
        row = row.split()
        if c < 1:
            columns += row
        if c >= 1 and len(row) > 1:
            sub_row = (home_away_team_aligner(row[:4]))
        #    print(sub_row + row[4:])
            stats.append(sub_row + row[4:])
    columns = column_list_format(columns)
    browser.quit()
    return pd.DataFrame(np.array(stats), index=None, columns=columns)

def column_list_format(columns):

    game_date = columns[columns.index('GAME')] + '_' + columns[columns.index('DATE')]
    #ast_ratio = columns[columns.index('AST')] + '_' + columns[columns.index('RATIO')]
    columns.pop(columns.index('UP'))
    columns.pop(columns.index('MATCH'))
    return columns[:1] + ['HOME_TEAM', 'AWAY_TEAM'] + [game_date] + columns[3:]

def home_away_team_aligner(row):

    if row[2] == '@':
        return [row[0]] + [row[3]] + [row[1]]
    else:
        return row[:2] + [row[3]]

def convert_date(date_str):
    return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    link = 'https://stats.nba.com/teams/boxscores-traditional/'
    logging.info('Beginning NBA Stats Traditional Team Stats incrementals pipeline {}'.format(str(datetime.datetime.now())))
    max_date = find_max_date(myConnection)
    stat_df = stat_scraper(link)
    stat_df['GAME_DATE'] = stat_df.loc[:, 'GAME_DATE'].apply(convert_date)
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))

    if stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].empty:
        print('No new data.')
        sys.exit(1)

    stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].to_sql(con=engine, name='traditional_team_boxscore_stats', if_exists='append', index=False)
    logging.info('Traditional Stats Dataframe Count: {}'.format(str(stat_df.count())))
    logging.info('NBA Stats Traditional Team Stats incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
