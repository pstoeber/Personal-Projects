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
from collections import defaultdict
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from contextlib import closing

####### HOME TEAM WILL BE FIRST COLUMN, AWAY TEAM WILL BE SECOND COLUMN

def find_max_date(conn):
    exe = conn.cursor()
    exe.execute('select max(game_date) from nba_stats.box_score_map')
    return exe.fetchall()[0][0]

def stat_scraper(link):

    columns, stats = [],[]
    chromeDriver = '/Users/Philip/Downloads/chromedriver'
    browser = webdriver.Chrome(executable_path=chromeDriver)
    browser.get(link)

    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[2]').click()
    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]').click()
    table = browser.find_element_by_class_name('nba-stat-table')

    for c, row in enumerate(table.text.split('\n')):
        row = row.split()
        if c < 2:
            columns += row
        if c > 1 and len(row) > 1:
            sub_row = (home_away_team_aligner(row[:4]))
            stats.append(sub_row + row[4:])
    columns = column_list_format(columns)
    return pd.DataFrame(np.array(stats), index=None, columns=columns)

def column_list_format(columns):

    game_date = columns[columns.index('GAME')] + '_' + columns[columns.index('DATE')]
    ast_ratio = columns[columns.index('AST')] + '_' + columns[columns.index('RATIO')]
    columns.pop(columns.index('UP'))
    columns.pop(columns.index('MATCH'))
    return columns[:1] + ['HOME_TEAM', 'AWAY_TEAM'] + [game_date] + columns[3:10] + [ast_ratio] + columns[12:]

def home_away_team_aligner(row):

    if row[2] == '@':
        return [row[0]] + [row[3]] + [row[1]]
    else:
        return row[:2] + [row[3]]

def convert_date(date_str):
    return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()

if __name__ == '__main__':

    myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)

    link = 'https://stats.nba.com/teams/boxscores-advanced/'
    max_date = find_max_date(myConnection)

    stat_df = stat_scraper(link)
    stat_df['GAME_DATE'] = stat_df.loc[:, 'GAME_DATE'].apply(convert_date)
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))

    if stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].empty:
        print('No new data.')
        sys.exit(1)

    stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].to_sql(con=engine, name='advanced_team_box_scores', if_exists='append')

    print(stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].count())
