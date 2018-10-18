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
from collections import defaultdict
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from selenium import webdriver
from contextlib import closing

####### HOME TEAM WILL BE FIRST COLUMN, AWAY TEAM WILL BE SECOND COLUMN

def stat_scraper(link):

    columns, stats = [],[]
    chromeDriver = '/Users/Philip/Downloads/chromedriver'
    browser = webdriver.Chrome(executable_path=chromeDriver)
    browser.get(link)

    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]').click()
    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]').click()
    table = browser.find_element_by_class_name('nba-stat-table')

    for c, row in enumerate(table.text.split('\n')):
        row = row.split()

        #print(row)
        if c < 6:
            columns += row
        if c > 6 and len(row) > 1:
            sub_row = (home_away_team_aligner(row[:4]))
            #print(sub_row + row[4:])
            stats.append(sub_row + row[4:])
    columns = column_list_format(columns)
    print(columns)

    return pd.DataFrame(np.array(stats), index=None, columns=columns)

def column_list_format(columns):

    return 'TEAM HOME_TEAM AWAY_TEAM GAME_DATE W/L MIN EFG% FTA_RATE TOV% OREB% OPP_EFG% OPP_FTA_RATE OPP_TOV% OPP_OREB%'.split()

def home_away_team_aligner(row):

    if row[2] == '@':
        return [row[0]] + [row[3]] + [row[1]]
    else:
        return row[:2] + [row[3]]

### Main ###

myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
link = 'https://stats.nba.com/teams/boxscores-four-factors/'
stat_df = stat_scraper(link)
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))

stat_df.to_sql(con=engine, name='figure4_team_boxscore_stats', if_exists='append', index=False)

print(stat_df.count())
