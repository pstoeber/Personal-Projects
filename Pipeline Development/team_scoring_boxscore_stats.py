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
import time
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

    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]').click()
    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]').click()
    table = browser.find_element_by_class_name('nba-stat-table')

    for c, row in enumerate(table.text.split('\n')):
        row = row.split()
        if c < 16:
            columns += row
        if c > 16 and len(row) > 1:
            sub_row = (home_away_team_aligner(row[:4]))
            stats.append(sub_row + row[4:])
    columns = column_list_format(columns)
    print(columns)
    return pd.DataFrame(np.array(stats), index=None, columns=columns)

def column_list_format(columns):
    game_date = columns[columns.index('GAME')] + '_' + columns[columns.index('DATE')]
    pt2_fg_pct = columns[columns.index('%FGA')] + '_' + columns[columns.index('2PT')]
    pt3_fg_pct = columns[9] + '_' + columns[columns.index('3PT')]
    pts_2p_pct = columns[11] + '_' + columns[12]
    pts_2p_mr_pct = columns[13] + '_' + columns[14] + '_' + columns[15]
    pct_3p = columns[16] + '_' + columns[17]
    pct_pts_fbps = columns]18] + columns[19]
    pct_pts_ft = columns[20] + '_' + columns[21]
    pct_pts_off_to = columns[22] + '_' + columns[23]
    pct_pts_pitp = columns[24] + '_' + columns[25]
    fgm_2p_ast_pct = columns[26] + '_' + columns[27]
    fgm_2p_uast_pct = columns[27] + '_' + columns[28]
    fgm_3p_ast_pct = columns[29] + '_' + columns[30]
    fgm_3p_uast_pct = columns[31] + '_' + columns[32]
    fgm_ast_pct = columns[33] + '_' + columns[34]
    fgm_uast_pct = columns[35] + columns[36]
    return columns[:1] + ['HOME_TEAM', 'AWAY_TEAM'] + [game_date] + columns[5:7] + [pt2_fg_pct] + [pt3_fg_pct] + \
           [pts_2p_pct] + [pts_2p_mr_pct] + [pct_3p] + [pct_pts_fbps] + [pct_pts_ft] + [pct_pts_off_to] + [pct_pts_pitp] +\
           [pct_pts_pitp] + [fgm_2p_ast_pct] + [fgm_2p_uast_pct] + [fgm_3p_ast_pct] + [fgm_3p_uast_pct] + [fgm_ast_pct] + [fgm_uast_pct]


    #return 'TEAM HOME_TEAM AWAY_TEAM GAME_DATE W/L MIN %FGA_2PT	%FGA_3PT %PTS_2PT %PTS_2PT_MR %PTS_3PT %PTS_FBPS %PTS_FT %PTS_OFF_TO %PTS_PITP 2FGM_%AST 2FGM_%UAST 3FGM_%AST 3FGM_%UAST FGM_%AST FGM_%UAST'.split()

def home_away_team_aligner(row):

    if row[2] == '@':
        return [row[0]] + [row[3]] + [row[1]]
    else:
        return row[:2] + [row[3]]

def convert_date(date_str):
    return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()

if __name__ == '__main__':
    myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    link = 'https://stats.nba.com/teams/boxscores-scoring/'
    max_date = find_max_date(myConnection)
    stat_df = stat_scraper(link)
    stat_df['GAME_DATE'] = stat_df.loc[:, 'GAME_DATE'].apply(convert_date)
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))

    if stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].empty:
        print('No new data.')
        sys.exit(1)

    stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].to_sql(con=engine, name='team_scoring_boxscore_stats', if_exists='replace', index=False)

    print(stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].count())
