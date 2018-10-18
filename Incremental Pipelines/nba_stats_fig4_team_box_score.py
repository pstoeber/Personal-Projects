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

    #time.sleep()
    while True:
        try:
            browser.get(link)

            browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[2]').click() ## Change to option 1
            time.sleep(2)

            ########REMOVE TILL NEXT COMMENT LINE AFTER TESTING#############
            browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]').click()
            time.sleep(2)
            ################################################################

            browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]').click()
            time.sleep(2)

            break
        except:
            logging.info('[CONNECTION TIME-OUT]: re-trying four factor pipeline')

    header = browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[2]/div[1]/table/thead')
    columns = column_format(list(itertools.chain(*[i.split() for i in header.text.split('\n')])))
    table = browser.find_element_by_class_name('nba-stat-table')

    for c, row in enumerate(table.text.split('\n')):
        row = row.split()
        if len(row) > 3 and row[2] in ['vs.', '@']:
            sub_row = (home_away_team_aligner(row[:4]))
            stats.append(sub_row + row[4:])
    browser.close()
    return pd.DataFrame(np.array(stats), index=None, columns=columns)

def column_format(header_list):

    game_date = header_list[header_list.index('GAME')] + '_' + header_list[header_list.index('DATE')]
    fta_rate = header_list[header_list.index('FTA')] + '_' + header_list[header_list.index('RATE')]
    opp_egf = header_list[header_list.index('OPP')] + '_' + header_list[header_list.index('EFG%')]
    opp_fta_rate = '_'.join([i for i in header_list[-7:-4]])
    opp_tov = header_list[-4] + '_' + header_list[-3]
    opp_reb_pct = header_list[-2] + '_' + header_list[-1]
    return header_list[:1] + ['HOME_TEAM', 'AWAY_TEAM'] + [game_date] + header_list[5:8] + [fta_rate] + header_list[8:10] + [opp_egf] + [opp_fta_rate] + [opp_tov] + [opp_reb_pct]

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
    link = 'https://stats.nba.com/teams/boxscores-four-factors/'
    logging.info('Beginning NBA Stats four factors Team Stats incrementals pipeline {}'.format(str(datetime.datetime.now())))
    max_date = find_max_date(myConnection)
    #max_date = datetime.datetime.strptime('10/05/2018', '%m/%d/%Y').date()

    stat_df = stat_scraper(link)
    stat_df['GAME_DATE'] = stat_df.loc[:, 'GAME_DATE'].apply(convert_date)
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))

    if stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].empty:
        print('No new data.')
        sys.exit(1)

    stat_df[stat_df.loc[:, 'GAME_DATE'] > max_date].to_sql(con=engine, name='figure4_team_boxscore_stats', if_exists='replace', index=False)
    logging.info('Four Factors Dataframe Count: {}'.format(str(stat_df.count())))
    logging.info('NBA Stats four factors incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
