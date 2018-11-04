import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import pymysql
import itertools
import datetime
import logging
from operator import itemgetter
from sqlalchemy import create_engine

def season_link_scraper(today):
    start_season = datetime.datetime.strptime('2018-10-01', '%Y-%m-%d').date()
    new_year = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d').date()
    end_season = datetime.datetime.strptime('2019-05-01', '%Y-%m-%d').date()

    if today > start_season and today < new_year:
        return today.year + 1
    elif today >= new_year and today < end_season:
        return today.year

def team_stat_scraper(team_link, year, conn):
    soup = BeautifulSoup(requests.get(team_link).content, "html.parser")
    header_list = get_headers(soup)
    table_names = get_table_names(header_list.pop(0).split())
    header_list = header_list[0].split()[1:]

    season_totals_df = get_stats(soup)
    slice_list = [[0,1,2,3], [0,4,5], [0,6,7,8], [0,9,10,11], [0,12,13]]
    return gen_tables(season_totals_df, table_names, header_list, slice_list, year)

def get_headers(soup):
    header_list= []
    for i in soup.findAll(True, {'class':['colhead']}):
        header_list.append(" ".join([p.text for p in i if '\xa0' not in p.text]))
    return sorted(set(header_list))

def get_table_names(table_names_raw):
    table_names = []
    for p, i in enumerate(table_names_raw):
        if i == "PCT":
            table_names.pop(table_names.index(table_names_raw[p -1]))
            i = table_names_raw[p -1] + "_" + i
        table_names.append(i)
    table_names.insert(0, 'Team_info')
    return table_names

def get_stats(soup):
    season_totals_df = pd.DataFrame()
    for i in soup.findAll(True, {'class':['oddrow', 'evenrow']}):
        row = ' '.join([p.text for p in i]).split()
        if not row[0][0].isalpha():
            row.pop(0)
        row = check_team(row)
        season_totals_df = pd.concat([season_totals_df, pd.DataFrame(np.array(row)).T])
    return season_totals_df

def check_team(row):
    city_prefixes = ["New", "LA", "San", "Golden", "Oklahoma"]
    if row[0] in city_prefixes:
        row[0] = row[0] + ' ' + row[1]
        row.pop(1)
    return row

def gen_tables(df, table_names, header_list, slice_list, year):
    table_dict = {}
    for slice, name in zip(slice_list, table_names[1:]):
        table = df.iloc[:, slice]
        table.columns=list(itemgetter(*slice)(header_list))
        table.insert(loc=1, column='year', value=year)
        table_dict[name] = table
    return table_dict

def get_teams_id(conn, team_list):
    id_dict = {}
    for team in team_list:
        get_id = 'select team_id from nba_stats.team_info where team like "{}%"'.format(team.replace('LA Lakers', 'Los Angeles'))
        id_dict[team] = sql_execute(conn, get_id)[0]
    return id_dict

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchone()

def insert_into_database(df, table):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))
    df.to_sql(con=engine, name=table, if_exists='replace', index=False)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    year = season_link_scraper(datetime.date.today())
    logging.info('Beginning ESPN team incrementials pipeline {}'.format(str(datetime.datetime.now())))

    team_link = "http://www.espn.com/nba/statistics/team/_/stat/team-comparison-per-game/sort/avgPoints/year/" + str(year) + "/seasontype/2"
    team_id_dict = {}
    for c, (k, v) in enumerate(team_stat_scraper(team_link, year, myConnection).items()):
        if c == 0:
            team_id_dict = get_teams_id(myConnection, v.loc[:, 'TEAM'].tolist())
        v['TEAM'] = v.loc[:, 'TEAM'].apply(lambda x: team_id_dict[x])
        insert_into_database(v, k)

if __name__ == '__main__':
    main()
