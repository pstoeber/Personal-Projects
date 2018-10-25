"""
script to scrape the content of box scores

command line arguement order/requirements
argv[1] = basic_box_score_stats.sql
argv[2] = advanced_box_score_stats.sql

example command call:
python3 box_score_nba_ref_incrementals.py sql\ ddl/basic_box_score_stats.sql sql\ ddl/advanced_box_score_stats.sql sql\ ddl/box_score_map_ddl.sql sql\ ddl/game_results_ddl.sql

"""

import requests
from bs4 import BeautifulSoup
import re
import pymysql
import itertools
import datetime
import hashlib
import codecs
import sys
import logging
from collections import defaultdict

def drop_tables(conn):
    table_list = ['nba_stats_staging.basic_box_stats',
                  'nba_stats_staging.advanced_box_stats',
                  'nba_stats_staging.box_score_map',
                  'nba_stats_staging.game_results']
    for table in table_list:
        drop = 'drop table {}'.format(table)
        sql_execute(conn, drop)

def extract_ddl(file):
    with open(file, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_dates(conn):

    links_list = []

    find_max_date = 'select max(game_date) from nba_stats.box_score_map'
    max_system_date = sql_execute(conn, find_max_date)[0][0]

    step = datetime.timedelta(days=1)
    start = max_system_date + step
    end = datetime.datetime.today().date()

    while start <= end:
        date = str(start).split('-')
        links_list.append('https://www.basketball-reference.com/boxscores/?month=' + date[1] + '&day=' + date[2] + '&year=' + date[0])
        start += step
    return links_list

def get_links(links_list):

    box_score_list = []
    for link in links_list:
        soup = BeautifulSoup(requests.get(link).content, "html.parser")
        game_check = soup.find('h2')

        if game_check != None:
            if re.search(r'\d*\WNBA Games', game_check.text):
                for a in soup.find_all('a', href=True):
                    if re.search(r'/boxscores/\d{8}', a['href']):
                        link = 'https://www.basketball-reference.com' + a['href']
                        box_score_list.append(link)
        else:
            logging.info('[FAILED TO CONNECT]: {}'.format(link))
            pass
    return sorted(set(box_score_list))

def box_scrape(page_link):
    print(page_link)

    count_dict, game_dict = {}, {}
    stat_dict = defaultdict(list)
    soup = BeautifulSoup(requests.get(page_link).content, "html.parser")

    game_tag = soup.find('h1')
    game_hash = hash_gen(game_tag)

    game_tag = game_tag.text.split(' at ')
    temp_tag = game_tag[1].split(' Box Score, ')
    game_tag = [game_hash] + [game_tag[0]] + temp_tag

    score = [i.get_text() for i in soup.findAll(True, {'class':['score']})]
    score = [game_hash] + score

    for row in soup.find_all('tbody'):
        start_index = 0
        switch = 0
        for c, name in enumerate(row.find_all('th')):
            stats = row.find_all('td')
            if name.text == '+/-' or name.text == 'DRtg':  ### swap for pts +/-
                switch = 1
            if (c < 5 or switch == 1) and name.text != '+/-' and name.text != 'DRtg': ###swap for PTS
                if name.text in count_dict:
                    count_dict[name.text] += 1
                else:
                    count_dict[name.text] = 1
                if count_dict[name.text] == 1:
                    stat_dict[name.text] = [[game_hash] + [stat.text for stat in stats[start_index:start_index + 20]]] ##change index back to 20
                    start_index += 20 #### change back to 20
                elif count_dict[name.text] == 2:
                    stat_dict[name.text].append([game_hash] + [stat.text for stat in stats[start_index:start_index + 15]])
                    start_index += 15
    game_dict[tuple(game_tag + score)] = stat_dict
    return game_dict

def hash_gen(game_info_string):
    return hashlib.md5(game_info_string.encode('utf-8')).hexdigest()

def create_insert_statements(conn, game_dict):

    for game in game_dict:
        box_score_map = 'insert into nba_stats_staging.box_score_map values ("' + '", "'.join([i for i in game[0:3]]) + '"' + ', str_to_date(\'' + game[3] + '\', \'%M %D %Y\'))'
        game_results = 'insert into nba_stats_staging.game_results values ("' + '", "'.join([str(i) for i in game[4:]]) + '")'

        sql_execute(conn, box_score_map)
        sql_execute(conn, game_results)
        for k, v in game_dict[game].items():
            for c, row in enumerate(v):
                if c == 0:
                    basic_insert = 'insert into nba_stats_staging.basic_box_stats values ("' + k + '", "' + '", "'.join([i for i in row]) + '")' ### use for seasons older than 2001 (name, game_hash, MP, FG, fga, FG_PCT, 3p, 3pa, 3p_pct, ft, fta, FT_PCT, orb, drb, trb, ast, stl, blk, tov, pf, pts)
                    sql_execute(conn, basic_insert)
                elif c == 1:
                    advanced_insert = 'insert into nba_stats_staging.advanced_box_stats values ("' + k + '", "' + '", "'.join([i for i in row]) + '")'
                    sql_execute(conn, advanced_insert)

def sql_execute(conn, insert_statement):

    exe = conn.cursor()
    try:
        exe.execute(insert_statement)
        return exe.fetchall()
    except:
        logging.info('[FAILED INSERT]: {}'.format(insert_statement))

def main(arg1, arg2, arg3, arg4):
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    logging.info('Beginning NBA Reference players incrementals pipeline {}'.format(str(datetime.datetime.now())))
    links_list = gen_dates(myConnection)
    box_score_links = get_links(links_list)

    drop_tables(myConnection)
    #creating tables table in staging schema
    for ddl in [arg1, arg2, arg3, arg4]:
        sql_execute(myConnection, ' '.join([i for i in extract_ddl(ddl)]))

    for box_link in box_score_links:
        game_dict = box_scrape(box_link)
        create_insert_statements(myConnection, game_dict)
    logging.info('NBA Reference players incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
