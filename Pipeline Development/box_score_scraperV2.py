"""
script to scrape the content of box scores
"""

import requests
from bs4 import BeautifulSoup
import re
import pymysql
import itertools
import datetime
import hashlib
import codecs
from collections import defaultdict

def gen_dates():

    links_list = []
    start_date = '1998-9-30' #2014-8-30
    end_date = '2000-5-30' #2018-6-30
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    step = datetime.timedelta(days=1)

    while start <= end:
        date = str(start.date()).split('-')
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
                        print(link)
                        box_score_list.append(link)
        else:
            print('FAILED: ' + link)
            pass
    return sorted(set(box_score_list))

def box_scrape(page_link):

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
            if name.text == 'PTS' or name.text == 'DRtg':  ### swap for pts +/-
                switch = 1
            if (c < 5 or switch == 1) and name.text != 'PTS' and name.text != 'DRtg': ###swap for +/-
                if name.text in count_dict:
                    count_dict[name.text] += 1
                else:
                    count_dict[name.text] = 1
                if count_dict[name.text] == 1:
                    stat_dict[name.text] = [[game_hash] + [stat.text for stat in stats[start_index:start_index + 19]]] ##change index back to 20
                    start_index += 19 #### change back to 20
                elif count_dict[name.text] == 2:
                    stat_dict[name.text].append([game_hash] + [stat.text for stat in stats[start_index:start_index + 15]])
                    start_index += 15
    game_dict[tuple(game_tag + score)] = stat_dict
    return game_dict

def hash_gen(game_info_string):

    return hashlib.md5(game_info_string.encode('utf-8')).hexdigest()

def create_insert_statements(conn, game_dict):

    for game in game_dict:
        box_score_map = 'insert into nba_stats.box_score_map values ("' + '", "'.join([i for i in game[0:3]]) + '"' + ', str_to_date(\'' + game[3] + '\', \'%M %D %Y\'))'
        game_results = 'insert into nba_stats.game_results values ("' + '", "'.join([str(i) for i in game[4:]]) + '")'

        sql_execute(conn, box_score_map)
        sql_execute(conn, game_results)
        for k, v in game_dict[game].items():
            for c, row in enumerate(v):
                if c == 0:
                    basic_insert = 'insert into nba_stats.basic_box_stats (name, game_hash, MP, FG, fga, FG_PCT, 3p, 3pa, 3p_pct, ft, fta, FT_PCT, orb, drb, trb, ast, stl, blk, tov, pf, pts) values ("' + k + '", "' + '", "'.join([i for i in row]) + '")' ### use for seasons older than 2001 (name, game_hash, MP, FG, fga, FG_PCT, 3p, 3pa, 3p_pct, ft, fta, FT_PCT, orb, drb, trb, ast, stl, blk, tov, pf, pts)
                    sql_execute(conn, basic_insert)
                elif c == 1:
                    advanced_insert = 'insert into nba_stats.advanced_box_stats values ("' + k + '", "' + '", "'.join([i for i in row]) + '")'
                    sql_execute(conn, advanced_insert)

def sql_execute(conn, insert_statment):

    exe = conn.cursor()
    try:
        exe.execute(insert_statment)
    except:
        print('failed insert: ', insert_statment)

### Main ###

myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit=True)

links_list = gen_dates()
box_score_links = get_links(links_list)
#box_score_links = ['https://www.basketball-reference.com/boxscores/200001310MIA.html']
#box_score_links = ['https://www.basketball-reference.com/boxscores/200901310MEM.html']

for box_link in box_score_links:
    print(box_link)
    game_dict = box_scrape(box_link)
    create_insert_statements(myConnection, game_dict)
