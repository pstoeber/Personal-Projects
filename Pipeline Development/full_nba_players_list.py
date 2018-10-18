from bs4 import BeautifulSoup
import requests
import re
import pymysql
import itertools
import datetime


def player_scrap(page_link):

    players = []

    link = requests.get(page_link)
    content = link.content
    soup = BeautifulSoup(content, "html.parser")
    names_raw = soup.find_all('tr')

    for i in names_raw:
        try:
            index_match = re.search(r'\d', i.text).start(0)
            players.append(i.text[:index_match].split())
        except(AttributeError):
            print(i.text)
    return players

def espn_id_scrap():

    link_list = []
    id_link = 'http://www.espn.com/nba/player/_/id/'

    for i in range(0,10):
        temp_link = id_link + str(i+1)
        #print(temp_link)
        link = requests.get(temp_link)
        content = link.content
        soup = BeautifulSoup(content, "html.parser")

        if soup.find('body', {'class':'nba nba-bg {sportId:46}'}) != None:
            break
        #if link_check(soup):
        if soup.find('tr', {'class':'total'}) != None:
            link_list.append('http://www.espn.com/nba/player/stats/_/id/' + str(i+1))
        else:
            print("LINK Failed:" + temp_link)
    return link_list

def link_check(soup_object):

    try:
        check = soup_object.find('li', {'class':'result last'}).text
        print(check)
        if check == 'Currently there are no items for this player.':

            return False
    except:
        return True






## Main ##
link = 'https://www.basketball-reference.com/players/a/'
#link = 'https://stats.nba.com/players/list/?Historic=Y'
#player_scrap(link)
#player_link_finder(name)
link_list = espn_id_scrap()

for i in link_list:
    print(i)
