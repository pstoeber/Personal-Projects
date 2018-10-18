from bs4 import BeautifulSoup
import requests
import re
import pymysql
import itertools
import datetime


def header_scrap(box_link):

    header_list = []

    link = requests.get(box_link)
    content = link.content
    soup = BeautifulSoup(content, "html.parser")
    header = soup.findAll(True, {'class':['thead']})

    for i in header:
        header_list.append(i.get_text().strip('\n'))

    header_list = sorted(set(header_list))
    header_list = [i for i in header_list[0].split('\n') if i != '\xa0']

    return header_list

def box_score_stats_scrap(box_link):

    stats_list = []

    link = requests.get(box_link)
    content = link.content
    soup = BeautifulSoup(content, "html.parser")

    table = soup.findAll('table', {'class': 'sortable stats_table now_sortable'})
    #th = table.find_all('thead')
    print(table)


#    stats = soup.findAll(True, {'class':['right']})
#    for i in stats:

#        print(i.get_text())



    #stats = soup.find_all('tr')
    #for i in stats:
    #    print(i.text)


### Main ###

#link = 'https://www.basketball-reference.com/friv/dailyleaders.cgi?lid=header_dateoutput&month=01&day=02&year=2016'

link = 'https://www.basketball-reference.com/boxscores/201801110LAL.html'

#header_list = header_scrap(link)
box_score_stats_scrap(link)
