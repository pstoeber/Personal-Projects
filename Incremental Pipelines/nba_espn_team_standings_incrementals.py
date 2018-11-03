"""
Pipeline usewd to scrap team standings for the last 2 decade of NBA teamsself.
Data will be landed into table TEAM_STANDINGS within the nba_stats databaseself.
"""

import re
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import pymysql
import itertools
import datetime
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine

def season_scraper(today):
    start_season = datetime.datetime.strptime('2018-10-01', '%Y-%m-%d').date()
    new_year = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d').date()
    end_season = datetime.datetime.strptime('2019-05-01', '%Y-%m-%d').date()

    if today > start_season and today < new_year:
        return today.year + 1
    elif today >= new_year and today < end_season:
        return today.year




def drop_table(conn):
    drop_statment = 'drop table nba_stats_staging.team_standings'
    sql_execute(drop_statment, conn)




def team_standing_scrap(standing_stats_link, year, chromeDriver):

    teams, team_standing_stats = {}, []
    options = Options()
    options.headless = True
    options.add_extensions = '/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0'
    browser = webdriver.Chrome(executable_path=chromeDriver, chrome_options=options)

    soup = BeautifulSoup(requests.get(standing_stats_link).content, "html.parser")

    standing_stats = soup.findAll(True, {'class':['Table2__td']})

    conference_list = get_conference(soup)
    print(conference_list)

    header_list = sorted(set(get_header(soup)))
    print(header_list)


    browser.get(standing_stats_link)
    for i in range(len(browser.find_elements_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[2]/div/section'))):
        #for p in i.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[2]/div/section/div[1]/section/table/tbody/tr'):
        print(browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[{}]/div/section'.format(str(i+1))).text)
        print('\n\n\n')
                                        #//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[2]/div/section/div[1]/section/table/tbody
        print(i.text)

    #tables = pd.read_html(standing_stats_link)
    #print(tables)

    #for table in tables:
    #    print(table)



    #for i in soup.findAll('table'):
    #    print(i.text)
    #    for p in i.findAll('tr'):
    #        print(p.text)



    #get_stats(standing_stats_link, conference_list, chromeDriver, len(header_list))




    #index_find = re.compile('[A-Z]{3,4}')
    #conference_index = 0

    #for line in standing_stats:
    #    try:
    #        if len(teams) == 15:
    #            conference_index = 1
    #        if '--' in line.get_text() or line.get_text()[2].isalpha() or '/' in line.get_text():
    #            for match in re.finditer(index_find, line.get_text()):
    #                teams[line.get_text()[match.end() -1:]] = conference_list[conference_index]
    #        else:
    #            team_standing_stats.append(line.get_text())
    #    except IndexError:
    #        print(line.text)
    #        team_standing_stats.append(line.text)

    #counter = 0
    #row = ''
    #team_standing = []

    #for stat in team_standing_stats:
    #    row += str(stat) + ' '
    #    counter += 1
    #    if counter == len(header_list):
    #        team_standing.append(row.split())
    #        counter = 0
    #        row = ''

    #standing_dict = {}
    #for team, standing_stats in zip(teams, team_standing):
    #    standing_dict[team, teams[team], year] = standing_stats


    #return standing_dict, header_list

def get_conference(soup):
    return [i.text for i in soup.findAll(True, {'class':['Table2__Title']})]

def get_header(soup):
    return [i.text for i in soup.findAll(True, {'class':['tar subHeader__item--content Table2__th']}) if i.text != '']

def get_stats(link, conference_list, chromeDriver, head_len):
    rows = []
    options = Options()
    options.headless = True
    browser = webdriver.Chrome(executable_path=chromeDriver, chrome_options=options)
    browser.get(link)

    for c, conf in enumerate(conference_list):
        stats = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[2]/div/section/div/div[{}]/section/table/tbody'.format(c+1))
        #print(stats.text)
        row = ''
        for p, data in enumerate(stats.text.split()):
            #row += str(data)
            print(data)
            #if (p+1) % head_len == 0:
            #    rows.append(row)
            #    print(row)
            #    row = ''

        #for i in stats:
        #    print(i.text)

#//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[2]/div/section/div/div[1]/section/table/tbody
#//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[2]/div/section/div/div[2]/section/table/tbody
#//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[2]/div/section/div/div[{}]/section




def int_check(input):
    try:
        int(input)
        return True
    except:
        return False

def create_standing_table(connection, header_list):

    create_statement = 'create table team_standings (team varchar(30), conference varchar(20), season int,'
    for p, field in enumerate(header_list):

        if field == 'W' or field == 'L':
            create_statement += field + ' int(10),\n'
        elif field == 'PCT' or field == 'PPG':
            create_statement += field + ' float(10),\n'
        elif field == 'OPP PPG' or field == 'GB':
            create_statement += field.replace(' ', '_') + ' float(10),\n'
        elif field == 'HOME' or field == 'AWAY' or field == 'CONF' or field == 'DIFF' or field == 'STRK' or field == 'L10':
            create_statement += field + ' varchar(10),\n'
        elif field == 'DIV':
            create_statement += '`' + field + '` varchar(10),'

    create_statement = create_statement[:-2] + ')'
    sql_execute(create_statement, connection)

def create_insert_statements(standing_dict, connection):

    for team in standing_dict:
        insert_statement = 'insert into team_standings values("' + team[0] + '", "' + team[1] + '", ' + str(team[2]) + ', '
        for c, value in enumerate(standing_dict[team]):
            if value == '-':
                value = 0
            if '-' in str(value) or 'W' in str(value) or 'L' in str(value):
                insert_statement += '"' + str(value) + '", '
            else:
                insert_statement += str(value) + ', '
        insert_statement = insert_statement[:-2] + ')'
        try:
            sql_execute(insert_statement, connection)
        except:
            logging.info('[FAILED INSERT]:' + insert_statement)
        insert_statement = ''

def update_statements(connection):
    update_utah = 'update team_standings set team = "Utah Jazz" where team = "HUtah Jazz"'
    sql_execute(update_utah, connection)

    #update_okc = 'update team_standings set team = "Utah Jazz" where team like "O/Oklahoma City%"'
    #sql_execute(update_okc, connection)

def sql_execute(query, connection):
        exe = connection.cursor()
        exe.execute(query)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    year = season_scraper(datetime.date.today())
    logging.info('Beginning ESPN team standings pipeline {}'.format(str(datetime.datetime.now())))
    chromeDriver = '/Users/Philip/Downloads/chromedriver'
    standing_stats_link = 'http://www.espn.com/nba/standings/_/season/' + str(year)
    team_standing_scrap(standing_stats_link, str(year), chromeDriver)

    #for c, year in enumerate(years_list):
    #    standing_stats_link = 'http://www.espn.com/nba/standings/_/season/' + str(year)
    #    standing_dict, header_list = team_standing_scrap(standing_stats_link, str(year))
    #    if c < 1:
    #        drop_table(myConnection)
    #        create_standing_table(myConnection, header_list)
    #    create_insert_statements(standing_dict, myConnection)

    #update_statements(myConnection)
    logging.info('ESPN team standings pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
