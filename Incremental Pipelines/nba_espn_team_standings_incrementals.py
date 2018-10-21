"""
Pipeline usewd to scrap team standings for the last 2 decade of NBA teamsself.
Data will be landed into table TEAM_STANDINGS within the nba_stats databaseself.
"""

import re
import requests
from bs4 import BeautifulSoup
import pymysql
import itertools
import datetime
import logging

def season_scraper():
    date_time = str(datetime.date.today())
    current_year = int(date_time[:date_time.index("-")])
    if current_year == 2018:
        return [current_year + 1]
    elif current_year == 2019:
        return [current_year]

def drop_table(conn):
    drop_statment = 'drop table nba_stats_staging.team_standings'
    sql_execute(drop_statment, conn)

def team_standing_scrap(standing_stats_link, year):

    teams, header_list, conference_list, team_standing_stats = {}, [], [], []

    link = requests.get(standing_stats_link)
    content = link.content
    soup = BeautifulSoup(content, "html.parser")

    conference = soup.findAll(True, {'class':['Table2__Title']})
    header = soup.findAll(True, {'class':['tar subHeader__item--content Table2__th']})
    standing_stats = soup.findAll(True, {'class':['Table2__td']})

    for conf in conference:
        conference_list.append(conf.get_text())
    print(conference_list)

    for line in header:
        if line.get_text() != '':
            header_list.append(line.get_text())

    header_list = sorted(set(header_list), key = header_list.index)

    index_find = re.compile('[A-Z]{3,4}')
    conference_index = 0

    for line in standing_stats:
        try:
            if len(teams) == 15:
                conference_index = 1
            if '--' in line.get_text() or line.get_text()[2].isalpha() or '/' in line.get_text():
                for match in re.finditer(index_find, line.get_text()):
                    teams[line.get_text()[match.end() -1:]] = conference_list[conference_index]
            else:
                team_standing_stats.append(line.get_text())
        except IndexError:
            team_standing_stats.append(line.text)

    counter = 0
    row = ''
    team_standing = []

    for stat in team_standing_stats:
        row += str(stat) + ' '
        counter += 1
        if counter == len(header_list):
            team_standing.append(row.split())
            counter = 0
            row = ''

    standing_dict = {}
    for team, standing_stats in zip(teams, team_standing):
        standing_dict[team, teams[team], year] = standing_stats
    return standing_dict, header_list

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
        #    pass
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
    years_list = season_scraper()
    logging.info('Beginning ESPN team standings pipeline {}'.format(str(datetime.datetime.now())))
    ##UNCOMMENT AFTER TESTING##
    #years_list = [2018]
    for c, year in enumerate(years_list):
        standing_stats_link = 'http://www.espn.com/nba/standings/_/season/' + str(year)
        #print(standing_stats_link)
        standing_dict, header_list = team_standing_scrap(standing_stats_link, str(year))
        if c < 1:
            drop_table(myConnection)
            create_standing_table(myConnection, header_list)
        create_insert_statements(standing_dict, myConnection)

    update_statements(myConnection)
    logging.info('ESPN team standings pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
