import requests
from bs4 import BeautifulSoup
import re
import pymysql
import itertools
import datetime
import logging

def drop_tables(connection, table_names):

    drop_statements = []
    for table in table_names:
        drop_statement = "drop table " + table
        drop_statements.append(drop_statement)
    return drop_statements

def season_link_scraper():

    date_time = str(datetime.date.today())
    current_year = int(date_time[:date_time.index("-")])
    if current_year == 2018:
        return [current_year + 1]
    elif current_year == 2019:
        return [current_year]

def team_stat_scraper(team_link, year):
    season_totals_list, header_list, table_names = [], [], []
    table_names.append("Team_info")

    link = requests.get(team_link)
    content = link.content
    soup = BeautifulSoup(content, "html.parser")

    headers = soup.findAll(True, {'class':['colhead']})
    for i in headers:
        header_list.append(" ".join([p.get_text() for p in i]))

    header_list = sorted(set(header_list))
    table_names_raw = header_list.pop(1).split()

    for p, i in enumerate(table_names_raw):
        if i == "PCT":
            table_names.pop(table_names.index(table_names_raw[p -1]))
            i = table_names_raw[p -1] + "_" + i
        table_names.append(i)

    team_stats_raw = soup.findAll(True, {'class':['oddrow', 'evenrow']})
    for i in team_stats_raw:
        season_totals_list.append(" ".join([p.get_text() for p in i]))

    header_list = header_list[0].split()
    iterated_header_list = [[header_list[1]], header_list[2:5], header_list[5:7], header_list[7:10], header_list[10:13], header_list[13:]]
    return table_names, iterated_header_list, season_totals_list

def create_table_statements(table_names, iterated_header_list, create_table_list):

    special_char = re.compile(r"\W")
    create_statement = "create table " + table_names[0] + " (team_id int,\nyear varchar(50),\n"

    for field in iterated_header_list[0]:
        if special_char.findall(field):
            field = '`' + field + '`'

        if field == "TEAM" or field == "DIFF":
            create_statement += field + " varchar(50),\n"
        else:
            create_statement += field + " float(10),\n"

    create_statement = create_statement[:-2] + ")"
    create_table_list.append(create_statement)

    if len(table_names) > 1:
        return create_table_statements(table_names[1:], iterated_header_list[1:], create_table_list)
    else:
        return create_table_list

def create_team_info_insert_statements(connection, season_total_list, table_names, iterated_header_list, year):

    city_prefixes = ["New", "LA", "San", "Golden", "Oklahoma"]
    cities = {}

    for p, i in enumerate(season_total_list):
        row = i.split()
        if re.match('[0-9]', row[0]):
            row.pop(0)

        if row[0] in city_prefixes:
            row[0] = row[0] + " " + row[1]
            row.pop(1)

        sql_name = row[0]
        if 'Lakers' in row[0]:
            sql_name = 'Los Angeles Lakers'

        find_team_id = 'select team_id from nba_stats.team_info where team like \'{}%\''.format(sql_name)
        team_id = sql_execute(connection, [find_team_id])[0][0]
        cities[row[0]] = int(team_id)
    return cities

def create_insert_statements(connection, season_total_list, table_names, iterated_header_list, year, cities):

    city_prefixes = ["New", "LA", "San", "Golden", "Oklahoma"]
    for i in season_total_list:
        row = i.split()
        if re.match('[0-9]', row[0]):
            row.pop(0)

        if row[0] in city_prefixes:
            row[0] = row[0] + " " + row[1]
            row.pop(1)

        team_index = cities[row[0]]

        points_state = 'insert into ' + table_names[1] + ' values (' + str(team_index) + ', ' + str(year) + ', ' + row[1] + ', ' + row[2] + ', ' + row[3] + ')'
        sql_execute(connection, [points_state])

        fg_state = 'insert into ' + table_names[2] + ' values (' + str(team_index) + ', ' + str(year) + ', ' + row[4] + ', ' + row[5] + ')'
        sql_execute(connection, [fg_state])

        fg3_state = 'insert into ' + table_names[3] + ' values (' + str(team_index) + ', ' + str(year) + ', ' + row[6] + ', ' + row[7] + ', ' + row[8] + ')'
        sql_execute(connection, [fg3_state])

        rb_state = 'insert into ' + table_names[4] + ' values (' + str(team_index) + ', ' + str(year) + ', ' + row[9] + ', ' + row[10] + ', ' + row[11] + ')'
        sql_execute(connection, [rb_state])

        to_state = 'insert into ' + table_names[5] + ' values (' + str(team_index) + ', ' + str(year) + ', ' + row[12] + ', ' + row[13] + ')'
        sql_execute(connection, [to_state])

def alter_team_info_table(connection):

    alter_statement = 'alter table team_info drop column year'
    sql_execute(connection, [alter_statement])

def sql_execute(connection, input_list):
    exe = connection.cursor()
    for i in input_list:
        exe.execute(i)
    return exe.fetchall()

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    # team_link = "http://www.espn.com/nba/statistics/team/_/stat/team-comparison-per-game/sort/avgPoints/year/2018/seasontype/2"
    years_list = season_link_scraper()
    ##UNCOMMENT AFTER TESTING##
    #years_list = [2018]
    logging.info('Beginning ESPN team incrementials pipeline {}'.format(str(datetime.datetime.now())))
    for c, year in enumerate(years_list):
        team_link = "http://www.espn.com/nba/statistics/team/_/stat/team-comparison-per-game/sort/avgPoints/year/" + str(year) + "/seasontype/2"
        table_names, iterated_header_list, season_totals_list = team_stat_scraper(team_link, year)

        if c < 1:
            # print("TRUE")

            drop_table_statements = drop_tables(myConnection, table_names)
            sql_execute(myConnection, drop_table_statements)
            create_table_list = create_table_statements(table_names, iterated_header_list, [])
            sql_execute(myConnection, create_table_list)

            cities = create_team_info_insert_statements(myConnection, season_totals_list, table_names, iterated_header_list, year)

        create_insert_statements(myConnection, season_totals_list, table_names, iterated_header_list, year, cities)

    alter_team_info_table(myConnection)
    logging.info('ESPN team incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
