"""
pipeline designed to create/update active roster table within the NBA states database
"""

import pymysql
import re

def drop_table(connection):
    drop_table_statement = 'drop table active_rosters'
    sql_execute(drop_table_statement, connection)

def get_cities(connection):  #function to extract all cities from team_info entity

    city_extract_query = "select team from team_info"
    cities_raw_extract = sql_execute(city_extract_query, connection)
    cities = data_extract_from_raw_data(cities_raw_extract)
    return cities

def get_team_names(connection):

    team_names_extract_query = "select distinct current_team from player_info"
    team_names_extract = sql_execute(team_names_extract_query, connection)
    teams = data_extract_from_raw_data(team_names_extract)
    return teams

def data_extract_from_raw_data(data):

    regex_catch = re.compile('[A-Za-z].*\w')
    cleaned_data_list = []

    for raw_line in data:
        try:
            data_string = re.search(regex_catch, str(raw_line))
            cleaned_data_list.append(data_string.group(0))
        except AttributeError:
            print("Data not captured in regex expression", raw_line)
    return cleaned_data_list

def create_city_team_dict(teams, cities):

    city_team_dict = {}
    for city in cities:
        for team in teams:
            if city in team:
                city_team_dict[city] = team
    #hard coding in LA Lakers
    city_team_dict["LA Lakers"] = "Los Angeles Lakers"
    return city_team_dict

def create_update_statements(city_team_dict, connection):

    for city_team in city_team_dict:
        update_statement = "update team_info set team = '" + str(city_team_dict[city_team]) + "' where team = '" + str(city_team) + "'"
        sql_execute(update_statement, connection)

def create_active_roster_table(connection):

    create_table_statement = 'create table nba_stats.active_rosters( \
                                select distinct reg.player_id, play.name, team.team_id, reg.team, stand.conference \
                                from RegularSeasonAverages as reg \
                                inner join player_info as play on play.player_id = reg.player_id \
                                inner join team_info as team on (team.team = reg.team) \
                                inner join team_standings as stand on ((team.team = stand.team) and (stand.season = reg.season)) \
                                where stand.season = (select max(season) from team_standings))'
    sql_execute(create_table_statement, connection)

    add_primary_key = 'alter table nba_stats.active_rosters\
                       add constraint active_roster_pk primary key(player_id, name, team_id, team)'
    sql_execute(add_primary_key, connection)

def sql_execute(query, connection):

    exe = connection.cursor()
    exe.execute(query)
    return exe.fetchall()

## main function
myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit="true")

#drop_table(myConnection)
cities = get_cities(myConnection)
teams = get_team_names(myConnection)
city_team_dict = create_city_team_dict(teams, cities)
create_update_statements(city_team_dict, myConnection)
create_active_roster_table(myConnection)
