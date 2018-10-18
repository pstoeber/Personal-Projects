"""
script designed to update the team names in all player stats tablesself.
Allows historical stats to be found and mapped to the teams the players played for
during a specific season.

This script with also update the team_info table by adding the seattle super Sonics and
the new jersey Nets
"""

import pymysql

def create_update_statements(connection):

    team_dict = {"NJ":"New Jersey Nets",
                 "MIL":"Milwaukee Bucks",
                 "SA":"San Antonio Spurs",
                 "GS":"Golden State Warriors",
                 "UTAH":"Utah Jazz",
                 "DAL":"Dallas Mavericks",
                 "CLE":"Cleveland Cavaliers",
                 "DEN":"Denver Nuggets",
                 "BOS":"Boston Celtics",
                 "PHX":"Phoenix Suns",
                 "ATL":"Atlanta Hawks",
                 "BKN":"Brooklyn Nets",
                 "MIA":"Miami Heat",
                 "HOU":"Houston Rockets",
                 "POR":"Portland Trail Blazers",
                 "NY":"New York Knicks",
                 "LAC":"LA Clippers",
                 "MEM":"Memphis Grizzlies",
                 "SAC":"Sacramento Kings",
                 "TOR":"Toronto Raptors",
                 "ORL":"Orlando Magic",
                 "CHI":"Chicago Bulls",
                 "MIN":"Minnesota Timberwolves",
                 "WSH":"Washington Wizards",
                 "OKC":"Oklahoma City Thunder",
                 "PHI":"Philadelphia 76ers",
                 "NO":"New Orleans Pelicans",
                 "LAL":"Los Angeles Lakers",
                 "IND":"Indiana Pacers",
                 "CHA":"Charlotte Hornets",
                 "DET":"Detroit Pistons",
                 "SEA":"Seattle Super Sonics",
                 "VAN":"Vancouver Grizzlies"}
    #print(len(team_dict))
    tables_list = ["RegularSeasonAverages",
                    "RegularSeasonMiscTotals",
                    "RegularSeasonTotals"]

    for city in team_dict:
        for table in tables_list:
            update_statement = "update " + table + " set team = \"" + team_dict[city] + "\" where team = \"" + city + "\""
            sql_execute(connection, update_statement)

def update_team_info(connection):

    nets_insert = "insert into team_info values (33, \"Vancouver Grizzlies\")"
    sql_execute(connection, nets_insert)

    #sonics_insert = "insert into team_info values (32, \"Seattle Super Sonics\")"
    #sql_execute(connection, sonics_insert)

def sql_execute(connection, update_statement):

    exe = connection.cursor()
    exe.execute(update_statement)

## Main function

myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit=True)
create_update_statements(myConnection)
#update_team_info(myConnection)
