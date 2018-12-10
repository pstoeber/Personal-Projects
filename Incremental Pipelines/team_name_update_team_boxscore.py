"""
script designed to update the team names in all player stats tablesself.
Allows historical stats to be found and mapped to the teams the players played for
during a specific season.

This script with also update the team_info table by adding the seattle super Sonics and
the new jersey Nets
"""

import pymysql
import logging
import datetime

def create_update_statements(connection):

    team_dict = {"NJ":"Brooklyn Nets",
                 "NJN":"Brooklyn Nets",
                 "MIL":"Milwaukee Bucks",
                 "SA":"San Antonio Spurs",
                 "SAS":"San Antonio Spurs",
                 "GS":"Golden State Warriors",
                 "GSW":"Golden State Warriors",
                 "UTAH":"Utah Jazz",
                 "UTA":"Utah Jazz",
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
                 "NYK":"New York Knicks",
                 "LAC":"LA Clippers",
                 "MEM":"Memphis Grizzlies",
                 "SAC":"Sacramento Kings",
                 "TOR":"Toronto Raptors",
                 "ORL":"Orlando Magic",
                 "CHI":"Chicago Bulls",
                 "MIN":"Minnesota Timberwolves",
                 "WSH":"Washington Wizards",
                 "WAS":"Washington Wizards",
                 "OKC":"Oklahoma City Thunder",
                 "PHI":"Philadelphia 76ers",
                 "NO":"New Orleans Pelicans",
                 "NOP":"New Orleans Pelicans",
                 "NOH":"New Orleans Pelicans",
                 "NOK":"New Orleans Pelicans",
                 "LAL":"Los Angeles Lakers",
                 "IND":"Indiana Pacers",
                 "CHA":"Charlotte Hornets",
                 "CHH":"Charlotte Hornets",
                 "DET":"Detroit Pistons",
                 "SEA":"Seattle Super Sonics",
                 "VAN":"Vancouver Grizzlies",
                 "Seattle Super Sonics":"Oklahoma City Thunder"}
    #print(len(team_dict))
    tables_list = ["advanced_team_boxscore_stats",
                   "figure4_team_boxscore_stats",
                   "team_scoring_boxscore_stats",
                   "traditional_team_boxscore_stats",
                   "team_misc_boxscore_stats",
                   "player_usage_stats",
                   "player_misc_stats",
                   "player_scoring_stats",
                   "player_info"]

    for city in team_dict:
        for table in tables_list:
            if table != 'player_info':
                update_statement = "update " + table + " set team = \"" + team_dict[city] + "\" where team = \"" + city + "\""
                sql_execute(connection, update_statement)
                update_home_statement = "update " + table + " set home_team = \"" + team_dict[city] + "\" where home_team = \"" + city + "\""
                sql_execute(connection, update_home_statement)
                update_away_statement = "update " + table + " set away_team = \"" + team_dict[city] + "\" where away_team = \"" + city + "\""
                sql_execute(connection, update_away_statement)
            elif table == 'player_info':
                player_update_statement = "update " + table + " set current_team = \"" + team_dict[city] + "\" where current_team = \"" + city + "\""
                sql_execute(connection, player_update_statement)

    update_box_map_away = 'update box_score_map set away_team = "{}" where away_team = "{}"'.format('LA Clippers', 'Los Angeles Clippers')
    sql_execute(connection, update_box_map_away)
    update_box_map_home = 'update box_score_map set home_team = "{}" where home_team = "{}"'.format('LA Clippers', 'Los Angeles Clippers')
    sql_execute(connection, update_box_map_home)

    updated_pred_boston = 'update nba_stats.total_points_predictions set team = "{}" where team = "{}"'.format('Boston Celtics', 'Boston')
    sql_execute(connection, updated_pred_boston)
    updated_pred_clip = 'update nba_stats.total_points_predictions set team = "{}" where team = "{}"'.format('LA Clippers', 'LA')
    sql_execute(connection, updated_pred_clip)
    updated_pred_boston = 'update nba_stats.total_points_predictions set team = "{}" where team = "{}"'.format('Portland Trail Blazers', 'Portland')
    sql_execute(connection, updated_pred_boston)

def update_team_info(connection):

    nets_insert = "insert into team_info values (33, \"Vancouver Grizzlies\")"
    #sql_execute(connection, nets_insert)

    #sonics_insert = "insert into team_info values (32, \"Seattle Super Sonics\")"
    #sql_execute(connection, sonics_insert)

def sql_execute(connection, update_statement):

    exe = connection.cursor()
    exe.execute(update_statement)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning NBA Stats team name {}'.format(str(datetime.datetime.now())))
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    create_update_statements(myConnection)
    #update_team_info(myConnection)
    logging.info('NBA Stats team name update completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
