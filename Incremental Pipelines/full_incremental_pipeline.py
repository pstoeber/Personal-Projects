"""
Script to wrap all incremental pipelines together

command line call:

python3 full_incremental_pipeline.py sql\ ddl/basic_box_score_stats.sql sql\ ddl/advanced_box_score_stats.sql sql\ ddl/box_score_map_ddl.sql sql\ ddl/game_results_ddl.sql sql\ ddl/active_rosters_player_id.sql sql\ ddl/active_rosters_team_info.sql
"""
import subprocess
import os
import sys
import pymysql
import shutil
import datetime
import logging
import nba_stats_adv_team_box_score
import nba_stats_fig4_team_box_score
import nba_stats_misc_team_box_score
import nba_stats_scoring_team_box_score
import nba_stats_traditional_team_box_score
import box_score_nba_ref_incrementals
import nba_espn_incrementals
import nba_espn_team_incrementals
import nba_espn_team_standings_incrementals
import espn_update_season_date
import espn_team_name_update
import team_name_update_team_boxscore
import player_name_nba_ref_boxscore
import date_lookup_table
import active_roster
import injured_players

def back_up_db(out_file):
    logging.info('Backing up nba_stats_backup database {}'.format(str(datetime.datetime.now())))
    os.system('mysqldump -u root -p nba_stats > {}'.format(out_file))
    logging.info('Backing up complete {}'.format(str(datetime.datetime.now())))

def compress_backup(out_file):
    logging.info('Compressing backup of nba_stats_backup database {}'.format(str(datetime.datetime.now())))
    shutil.make_archive('nba_stats_backup', 'zip', "/Users/Philip/Documents/NBA Database Backups", 'nba_stats_{}.sql'.format(str(datetime.date.today())))
    os.system('mv "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/nba_stats_backup.zip" "/Users/Philip/Documents/NBA Database Backups"')
    logging.info('Compression complete {}'.format(str(datetime.datetime.now())))

def espn_delete_max_season(conn):
    logging.info('Deleting Max season from ESPN tables {}'.format(str(datetime.datetime.now())))
    table_list = ['RegularSeasonAverages',
                  'RegularSeasonMiscTotals',
                  'RegularSeasonTotals',
                  'team_standings',
                  '3PT_PCT',
                  'FG_PCT',
                  'POINTS',
                  'REBOUND_PCT',
                  'TURNOVERS']
    for c, table in enumerate(table_list):
        field = 'season'
        if c > 3:
            field = 'year'
        delete = 'delete from nba_stats_backup.{} where {} = 2019'.format(table, field)
        sql_execute(conn, delete)
    logging.info('Deletion from ESPN tables complete {}'.format(str(datetime.datetime.now())))

def insert_into_nba_stats(conn):
    logging.info('Beginning insert into nba_stats_backup from nba_stats_staging {}'.format(str(datetime.datetime.now())))
    get_tables = 'show tables'
    tables = sql_execute(conn, get_tables)

    for table in tables:
        insert = 'insert into nba_stats_backup.{} (select * from nba_stats_staging.{})'.format(table[0], table[0])
        sql_execute(conn, insert)
    logging.info('Insert completed {}'.format(str(datetime.datetime.now())))

def recreate_database(conn):
    logging.info('Dropping nba_stats_test database {}'.format(str(datetime.datetime.now())))
    sql_execute(conn, 'drop database nba_stats_TEST')
    sql_execute(conn, 'create database nba_stats_TEST')
    logging.info('nba_stats_test schema re-created {}'.format(str(datetime.datetime.now())))

def liquibase_call():
    logging.info('Calling liquibase for nba_stats_test refresh {}'.format(str(datetime.datetime.now())))
    os.system("""liquibase --driver=com.mysql.jdbc.Driver \
                 --classpath="/Users/Philip/Downloads/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46-bin.jar" \
                 --changeLogFile="/Users/Philip/Documents/NBA prediction script/Changelogs/nba_stats_prod_changeLogV3_TEST.xml" \
                 --url="jdbc:mysql://localhost:3306/nba_stats_TEST?autoReconnect=true&amp;useSSL=false" \
                 --username=root \
                 --password=Sk1ttles update""")
    logging.info('Incrementials Pipeline completed {}'.format(str(datetime.datetime.now())))

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

if __name__ == '__main__':
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='w', level=logging.INFO)
    logging.info('Attempting to connect to nba_stats_staging database {}'.format(str(datetime.datetime.now())))
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    logging.info('Successfully connected to nba_stats_staging {}'.format(str(datetime.datetime.now())))
    out_file = '/Users/Philip/Documents/NBA\ Database\ Backups/nba_stats_{}.sql'.format(str(datetime.date.today()))

    back_up_db(out_file)
    compress_backup(out_file)
    nba_espn_incrementals.main()
    nba_espn_team_incrementals.main()
    nba_espn_team_standings_incrementals.main()
    box_score_nba_ref_incrementals.main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    nba_stats_adv_team_box_score.main()
    nba_stats_fig4_team_box_score.main()
    nba_stats_misc_team_box_score.main()
    nba_stats_scoring_team_box_score.main()
    nba_stats_traditional_team_box_score.main()
    espn_update_season_date.main()
    espn_team_name_update.main()
    team_name_update_team_boxscore.main()
    player_name_nba_ref_boxscore.main()
    #espn_delete_max_season(connection)
    #insert_into_nba_stats(connection)
    date_lookup_table.main()
    active_roster.main(sys.argv[5], sys.argv[6])
    injured_players.main(sys.argv[5])
    #recreate_database(connection)
    #liquibase_call()
