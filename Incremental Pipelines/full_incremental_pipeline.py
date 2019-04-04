"""
Script to wrap all incremental pipelines together

command line call:

python3 full_incremental_pipeline.py sql\ ddl/active_rosters_player_id.sql sql\ ddl/active_rosters_team_info.sql sql\ ddl/player_team_map.sql production_insert_statements/primary_queries production_insert_statements/multithread
"""

import subprocess
import os
import sys
import pymysql
import shutil
import datetime
import logging
import hashlib
import nba_stats_team_boxscores
import box_score_nba_ref_incrementals
import nba_espn_incrementals_mp
import nba_espn_team_incrementals
import nba_espn_team_standings_incrementals
import espn_update_season_date
import espn_team_name_update
import team_name_update_team_boxscore
import player_name_nba_ref_boxscore
import predictions_team_name_update
import date_lookup_table
import active_roster
import injured_players
import nba_stats_player_boxscores_inc
import migrate_to_prod_mp

def gen_time_stamp():
    return str(datetime.datetime.now())

def back_up_db(out_file):
    logging.info('Backing up nba_stats_backup database {}'.format(gen_time_stamp()))
    os.system('/usr/local/mysql-5.7.19-macos10.12-x86_64/bin/mysqldump --defaults-file=/Users/Philip/my.cnf nba_stats > "{}"'.format(out_file))
    logging.info('Backing up complete {}'.format(gen_time_stamp()))
    return

def compress_backup(out_file):
    logging.info('Compressing backup of nba_stats_backup database {}'.format(gen_time_stamp()))
    shutil.make_archive(out_file,
                        'zip',
                        "/Users/Philip/Documents/NBA Database Backups",
                        '{file}'.format(file=out_file.split('/')[-1]))
    logging.info('Compression complete {}'.format(gen_time_stamp()))
    return

def clean_up(out_file):
    os.remove(out_file)
    return

def espn_delete_max_season(conn):
    logging.info('Deleting Max season from ESPN tables {}'.format(gen_time_stamp()))
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
        delete = 'delete from nba_stats.{} where {} = 2019'.format(table, field)
        sql_execute(conn, delete)
    logging.info('Deletion from ESPN tables complete {}'.format(gen_time_stamp()))
    return

def insert_into_nba_stats(conn):
    logging.info('Beginning insert into nba_stats from nba_stats_staging {}'.format(gen_time_stamp()))
    get_tables = 'show tables'
    tables = sql_execute(conn, get_tables)

    for table in tables:
        insert = 'insert into nba_stats.{} (select * from nba_stats_staging.{})'.format(table[0], table[0])
        sql_execute(conn, insert)
    logging.info('Insert completed {}'.format(gen_time_stamp()))
    return

def extract_file(file):
    with open(file, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_cmd_str(cmd):
    return ''.join([i for i in cmd])

def refresh_player_team_map(conn, player_team_file):
    truncate = 'truncate table nba_stats.player_team_map;'
    sql_execute(conn, truncate)
    sql = gen_cmd_str(extract_file(player_team_file))
    sql_execute(conn, sql)
    return

def pipeline_auditlog(conn, desc):
    pipeline_insert = 'insert into nba_stats.pipeline_auditlog values ("{}", "{}", "{}")'.format(gen_hash(gen_time_stamp()), gen_time_stamp(), desc)
    sql_execute(conn, pipeline_insert)
    return

def gen_hash(row):
    return hashlib.md5(row.encode('utf-8')).hexdigest()

def recreate_database(conn):
    logging.info('Dropping nba_stats_prod database {}'.format(gen_time_stamp()))
    sql_execute(conn, 'drop database nba_stats_prod')
    sql_execute(conn, 'create database nba_stats_prod')
    logging.info('nba_stats_prod schema re-created {}'.format(gen_time_stamp()))
    return

def liquibase_call(file):
    logging.info('Calling liquibase for nba_stats_prod refresh {}'.format(gen_time_stamp()))
    os.system('''/usr/local/bin/liquibase --driver=com.mysql.jdbc.Driver \
                 --classpath="/Users/Philip/Downloads/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46-bin.jar" \
                 --changeLogFile={file} \
                 --url="jdbc:mysql://localhost:3306/nba_stats_prod?autoReconnect=true&amp;useSSL=false" \
                 --username=root \
                 --password=Sk1ttles \
                 update'''.format(file=file))
    logging.info('Incrementials Pipeline completed {stamp}'.format(stamp=gen_time_stamp()))
    return

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

if __name__ == '__main__':
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='w', level=logging.INFO)
    logging.info('Attempting to connect to nba_stats_staging database {}'.format(gen_time_stamp()))
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    logging.info('Successfully connected to nba_stats_staging {}'.format(gen_time_stamp()))
    out_file = '/Users/Philip/Documents/NBA Database Backups/nba_stats_{}.sql'.format(str(datetime.date.today()))
    desc = 'full incremental pipeline run'

    back_up_db(out_file)
    compress_backup(out_file)
    clean_up(out_file)
    active_roster.main(sys.argv[1], sys.argv[2])
    injured_players.main(sys.argv[1])
    nba_espn_incrementals_mp.main()
    nba_espn_team_incrementals.main()
    nba_espn_team_standings_incrementals.main()
    box_score_nba_ref_incrementals.main()
    nba_stats_team_boxscores.main()
    nba_stats_player_boxscores_inc.main()
    espn_update_season_date.main()
    espn_team_name_update.main()
    team_name_update_team_boxscore.main()
    player_name_nba_ref_boxscore.main()
    espn_delete_max_season(connection)
    insert_into_nba_stats(connection)
    refresh_player_team_map(connection, sys.argv[3])
    date_lookup_table.main()
    predictions_team_name_update.main()
    pipeline_auditlog(connection, desc)
    recreate_database(connection)
    liquibase_call('/Users/Philip/Documents/NBA\ prediction\ script/Changelogs/nba_stats_prod_changeLogProd.xml')
    migrate_to_prod_mp.main(sys.argv[4], sys.argv[5])
    liquibase_call('/Users/Philip/Documents/NBA\ prediction\ script/Changelogs/nba_stats_prod_changeLogKeys.xml')
