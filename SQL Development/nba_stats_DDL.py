"""
Pipeline designed to create entites and relationships between entities within the nba_stats database
This pipeline needs to be run after the completion of the active_roster.py pipeline because the active_rosters
entity needs to be in place for relationships to be formed
"""

import pymysql

def drop_tables(connection):
    drop_statement = 'drop table regular_season_averages'
    sql_execute(drop_statement, connection)

def create_table_regular_season_averages(connection):

    create_statement = 'create table regular_season_averages(\
                                         select player_info.player_id,\
                                         reg.season,\
                                         reg.team,\
                                         reg.GP,\
                                         reg.GS,\
                                         reg.MIN,\
                                         reg.`FGM-A`,\
                                         reg.`FG%`,\
                                         reg.`3PM-A`,\
                                         reg.`3P%`,\
                                         reg.`FTM-A`,\
                                         reg.`FT%`,\
                                         reg.`OR`,\
                                         reg.DR,\
                                         reg.REB,\
                                         reg.AST,\
                                         reg.BLK,\
                                         reg.STL,\
                                         reg.PF,\
                                         reg.`TO`,\
                                         reg.PTS\
                            from player_info\
                            join regularseasonaverages as reg on reg.player_id = player_info.player_id)'

    sql_execute(create_statement, connection)

    alter_statement_FK = 'alter table regular_season_averages\
                          add constraint regular_season_averages_FK\
                          foreign key (player_id) references active_rosters (player_id)'

    sql_execute(alter_statement_FK, connection)

    alter_statement_PK = 'alter table regular_season_averages\
                       add constraint regular_season_averages_PK primary key(player_id, season)'

    sql_execute(alter_statement_PK, connection)

#def

def sql_execute(query, connection):
    exe = connection.cursor()
    exe.execute(query)


### main function

myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats', autocommit=True)
drop_tables(myConnection)
create_table_regular_season_averages(myConnection)
