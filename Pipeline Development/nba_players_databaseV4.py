import requests
from bs4 import BeautifulSoup
import re
import pymysql
import itertools

def file_extract():

    with open('nba_links_log.txt', 'r') as infile:
        return [r.strip('\n').split('\t')[1] for r in infile.readlines()]

##Method used to drop tables prior to updating the data model
def drop_tables(connection, table_names):

    for table in table_names:
        table = "".join([i for i in table.split()])
        exe = connection.cursor()
        exe.execute("drop table " + table)

##Method used to scrap the individual player stat's from each player's web page
def player_stat_scrapper(player):

    header_list, table_names, stats = [], [], []

    stats_link = requests.get(player)
    content = stats_link.content
    soup = BeautifulSoup(content, "html.parser")

    name = soup.find("h1").get_text() #finding player name
    bio = soup.find("ul", class_="player-metadata").get_text()
    raw_exp = re.search("Experience(.*\d)", bio) #extracting years of experience
    current_team = soup.find("li", class_="last").get_text()

    #error handling for rookies, rookies web pages do not show a number for years of experience
    #manually setting experience to zero when error present
    try:
        exp = int(raw_exp.group(1))
    except AttributeError:
        exp = 0

    table_heads = soup.find_all("tr", class_="stathead") #finding the names of tables on web page
    header = soup.find_all("tr", class_="colhead") #finding the header of each table
    stats_raw = soup.findAll(True, {'class':['oddrow', 'evenrow']}) #finding specific stats for seasons

    for i, n in zip(header, table_heads):
        header_list.append(" ".join([j.get_text() for j in i])) #creating list of all headers from each table
        table_names.append(n.get_text()) #creating list to store table names

    for p in stats_raw:
        stats.append(" ".join([i.get_text() for i in p])) #creating list of all stats collected

    index = int(len(stats) / len(table_names)) #finding break points within stats list based on years of experience

    loop_iterations = [stats[:index], stats[index:(len(stats) - index)], stats[(len(stats) - index):]] #splitting tables out of the stats list
    stat_dict = create_stat_dicts(loop_iterations, table_names, header_list, name, {})
    return table_names, header_list, stat_dict, current_team, exp

##Method to create dictionary for each player's stats tables are keys and values are made up of a list of each seasons stats
##this is a 3D dictionary compose of player_name:table_names[season_data]
def create_stat_dicts(loop_iterations, table_names, header_list, name, stat_dict):

    table = []
    for row in loop_iterations[0]: #iterating through rows of each table
        if row == 'No stats available.': #checking for players that have no career stats
            return None #returning none
        table.append(row)

    stat_dict[table_names[0]] = table

    if len(loop_iterations) > 1:
        return create_stat_dicts(loop_iterations[1:], table_names[1:], header_list, name, stat_dict)
    else:
        output_stats = {}
        output_stats[name] = stat_dict #creating dictionary containing player name as key, values are tables and data
        return output_stats

##Method to automate the create of SQL tables
def create_sql_table_statements(table_names, header_list, create_table_list):

    header = header_list[0].split()
    name = "".join([i for i in table_names[0].split()])

    create_table_statement = "create table " + name + "(\nPLAYER_ID int,\n"
    special_char = re.compile(r"[O]|\W")

    for field in header: #iterating through the fields in each tables header
        if special_char.findall(field): #finding if special characters exist
            field = "`" + field + "`" #bracketing field in `` if special characters persent

        if field == "TEAM" or field == "NAME" or field == "CURRENT_TEAM":
            create_table_statement += field + " varchar(50),\n"
        elif field == "EXPERIENCE":
            create_table_statement += field + " int,\n"
        elif field == "`SEASON`" or field == "`FGM-A`" or field == "`FTM-A`" or field == "`3PM-A`":
            create_table_statement += field + " text(100),\n"
        else:
            create_table_statement += field + " float(10),\n"

    create_table_list.append(create_table_statement[:-2] + ")")

    if len(table_names) > 1:
        return create_sql_table_statements(table_names[1:], header_list[1:], create_table_list)
    else:
        return create_table_list

##Method to execute create statements
def create_sql_tables(connection, create_table_list):

    for table_create in create_table_list:
        exe = connection.cursor()
        exe.execute(table_create)

##Method to automate the creation of insert statements into tables witin the database
def create_sql_insert_statements(connection, stat_dict, header_list, player_index, current_team, exp):

    #error handling to check for None entries for players that have no stats
    try:
        index_dict = {}
        for player in stat_dict: #iterating through player in dictionary containing player and all table data
            name = player #assing place holder variable
            special_char = player.find("'") #checking if player's name contains '
            if special_char != -1:
                name = player[:special_char] + "\\" + player[special_char:] #escaping special character

            player_statement = "insert into player_info (player_id, name, experience) values (" + str(player_index) + ", '" + name + "', "  + str(exp) + ")" #inserting data into player_info table
            #print(player_statement)
            insert_into_sql_table(connection, player_statement)

            for table in stat_dict[player]: #iterating through the tables within the players stats dictionary
                for season in range(len(stat_dict[player][table])): #iteration through the seasons within the dictionary
                    insert_statement = 'insert into ' + ''.join([i for i in table.split()]) + ' values ("' + str(player_index) + '", '\
                                       '"' + '", "'.join([p for p in stat_dict[player][table][season].split()]) + '")'


                    try:
                        print(insert_statement)
                        insert_into_sql_table(connection, insert_statement)
                    except:
                        print('INSERT FIAL: \n' + insert_statement + '\n')
    #skipping inserts on players that don't have stats
    except TypeError:
        pass

##Method to execute create statements
def insert_into_sql_table(connection, insert_statement):
    exe = connection.cursor()
    exe.execute(insert_statement)

#######   Main method   #######

myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit=True)

#player_links = espn_id_scrap()
#player_links = file_extract()
#print(player_links[983293])

count = 10   ### change to 0 if doing complete rebuild


#for i, player_id in enumerate(player_links[:50000]):
for i in range(0, 1):
    #print(player_id)
    player_id = 'http://www.espn.com/nba/player/stats/_/id/3001308/josh-gray'
    print(player_id)

    try:
        table_names, header_list, stat_dict, current_team, exp = player_stat_scrapper(player_id)

        if count == 0:
            table_names.append("player_info")
            header_list.append("NAME CURRENT_TEAM EXPERIENCE")
            drop_tables(myConnection, table_names)
            create_table_list = create_sql_table_statements(table_names, header_list, [])
            create_sql_tables(myConnection, create_table_list)
            count += 1

        create_sql_insert_statements(myConnection, stat_dict, header_list, i +6000020, current_team, exp)

    except AttributeError:
        pass

##for testing
#player_links = ["http://www.espn.com/nba/player/stats/_/id/2579458/marcus-thornton", "http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/6462/marcus-morris"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3213/al-horford"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/4237/john-wall", "http://www.espn.com/nba/player/stats/_/id/6580/bradley-beal"]  #"6580/bradley-beal",  #4237/john-wall
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3134880/kadeem-allen"]
