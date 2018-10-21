import requests
import datetime
from bs4 import BeautifulSoup
import re
import pymysql
import itertools
import logging

##Method used to scrap team links. These links allow the individual player's
##stats to be scraped from their individual stat's page
def find_team_names():

    links, team_links = [], []

    espn_link = requests.get("http://www.espn.com/nba/players")
    content = espn_link.content
    soup = BeautifulSoup(content, "html.parser")

    for i in soup.find_all('a', href=True): #Finding all the links embedded on the web page
        if i.text: #adding string of link to list
            links.append(i['href'])

    for link in links:
        if "http://www.espn.com/nba/team/_/name/" in link: #searching for specific team links
            team_links.append(link)
    return team_links

##Method used to scrape the links of individuals player's stat page
def player_id_scraper(team_links, player_links):

    raw_links, player_links = [], []
    for team in team_links: #iterating through the list of team links for roster of players

        index = team.find("team/") + 4
        spliced_link = team[:index] + '/roster' + team[index:] #creating new link pointing to the web page containing team roster

        link = requests.get(spliced_link) #point to roster web page
        content = link.content
        soup = BeautifulSoup(content, "html.parser")

        for i in soup.find_all('a', href=True): #finding all links
            if i.text:
                raw_links.append(i['href'])

        for link in raw_links:
            if "http://www.espn.com/nba/player/_/id/" in link: #finding specific links pointing to player's stat page
                index = link.find("player/") + len("player/") #finding index of break point in string, used for splicing
                spliced_link = link[:31] + "stats/" + link[31:] #adding "stats/" to the url from the roster page
                player_links.append(spliced_link)

    player_links = sorted(set(player_links)) #filtering out repeats from the spliced links list
    return player_links

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
    try:
        current_team = soup.find("li", class_="last").get_text()
    except AttributeError:
        current_team = 'no team'

    #error handling for rookies, rookies web pages do not show a number for years of experience
    #manually setting experience to zero when error present
    try:
        bio = soup.find("ul", class_="player-metadata").get_text()
        raw_exp = re.search("Experience(.*\d)", bio) #extracting years of experience
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

    try:
        index = int(len(stats) / len(table_names)) #finding break points within stats list based on years of experience
        loop_iterations = [stats[:index], stats[index:(len(stats) - index)], stats[(len(stats) - index):]] #splitting tables out of the stats list
        stat_dict = create_stat_dicts(loop_iterations, table_names, header_list, name, {})
        return table_names, header_list, stat_dict, current_team, exp
    except ZeroDivisionError:
        return None

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

    find_max_id = 'select max(player_id) from nba_stats.player_info'
    ## UNCOMMENT AFTER TESTING##
    #find_max_id = 'select max(player_id) from nba_stats_backup.player_info'
#    print(insert_into_sql_table(connection, find_max_id)[0][0])
    player_index = insert_into_sql_table(connection, find_max_id)[0][0] + 100 + player_index
    #print(player_index)
    #print(stat_dict)
    flag = False

    try:
        index_dict = {}
        for player in stat_dict: #iterating through player in dictionary containing player and all table data
            name = player #assing place holder variable
            special_char = player.find("'") #checking if player's name contains '
            if special_char != -1:
                name = player[:special_char] + "\\" + player[special_char:] #escaping special character\

            find_player_id = "select player_id from nba_stats.player_info where name like '{}'".format(name)
            ## UNCOMMENT AFTER TESTING##
            #find_player_id = "select player_id from nba_stats_backup.player_info where name like '{}'".format(name)
            result = insert_into_sql_table(connection, find_player_id)

            try:
                player_index = result[0][0]
            except IndexError:
                player_statement = "insert into player_info values (" + str(player_index) + ", '" + name + "', '" + current_team + "', " + str(exp) + ")" #inserting data into player_info table
                insert_into_sql_table(connection, player_statement)
                flag = True
                logging.info('Creating new player_id for {}'.format(name))

            for table in stat_dict[player]: #iterating through the tables within the players stats dictionary
                for season in range(len(stat_dict[player][table])): #iteration through the seasons within the dictionary
                    insert_statement = 'insert into ' + ''.join([i for i in table.split()]) + ' values ("' + str(player_index) + '", '\
                                       '"' + '", "'.join([p for p in stat_dict[player][table][season].split()]) + '")'
                    if flag == True or (flag == False and stat_dict[player][table][season].split()[0][-2:] == '19'):
                    #if flag == True or (flag == False and stat_dict[player][table][season].split()[0][-2:] == '18'):
                        try:
                            insert_into_sql_table(connection, insert_statement)
                        except:
                            logging.info('[FAILED INSERT]: {}'.format(insert_statement))
                            #print(insert_statement)
                    else:
                        pass
    #skipping inserts on players that don't have stats
    except TypeError:
        pass

##Method to execute create statements
def insert_into_sql_table(connection, insert_statement):
    exe = connection.cursor()
    exe.execute(insert_statement)
    return exe.fetchall()

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    team_links = find_team_names()
    player_links = player_id_scraper(team_links, [])
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    logging.info('Beginning ESPN players incrementals pipeline {}'.format(str(datetime.datetime.now())))

    for i, player_id in enumerate(player_links):
        try:
            table_names, header_list, stat_dict, current_team, exp = player_stat_scrapper(player_id)
        except TypeError:
            pass
        if i == 0:
            table_names.append("player_info")
            header_list.append("NAME CURRENT_TEAM EXPERIENCE")
            drop_tables(myConnection, table_names)
            create_table_list = create_sql_table_statements(table_names, header_list, [])
            create_sql_tables(myConnection, create_table_list)

        try:
            create_sql_insert_statements(myConnection, stat_dict, header_list, i +1, current_team, exp)
        except TypeError:
            pass
    logging.info('ESPN players incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
##for testing
#player_links = ["http://www.espn.com/nba/player/stats/_/id/2579458/marcus-thornton", "http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/6462/marcus-morris"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3213/al-horford"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/4237/john-wall", "http://www.espn.com/nba/player/stats/_/id/6580/bradley-beal"]  #"6580/bradley-beal",  #4237/john-wall
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3134880/kadeem-allen"]
