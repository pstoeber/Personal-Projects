import requests
from bs4 import BeautifulSoup
import re
import psycopg2

def player_stat_scrapper(player):

    header_list = []
    table_names = []
    stats = []

    stats_link = requests.get("http://www.espn.com/nba/player/stats/_/id/" + player)
    content = stats_link.content
    soup = BeautifulSoup(content, "html.parser")

    name = soup.find("h1").get_text()
    bio = soup.find("ul", class_="player-metadata").get_text()
    raw_exp = re.search("Experience.*(\d)", bio)
    exp = int(raw_exp.group(1))

    table_heads = soup.find_all("tr", class_="stathead")
    header = soup.find_all("tr", class_="colhead")
    stats_odd = soup.find_all("tr", class_="oddrow")
    stats_even = soup.find_all("tr", class_="evenrow")

    for i, n in zip(header, table_heads):
        header_list.append(" ".join([j.get_text() for j in i]))
        table_names.append(n.get_text())

    for p, s in zip(stats_odd, stats_even):
        stats.append(" ".join([i.get_text() for i in p]))
        stats.append(" ".join([i.get_text() for i in s]))

    loop_iterations = [stats[:exp +1], stats[exp +1:(len(stats) - exp) -1], stats[(len(stats) - exp) -1:]]

    #create_stat_dicts(table_names, header_list, stats, exp, name)
    stat_dict = create_stat_dicts(loop_iterations, table_names, name, {})

#def create_stat_dicts(table_names, header_list, stats, exp, name):
def create_stat_dicts(loop_iterations, table_names, name, stat_dict):

    table = []
    for row in loop_iterations[0]:
        table.append(row)
    stat_dict[table_names[0]] = table

    if len(loop_iterations) > 1:
        return create_stat_dicts(loop_iterations[1:], table_names[1:], name, stat_dict)
    else:
         return stat_dict


    #output = {}

    #print(name)
    #for t, j in enumerate(header_list):
    #    table =[]

    #    if t == 0:
    #        table.append(j)
    #        for row in stats[:exp +1]:
    #            table.append(row)
    #        output[table_names[t]] = table
    #    elif t +1 == len(header_list):
    #        table.append(j)
    #        for row in stats[(len(stats) - exp) -1:]:
    #            table.append(row)
    #        output[table_names[t]] = table
    #    else:
    #        table.append(j)
    #        for row in stats[exp +1:(len(stats) - exp) -1]:
    #            table.append(row)
    #        output[table_names[t]] = table
    #print(output)



player_id = "4237/john-wall"  #6580/bradley-beal

player_stat_scrapper(player_id)
