import requests
from bs4 import BeautifulSoup
import re

def find_team_names():

    links, team_links = [], []

    espn_link = requests.get("http://www.espn.com/nba/players")
    content = espn_link.content
    soup = BeautifulSoup(content, "html.parser")

    for i in soup.find_all('a', href=True):
        if i.text:
            links.append(i['href'])

    for link in links:
        if "http://www.espn.com/nba/team/_/name/" in link:
            team_links.append(link)
    #print(team_links)

    return team_links

    #team_names_raw = soup.find_all("div", class_="mod-container mod-open-list mod-no-footer mod-teams-list-small")
    #print(team_names_raw)
    #for i in team_names_raw:
    #    temp = re.search("(href=\"http://www.espn.com/nba/team/_/name/).*?\"", i)
    #    print(temp.group(0))
    #team_links = soup.find_all("href")
    #print(team_links)

    #team_names_raw = soup.find("div", id="my-players-table")  #"(.*/nba/team/roster/_/name/).*"
    #print(team_names_raw)



    #filtered_names = re.search("(team/roster/_/name/).*\"", team_names_raw)
    #names = filtered_names.group(0)

    #print(names)

def player_id_scraper(team_links, player_links):

    raw_links, player_links = [], []
    for team in team_links:

        index = team.find("team/") + 4
        spliced_link = team[:index] + '/roster' + team[index:]

        link = requests.get(spliced_link)
        content = link.content
        soup = BeautifulSoup(content, "html.parser")

        for i in soup.find_all('a', href=True):
            if i.text:
                raw_links.append(i['href'])

        for link in raw_links:
            if "http://www.espn.com/nba/player/_/id/" in link:

                index = link.find("player/") + len("player/")
                spliced_link = link[:31] + "stats/" + link[31:]
                player_links.append(spliced_link)
    player_links = sorted(set(player_links))
    return player_links

team_links = find_team_names()
player_links = player_id_scraper(team_links, [])

[print(i) for i in player_links]
print(len(player_links))
