import json
from dataclasses import dataclass
import bs4
import logging
import requests
import requests.api
import pandas as pd
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.DEBUG)

BASE: str = "https://www.vlr.gg/"
MATCHES: str = "matches/"
RANKINGS: str = "rankings/"
TEAM: str = "team/"
NEWS: str = "news/"
FORUMS: str = "forum/"
PLAYER: str = "player/"


@dataclass(order=True)
class Player:
    match_id: int
    match_date: str
    match_score: str
    game_index: int
    map: int
    game_score: str
    player_agent: str
    rounds_played: int
    player_id: int
    player_name: str
    team_id: int
    team_name: str
    team_name_short: str
    team_vlr_rating: int
    player_adr: int
    player_kills: int
    player_deaths: int
    player_assists: int
    player_kpr: float
    opponent_id: int
    opponent_name_long: str
    opponent_name_short: str
    opponent_vlr_rating: int

# player = Player('s0m', 655, 400, 12, 5, 6, 210233, '2023-06-31')


class RequestString(str):
    def __init__(self, string: str) -> None:
        self.string = string

    def __repr__(self) -> str:
        return self.string


def get_soup(address: str) -> BeautifulSoup:
    """Allows bs4 to parse the required address"""
    request_link: str = BASE + address
    requested = requests.get(request_link)
    logging.debug(f"requesting url: {request_link} : {str(requested)}")
    soup = bs4.BeautifulSoup(requested.content, 'lxml')
    if requested.status_code == 404:
        return None
    else:
        return soup


def get_game_soups(match_id: int = None, match_soup: BeautifulSoup = None) -> list:
    """Retrieves a list of bs4 strings for each map, removes 'all' game and any non played maps"""
    if match_soup is None:
        match_soup = get_soup(str(match_id))
    stat_tab = match_soup.find(class_="vm-stats-container")
    game_soups = stat_tab.find_all(class_="vm-stats-game")
    game_soups = [game for game in game_soups if game.get(
        'data-game-id') != 'all' and get_game_map(game_soup=game) != 'TBD']
    return game_soups


def add_player_to_dataFrame(dataframe, player):
    dataframe.append(player)


def get_match_id_from_soup(match_soup: BeautifulSoup) -> int:
    """Returns the match id from a given match soup

    Args:
        match_soup (BeautifulSoup): a soup from the match

    Returns:
        int: match id in integer form
    """
    return int(match_soup.find(class_='vm-stats').get('data-url').split('/', maxsplit=2)[1])


def get_match_data(match_soup: BeautifulSoup = None, match_id: int = None) -> list:
    """Returns a list of player objects which contain all the columns of data in key value pairs.

    Args:
        match_soup (BeautifulSoup, optional): Directly pass the pre fetched bs4 object to improve performace. Defaults to None.\n
        match_id (int, optional): The function will fetch the soup before continuing. Defaults to None.

    Returns:
        list: A list of player objects containing all data associated to them in a match
    """
    match_data = []
    if match_id is None:
        match_id = get_match_id_from_soup(match_soup)
    game_soups = get_game_soups(match_soup=match_soup)
    match_date = get_match_date(match_soup=match_soup)
    # match_style = get_match_style(match_soup=match_soup)
    # match_event = get_match_event(match_soup=match_soup)
    match_score = get_match_score(match_soup=match_soup)
    team_name_long = get_team_names_long(match_soup=match_soup)
    team_id = get_team_ids(match_soup=match_soup)
    team_elo = get_team_elos(match_soup=match_soup)
    # Looping through each map in a series (match)
    # Sets the information that changes between maps
    # creates lists for each variable in order of players retrieved
    for i, game_soup in enumerate(game_soups):
        # not including games that are less than 10 players because they shouldnt exist
        player_names = get_player_names(game_soups[i])
        if len(player_names) < 10:
            continue
        game_index = i
        team_name_short = get_team_names_short(game_soup=game_soup)
        player_id = get_player_ids(game_soup)
        player_agent = get_player_agents(game_soup)
        map = get_game_map(game_soup)
        rounds_played = get_game_rounds_played(game_soup)
        player_adr = get_player_adrs(game_soup)
        player_kills = get_player_kills(game_soup)
        player_deaths = get_player_deaths(game_soup)
        player_assists = get_player_assists(game_soup)
        game_score = get_game_score(game_soup)

        # Loops through all players playing a map
        for index, player_name in enumerate(player_names):
            # Team 1
            if index <= 4:
                player_team_long = team_name_long[0]
                player_team_short = team_name_short[0]
                player_team_id = team_id[0]
                player_team_elo = team_elo[0]
                player_opponent_long = team_name_long[1]
                player_opponent_short = team_name_short[5]
                player_opponent_id = team_id[1]
                player_opponent_elo = team_elo[1]
            # Team 2
            else:
                player_team_long = team_name_long[1]
                player_team_short = team_name_short[5]
                player_team_id = team_id[1]
                player_team_elo = team_elo[1]
                player_opponent_long = team_name_long[0]
                player_opponent_short = team_name_short[0]
                player_opponent_id = team_id[0]
                player_opponent_elo = team_elo[0]
            # Building a row for each player
            if isinstance(player_kills[index], str) or isinstance(rounds_played, str):
                player_kpr = -1
            else:
                player_kpr = round(player_kills[index] / int(rounds_played), 2)

            match_data.append(Player(
                match_id,
                match_date,
                match_score,
                game_index,
                map,
                game_score,
                player_agent[index],
                rounds_played,
                player_id[index],
                player_name,
                player_team_id,
                player_team_long,
                player_team_short,
                player_team_elo,
                player_adr[index],
                player_kills[index],
                player_deaths[index],
                player_assists[index],
                player_kpr,
                player_opponent_id,
                player_opponent_long,
                player_opponent_short,
                player_opponent_elo
            ))

    return match_data


def get_match_datas(match_ids: list, data_file: str = '', soups_file: str = ''):
    """
        returns match data for players specified, if all_players
        returns all player data from matches, returns the match_soups in a list as well
    """

    # Finding matches that have already been scraped into a dataset, only includes new matches to scrape
    used_match_ids = []
    data = []
    filename = 'default'
    makeNewFile = True
    try:
        data = pd.read_csv(data_file)
        if data_file is not None or data.columns != len(Player.__annotations__):
            used_match_ids = data.match_id.drop_duplicates().to_list()
            used_match_ids = [str(elem) for elem in used_match_ids]
            match_ids = [
                match for match in match_ids if match not in used_match_ids]
            print(f'DATASET DETECTED - APPPENDING {len(match_ids)} MATCHES')
            makeNewFile = False
        else:
            print(
                f"Incorrect number of columns. Creating a new file with name '{filename}.csv'")

    except FileNotFoundError:
        print(f"No file found. Creating new file with name '{filename}.csv'")

    # Handling loading the stored soups into a list
    stored_soups = None
    try:
        print(f"Loading saved soup file: {soups_file}")
        stored_soups = pd.read_csv(soups_file)
        # print(stored_soups)
    except FileNotFoundError:
        print('No stored soups found.')
        stored_soups = pd.DataFrame(stored_soups, columns=['match_id', 'soup'])

    # Looping through each match in the match_id list
        # Sets the information that doesnt through map/players
    for match_id in match_ids:
        print(f"Match {match_ids.index(match_id)} / {len(match_ids)}")
        # Getting the index of a match soup by comparing to stored_soup match_id
        index = stored_soups.loc[stored_soups['match_id']
                                 == int(match_id)].index.tolist()
        if bool(index):
            # Making sure the loaded soup is in the bs4 format before going to scraping functions
            if str(type(stored_soups['soup'][index[0]])) == "<class 'bs4.BeautifulSoup'>":
                match_soup = stored_soups['soup'][index[0]]
            else:
                match_soup = BeautifulSoup(
                    stored_soups['soup'][index[0]], 'html.parser')
        else:  # If there is no match in the stored soups it will look up the match and store to the list for future use
            match_soup = get_soup(str(match_id))
            stored_soups.loc[len(stored_soups.index)] = [match_id, match_soup]

        data += get_match_data(match_soup)

    return data, stored_soups


def get_match_date(match_id: int = None, match_soup: BeautifulSoup = None) -> str:
    """Returns the date of the match"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    date = RequestString(match_soup.find(
        class_="moment-tz-convert").get('data-utc-ts').split(' ')[0])
    return date.strip('\n').strip('\t')


def get_match_style(match_id: int = None, match_soup: BeautifulSoup = None) -> str:
    """Returns the match style (i.e. Bo3)"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    match_style = RequestString(match_soup.find_all(
        class_="match-header-vs-note")[1].text)
    return match_style.strip('\n').strip('\t')


def get_match_event(match_id: int = None, match_soup: BeautifulSoup = None) -> str:
    """Returns the event that the match took place in"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    return match_soup.find(class_="match-header-event").text


def get_match_score(match_id: int = None, match_soup: BeautifulSoup = None) -> str:
    """Returns the match score in a string (2:1)"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    total_score = RequestString(match_soup.find(class_="js-spoiler").text)
    return total_score.strip('\n').strip('\t').replace('\t', '').replace('\n', '')


def get_team_names_long(match_id: int = None, match_soup: BeautifulSoup = None) -> list:
    """Returns the full team names listed on VLR"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    # team_tab = soup.find(class_="match-header-vs")
    team_names = [RequestString(result.text).strip('\n').strip(
        '\t') for result in match_soup.find_all(class_="wf-title-med")]
    return team_names


def get_team_names_short(match_id: int = None, game_soup: BeautifulSoup = None) -> list:
    """Returns shortened versions of the team names"""
    if game_soup is None:
        # Teams stay the same between maps so using map 1 to determine order is ok
        game_soup = get_game_soups(match_id)[0]
    player_teams = []
    player_teams_html = game_soup.find_all("a", href=True)
    for htelements in player_teams_html:
        player_teams.append(htelements.text.split('\n')[-2].replace('\t', ''))
    return player_teams


def get_team_ids(match_id: int = None, match_soup: BeautifulSoup = None) -> list:
    """Returns team ids for a match - [Team1, Team2]"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    team_ids = []
    team_tab = match_soup.find(class_="match-header-vs")
    for i in range(2):
        team_ids.append(int(team_tab.find(
            "a", class_=f"match-header-link wf-link-hover mod-{i+1}").get('href').split('/')[2]))
    return team_ids


def get_team_elos(match_id: int = None, match_soup: BeautifulSoup = None) -> list:
    """Returns vlr ratings for both teams [Team1, Team2]"""
    team_elos = []
    if not match_soup:
        match_soup = get_soup(str(match_id))
    team_tab = match_soup.find(class_="match-header-vs")
    for result in team_tab.find_all(class_="match-header-link-name-elo"):
        if RequestString(result.text).strip('\n').strip('\t').strip('\n').strip('[').strip(']') == '':
            team_elos.append(-1)
            continue
        team_elos.append(int(RequestString(result.text).strip(
            '\n').strip('\t').strip('\n').strip('[').strip(']')))
    return team_elos


def get_opponent_elos(match_id: int = None, match_soup: BeautifulSoup = None) -> list:
    """Returns reversed vlr ratings for both teams [Team2, Team1]"""
    opponent_elos = []
    if not match_soup:
        match_soup = get_soup(str(match_id))
    team_tab = match_soup.find(class_="match-header-vs")
    for result in team_tab.find_all(class_="match-header-link-name-elo"):
        if RequestString(result.text).strip('\n').strip('\t').strip('\n').strip('[').strip(']') == '':
            opponent_elos.append(-1)
            continue
        opponent_elos.append(int(RequestString(result.text).strip(
            '\n').strip('\t').strip('\n').strip('[').strip(']')))
    return opponent_elos[::-1]


def get_opponent_ids(match_id: int = None, match_soup: BeautifulSoup = None) -> list:
    """Returns reversed team ids for both teams [Team2, Team1]"""
    if not match_soup:
        match_soup = get_soup(str(match_id))
    opponent_ids = []
    team_tab = match_soup.find(class_="match-header-vs")
    for i in range(2):
        opponent_ids.append(int(team_tab.find(
            "a", class_=f"match-header-link wf-link-hover mod-{i+1}").get('href').split('/')[2]))
    return opponent_ids[::-1]


def get_player_names(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of names in a map, in retrieved order from vlr"""
    player_names_html = game_soup.find_all(class_="text-of")
    player_names = []
    for htelement in player_names_html:
        player_names.append(RequestString(htelement.text).split(' ')[
                            0].replace('\t', '').replace('\n', ''))
    return player_names


def get_player_game_stats(game_soup: BeautifulSoup, player_index: int = False, stat_column: str = False) -> list:
    """Pulls info from the stats table and gives a table of the values

    Args:
        game_soup (BeautifulSoup): Submit a soup to improve speed. Defaults to None.
        player_index (int, optional): Option to return a specific row of player stats. Defaults to False.
        stat_column (str, optional): Option to return a specific row of player data. Defaults to False.

    Returns:
        list: Returns a pandas DataFrame with applicable column titles
    """
    player_stat_html = game_soup.find_all(class_="mod-stat")
    player_stat_list = []
    player_stat = []
    columns = ['player_rating', 'player_acs', 'player_kills', 'player_deaths', 'player_assists',
               'player_kills-deaths', 'player_kast', 'player_adr', 'player_hs', 'player_fk', 'player_fd', 'player_fk-fd']
    for index, htelement in enumerate(player_stat_html):
        #print(htelement.text.split('\n'))
        stat = htelement.text.replace('/', '').replace('\n', ' ').strip().split(' ')[0]
        try:
            stat = float(stat)
        except ValueError:
            stat = float(stat.replace('%', '')) / 100
        player_stat.append(stat)
        if (index + 1) % 12 == 0:
            player_stat_list.append(player_stat)
            player_stat = []
    player_stat_list = pd.DataFrame(player_stat_list, columns=columns)
    if not stat_column and not player_index:
        return player_stat_list
    if stat_column and not player_index:
        return player_stat_list[stat_column] 
    if stat_column and player_index:
        return player_stat_list.iloc[[player_index]]
    return player_stat_list[player_index][stat_column]


def get_player_kills(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of kill # in a map, in retrieved order from vlr"""
    player_kills_html = game_soup.find_all(class_="mod-stat mod-vlr-kills")
    player_kills = []
    for htelement in player_kills_html:
        if RequestString(htelement.text).strip().split('\n', maxsplit=1)[0] == '':
            player_kills.append('***')
        else:
            player_kills.append(
                int(RequestString(htelement.text).strip().split('\n', maxsplit=1)[0]))
    return player_kills


def get_player_deaths(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of death # in a map, in retrieved order from vlr"""
    player_deaths_html = game_soup.find_all(class_="mod-stat mod-vlr-deaths")
    player_deaths = []
    for htelement in player_deaths_html:
        if RequestString(htelement.find(class_='stats-sq').text).replace('/', '').strip().split('\n', maxsplit=1)[0] == '':
            player_deaths.append('***')
        else:
            player_deaths.append(int(RequestString(htelement.find(class_='stats-sq').text).replace('/', '').strip().split('\n', maxsplit=1)[0]))
    return player_deaths


def get_player_assists(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of assist # in a map, in retrieved order from vlr"""
    player_assists_html = game_soup.find_all(class_="mod-stat mod-vlr-assists")
    player_assists = []
    for htelement in player_assists_html:
        if RequestString(htelement.text).strip().split('\n', maxsplit=1)[0] == '':
            player_assists.append('***')
        else:
            player_assists.append(
                int(RequestString(htelement.text).strip().split('\n', maxsplit=1)[0]))
    return player_assists


def get_game_score(game_soup: BeautifulSoup = None) -> list:
    """Returns the score of an individual map (13:7) (Team1 Score, Team2 Score)"""
    game_score = f"{game_soup.find_all(class_='score')[0].text}: {game_soup.find_all(class_='score')[1].text}"
    return game_score


def get_game_rounds_played(game_soup: BeautifulSoup = None) -> int:
    """Returns the total amount of rounds played in a map"""
    return int(game_soup.find_all(class_='score')[0].text) + int(game_soup.find_all(class_='score')[1].text)


def get_game_map(game_soup: BeautifulSoup = None) -> str:
    """Returns the map played"""
    map_div = game_soup.find(class_='map')
    map = map_div.find('span', style='position: relative;').text.replace(
        "PICK", '').replace('\n', '').replace('\t', '')
    return map


def get_player_adrs(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of adr # in a map, in retrieved order from vlr"""
    player_adr_html = game_soup.find_all(class_="stats-sq mod-combat")
    player_adrs = []
    for htelement in player_adr_html:
        if RequestString(htelement.text).strip().split('\n', maxsplit=1)[0] == '':
            player_adrs.append('***')
        else:
            player_adrs.append(
                int(RequestString(htelement.text).strip().split('\n', maxsplit=1)[0]))
    return player_adrs


def get_player_agents(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of agents in a map, in retreived order from vlr"""
    players_agents_images = game_soup.find_all('img')
    players_agents = []
    if len(players_agents_images) < 10:
        for i in range(0, 10):
            players_agents.append('***')
    for image in players_agents_images:
        if (image.get("title")):
            players_agents.append(image.get("title"))
    return players_agents


def get_player_ids(game_soup: BeautifulSoup = None) -> list:
    """Returns a list of player ids in a map, in retrieved order from vlr"""
    player_ids = []
    player_ids_html = game_soup.find_all("a", href=True)
    for htelements in player_ids_html:
        player_ids.append((htelements)['href'].split('/')[2])
    return player_ids


def get_opponent_name_short(match_id: int = None, game_soup: BeautifulSoup = None) -> list:
    """Returns a reversed list of short team names in retrieved order"""
    if game_soup is None:
        # Teams stay the same between maps so using map 1 to determine order is ok
        game_soup = get_game_soups(match_id)[0]
    player_teams = []
    player_teams_html = game_soup.find_all("a", href=True)
    for htelements in player_teams_html:
        player_teams.append(htelements.text.split('\n')[-2].replace('\t', ''))
    return player_teams[::-1]


def get_opponent_name_long(match_id: int = None, soup: BeautifulSoup = None) -> list:
    """Returns a reversed list of long team names (Team2, Team1)"""
    if not soup:
        soup = get_soup(str(match_id))
    team_names = [RequestString(result.text).strip('\n').strip(
        '\t') for result in soup.find_all(class_="wf-title-med")]
    return team_names[::-1]


def get_player_infos(player_id: int) -> dict:
    """Gets player info from profile page"""
    player_soup = get_soup(PLAYER + str(player_id))
    header = player_soup.find(class_="wf-card mod-header mod-full")
    name = header.find(class_="wf-title").text
    real_name = header.find(class_="player-real-name").text
    twitter_link = header.find("a", href=True)
    twitch_link = header.find_next("a", href=True)
    country = header.find_all("div")
    return {"name": RequestString(name), "real_name": real_name,
            "twitter": twitter_link["href"], "twitch": twitch_link["href"],
            "country": RequestString(country[6].text)}


def get_player_match_ids(player_id: int, amount: int = 1) -> list:
    """Fetches a list of match ids from a given number of previous matches user defined length"""
    match_ids = []
    for i in range(int(amount/50) + 1):
        player_matches_soup = get_soup(
            PLAYER + MATCHES + str(player_id) + '/?page=' + str(i+1))
        matches = player_matches_soup.find_all(
            "a", class_="wf-card fc-flex m-item")
        for match in matches:
            match_ids.append(match.get("href").split('/')[1])
    return match_ids[0:amount]


def get_team_match_ids(team_id: int, amount: int = 1) -> list:
    """Fetches a list of match ids from previous games a team has played in user defined list length"""
    match_ids = []
    for i in range(int(amount/50) + 1):
        player_matches_soup = get_soup(
            TEAM + MATCHES + str(team_id) + '/?page=' + str(i+1))
        matches = player_matches_soup.find_all(
            "a", class_="wf-card fc-flex m-item")
        for match in matches:
            match_ids.append(match.get("href").split('/')[1])
    return match_ids[0:amount]


def to_json(filename: str, data: dict, indent: int = 4, append: bool = False) -> None:
    """Converts a python dictionary to json format"""
    with open(file=f"{filename}.json", mode="a") as file:
        json.dump(data, file, indent=indent)
        # Add a newline after each JSON object for readability
        file.write('\n')

# def to_csv(self : pd.DataFrame, filename : str = 'default') -> None:
#     """Converts a pandas dataframe to a .csv file"""
#     self.to_csv(f'{filename}.csv', index=False)

# print(get_player_ratings(get_game_soups(match_id=183795)[1]))
