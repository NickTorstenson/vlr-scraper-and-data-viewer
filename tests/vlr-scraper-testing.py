from datetime import date
import pandas as pd
import vlrstatsfetcher.vlrscraperVbeta as vlrs
import bs4
import cProfile
import pstats

#sapi.get_match_by_id(183777)
#print(sapi.get_player_infos(864))
#sapi.to_json("T1-PRX-bothgame", sapi.get_match_by_id(167393))
#print(len(sapi.get_player_match_ids(864, 3)))
#sapi.to_json(sapi.get_team_match_ids(5248, 4))

americas = [
    2406,
    6961,
    2359,
    188,
    7389,
    1034,
    2,
    120,
    5248,
    2355
]
emea = [
    2593,
    1184,
    474,
    4915,
    2059,
    2304,
    1001,
    7035,
    8877,
    397
]
pacific =[
    8185,
    624,
    17,
    6199,
    14,
    5448,
    878,
    918,
    278,
    8304
]
today = date.today()
all_regions = americas + emea + pacific
#print(all_regions)
data = []
unique_matches = []
matches = []
# for team in all_regions:
#     matches += vlrs.get_team_match_ids(team, 50)
#     unique_matches = list(set(matches))
#     print(len(unique_matches))
    
MATCH = 184805

# test_soups = [[MATCH], [vlrs.get_soup(str(MATCH))]]
# test_soups = pd.DataFrame(test_soups)
# test_soups.to_csv('test_soups.csv', index=False)

# unique_matches = pd.DataFrame(unique_matches, columns=['match_id'])
# unique_matches.sort_values(by='match_id', ascending=False)
# unique_matches.to_csv('match_ids(AllRegionsLast50)', index=False)

unique_matches = pd.read_csv('match_ids.csv')['match_id'].values.tolist()

# with cProfile.Profile() as pr:
#     vlrs.get_match_datas(match_ids=[MATCH], soups_file='test_soups')

# stats = pstats.Stats(pr)
# stats.sort_stats(pstats.SortKey.TIME)
# #stats.print_stats()
# stats.dump_stats(filename='profiling2.prof')
# print(vlrs.get_player_adrs(vlrs.get_game_soups(183804)[0]))

#vlrs.to_csv(data, f'match_soups_storage({today})(1)')
#vlrs.get_match_player_data([64566])[1]
#print(unique_matches)
SOUPSFILE = r'C:\Users\nickt\OneDrive\Documents\GitHub\vlr-scraper-and-data-viewer\data\match_soups_storage(2023-04-24).csv'
storage = None
with cProfile.Profile() as pr:
    data = vlrs.get_match_datas(unique_matches, soups_file=SOUPSFILE)
    
stats = pstats.Stats(pr)
stats.sort_stats(pstats.SortKey.TIME)
stats.dump_stats(filename='newMethod10.prof')

match_data = pd.DataFrame(data[0])
print(match_data)
storage = pd.DataFrame(data[1])
match_data.to_csv(f'playerdata({today})', index=False)
storage.to_csv(SOUPSFILE, index=False)
