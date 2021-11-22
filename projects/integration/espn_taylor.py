from numpy import mat, nan
import requests
from textwrap import dedent
from pandas import DataFrame, Series
import pandas as pd
from utilities import (LICENSE_KEY, generate_token, master_player_lookup, SWID,
                       ESPN_S2)
import sqlite3
import json
############
# PARAMETERS
############
LEAGUE_ID = 1011602
TEAM_ID = 10

BASE_URL = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{LEAGUE_ID}?view='
## Potential Options for view= : mTeam, mBoxscore, mRoster, mSettings, kona_player_info, player_wl, mSchedule

# ROSTER DATA
roster_url = BASE_URL + 'mRoster'
roster_json = requests.get(roster_url, cookies= {'swid': SWID, 'espn_s2': ESPN_S2}).json()
list_of_rosters = roster_json['teams']
rosters0 = list_of_rosters[0]
list_of_players_on_roster0 = rosters0['roster']['entries']
roster0_player0 = list_of_players_on_roster0[0]
roster0_player0.keys()
roster0_player0['playerPoolEntry']['player']['fullName']
## We need 3 things for each player
### 1) ESPN Player ID
### 2) If they are starting and at what position (ex: RB or Flex)
### 3) Player Name and Position

def process_player1(player):
    dict_to_return = {}
    dict_to_return['team_position'] = player['lineupSlotId']
    dict_to_return['espn_id'] = player['playerId']
    dict_to_return['name'] = player['playerPoolEntry']['player']['fullName']
    dict_to_return['player_position'] = player['playerPoolEntry']['player']['defaultPositionId']
    return dict_to_return

process_player1(roster0_player0)
processed_roster = [process_player1(player) for player in list_of_players_on_roster0]
[player['name'] for player in processed_roster]

## Create Position Mapping
TEAM_POSITION_MAP = {
    0: 'QB', 1: 'TQB', 2: 'RB', 3: 'RB/WR', 4: 'WR', 5: 'WR/TE',
    6: 'TE', 7: 'OP', 8: 'DT', 9: 'DE', 10: 'LB', 11: 'DL',
    12: 'CB', 13: 'S', 14: 'DB', 15: 'DP', 16: 'D/ST', 17: 'K',
    18: 'P', 19: 'HC', 20: 'BE', 21: 'IR', 22: '', 23: 'RB/WR/TE',
    24: 'ER', 25: 'Rookie', 'QB': 0, 'RB': 2, 'WR': 4, 'TE': 6,
    'D/ST': 16, 'K': 17, 'FLEX': 23, 'DT': 8, 'DE': 9, 'LB': 10,
    'DL': 11, 'CB': 12, 'S': 13, 'DB': 14, 'DP': 15, 'HC': 19
}

PLAYER_POSITION_MAP = {1: 'QB', 2: 'RB', 3: 'WR', 4: 'TE', 5: 'K', 16: 'D/ST'}

def process_player2(player):
    dict_to_return = {}
    dict_to_return['team_position'] = TEAM_POSITION_MAP[player['lineupSlotId']]
    dict_to_return['espn_id'] = player['playerId']
    dict_to_return['name'] = player['playerPoolEntry']['player']['fullName']
    dict_to_return['player_position'] = PLAYER_POSITION_MAP[player['playerPoolEntry']['player']['defaultPositionId']]
    return dict_to_return

process_player2(roster0_player0)
[process_player2(player) for player in list_of_players_on_roster0]
roster0 = DataFrame([process_player2(player) for player in list_of_players_on_roster0])
roster0.query("team_position == 'WR'")
wrs = roster0.query("team_position == 'WR'")
## Add in suffix to team_position (ex: WR1 and WR2)
suffix = Series(range(1,len(wrs) + 1), index=wrs.index)
wrs['team_position'] = wrs['team_position'] + suffix.astype(str)

# Create a function to add suffix to all relevant positions
def add_pos_suffix(df_subset):
    if len(df_subset) > 1:
        suffix = Series(range(1, len(df_subset) + 1), index=df_subset.index)

        df_subset['team_position'] = df_subset['team_position'] + suffix.astype(str)
    return df_subset

## Run function on every position in our roster dataframe
### My First Take is below
### rost_by_pos = [roster0.loc[roster0['team_position'] == pos] for pos in list(roster0['team_position'].value_counts().index)]
### pd.concat([add_suffix(pos_df) for pos_df in rost_by_pos])

roster0['team_position'].unique() # This gets a list of all unique positions
roster0_df2 = pd.concat([add_pos_suffix(roster0.query(f"team_position == '{pos}'")) for pos in roster0['team_position'].unique()])

# Identify starters and create new Start column
## Hint: ~ (tilde) flips binary outcomes
roster0_df2['start'] = ~roster0_df2['team_position'].str.startswith('BE')

# Create a function that takes a roster and adds in team position suffixes and starter status
def process_players(entries):
    roster_df = DataFrame([process_player2(player) for player in entries])

    roster_df2 = pd.concat([add_pos_suffix(roster_df.query(f"team_position == '{pos}'")) for pos in roster_df['team_position'].unique()])
    roster_df2['start'] = ~roster_df2['team_position'].str.startswith('BE')
    return roster_df2

process_players(list_of_players_on_roster0)

# Create a function that wraps (calls) both functions inside of it, adds team id, then returns it
def process_roster(team):
    roster_df = process_players(team['roster']['entries'])
    team_id = team['id']

    roster_df['team_id'] = team_id
    return roster_df

rosters1 = list_of_rosters[1]
process_roster(rosters1)

## Run this on every team and stick the dfs together for a complete roster
all_rosters = pd.concat([process_roster(roster) for roster in list_of_rosters], ignore_index=True)
all_rosters.sample(15)

#######
# View Boxscore Data to add actual scores that may have occurred (ex: looking after a Thursday night game)
boxscore_url = BASE_URL + 'mBoxscore'
boxscore_json = requests.get(boxscore_url, cookies= {'swid': SWID, 'espn_s2': ESPN_S2}).json()

## The above code is to get the current data - below we are overwriting to work with the data from the book
with open('./projects/integration/raw/espn/boxscore.json') as f:
    boxscore_json = json.load(f)

boxscore_json.keys()
matchup_list = boxscore_json['schedule']
matchup0 = matchup_list[0]
matchup0_home0 = matchup0['home']['rosterForMatchupPeriod']['entries'][0]
matchup0_home0

# Create a function to process actual points for players who have played
def proc_played(played):
    dict_to_return = {}
    dict_to_return['espn_id'] = played['playerId']
    dict_to_return['actual'] = played['playerPoolEntry']['player']['stats'][0]['appliedTotal']

    return dict_to_return

proc_played(matchup0_home0)

## Note: 'rosterForMatchupPeriod' will only be present for midweek games (those that have already been played)
def proc_played_team(team):
    if 'rosterForMatchupPeriod' in team.keys():
        return DataFrame([proc_played(player) for player in team['rosterForMatchupPeriod']['entries']])
    else:
        return DataFrame()

proc_played_team(matchup0['home'])

def proc_played_matchup(matchup):
    return pd.concat([proc_played_team(matchup['home']), proc_played_team(matchup['away'])], ignore_index=True)

proc_played_matchup(matchup0)

# Get actual scores for all players in the weekly schedule (matchup_list)
scores = pd.concat([proc_played_matchup(game) for game in matchup_list])

# Connect this to our main roster
## Since I used actual rosters above, I need to generate the rosters from the provided document
with open('./projects/integration/raw/espn/roster.json') as f:
    roster_json = json.load(f)

all_rosters = pd.concat([process_roster(team) for team in roster_json['teams']], ignore_index=True)
all_rosters_w_points = pd.merge(all_rosters, scores, how='left')

# Include fantasymath id - Use the master_player_lookup function from utilities to get this info
fantasymath_players = master_player_lookup(generate_token(LICENSE_KEY)['token'])
fantasymath_players.head()
## Merge this info with our roster info
all_rosters_w_id = pd.merge(all_rosters_w_points, fantasymath_players[['fantasymath_id', 'espn_id']], how='left')
all_rosters_w_id.drop('espn_id',axis=1)

# Create the the 'get_league_rosters' function - Final Output: fantasymath_id, name, player_position, team_position, start, actual, and team_id
def get_league_rosters(fantasymath_lookup, league_id):
    roster_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mRoster'
    roster_json = requests.get(roster_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()
    rosters = roster_json['teams']
    all_rosters = pd.concat([process_roster(roster) for roster in rosters], ignore_index=True)

    boxscore_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mBoxscore'
    boxscore_json = requests.get(boxscore_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()
    matchups = boxscore_json['schedule']
    scores = pd.concat([proc_played_matchup(matchup) for matchup in matchups])

    # Add actuals data to all rosters
    if len(scores) > 0:
        all_rosters = pd.merge(all_rosters, scores, how='left')
    else:
        all_rosters['actual'] = nan

    all_rosters_w_id = pd.merge(all_rosters, fantasymath_lookup[['espn_id', 'fantasymath_id']], how='left').drop('espn_id', axis=1)

    return all_rosters_w_id

complete_league_rosters = get_league_rosters(fantasymath_players, LEAGUE_ID)




################################################################################################
################################################################################################
# TEAM DATA
## team_id | owner_id | owner_name | league_id
################################################################################################
################################################################################################
team_info_url = BASE_URL + 'mTeam'
team_json = requests.get(team_info_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()
team_json.keys()

# The info we need seems to be in 'Teams' and 'Members' - Process seperately then link them up
teams = team_json['teams']
members = team_json['members']

team0 = teams[0]
member0 = members[0]

def process_team(team):
    dict_to_return = {}
    dict_to_return['team_id'] = team['id']
    dict_to_return['owner_id'] = team['primaryOwner']
    return dict_to_return
process_team(team0)

def process_member(member):
    dict_to_return = {}
    dict_to_return['owner_id'] = member['id']
    dict_to_return['owner_name'] = member['firstName'] + ' ' + member['lastName']
    return dict_to_return
process_member(member0)

DataFrame([process_team(team) for team in teams])
DataFrame([process_member(member) for member in members])

# Create a function 'get_teams_in_league' that returns: team_id | owner_id | owner_name | league_id
def get_teams_in_league(league_id):
    teams_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mTeam'
    teams_json = requests.get(teams_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()

    teams = DataFrame([process_team(team) for team in teams_json['teams']])
    members = DataFrame([process_member(member) for member in teams_json['members']])
    teams = pd.merge(teams, members, how='left')
    teams['league_id'] = teams_json['id']
    return teams

get_teams_in_league(1011602)

################################################################################################
################################################################################################
# SCHEDULE INFO
## Per game: matchup_id | "home" team_id | "away" team_id | week
################################################################################################
################################################################################################
schedule_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{LEAGUE_ID}?view=mBoxscore'
schedule_json = requests.get(schedule_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()

matchup_list = schedule_json['schedule']
matchup0 = matchup_list[0]
# Required Info
matchup0['id']
matchup0['home']['teamId']
matchup0['away']['teamId']
matchup0['matchupPeriodId']

def process_matchup(matchup):
    dict_to_return = {}
    dict_to_return['matchup_id'] = matchup['id']
    dict_to_return['home_id'] = matchup['home']['teamId']
    dict_to_return['away_id'] = matchup['away']['teamId']
    dict_to_return['week'] = matchup['matchupPeriodId']
    return dict_to_return

process_matchup(matchup0)

all_matchups = DataFrame([process_matchup(matchup) for matchup in matchup_list])
## Per instructions: The schedule table includes: team1_id, team2_id, matchup_id, season, week, and league_id

def get_league_schedule(league_id, season):
    schedule_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mBoxscore'
    schedule_json = requests.get(schedule_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()

    schedule = DataFrame([process_matchup(matchup) for matchup in schedule_json['schedule']])
    schedule['season'] = season
    schedule['league_id'] = league_id
    schedule.rename(columns={'home_id':'team1_id', 'away_id':'team2_id'}, inplace=True)
    return schedule

get_league_schedule(LEAGUE_ID, 2021)