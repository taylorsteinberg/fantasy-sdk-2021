import requests
from pandas import DataFrame, Series
import pandas as pd
from utilities import (LICENSE_KEY, generate_token, master_player_lookup, SWID, ESPN_S2)
import json
from numpy import nan

pd.options.mode.chained_assignment = None

#################################
# Top-Level Functions
# 1) get_league_rosters
# 2) get_teams_in_league
# 3) get_league_schedule
#################################

def get_league_rosters(fantasymath_lookup, league_id):
    roster_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mRoster'
    roster_json = requests.get(roster_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()
    rosters = roster_json['teams']
    all_rosters = pd.concat([_process_roster(roster) for roster in rosters], ignore_index=True)

    boxscore_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mBoxscore'
    boxscore_json = requests.get(boxscore_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()
    matchups = boxscore_json['schedule']
    scores = pd.concat([_proc_played_matchup(matchup) for matchup in matchups])

    # Add actuals data to all rosters
    if len(scores) > 0:
        all_rosters = pd.merge(all_rosters, scores, how='left')
    else:
        all_rosters['actual'] = nan

    all_rosters_w_id = pd.merge(all_rosters, fantasymath_lookup[['espn_id', 'fantasymath_id']], how='left').drop('espn_id', axis=1)

    return all_rosters_w_id

def get_teams_in_league(league_id):
    teams_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mTeam'
    teams_json = requests.get(teams_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()

    teams = DataFrame([_process_team(team) for team in teams_json['teams']])
    members = DataFrame([_process_member(member) for member in teams_json['members']])
    teams = pd.merge(teams, members, how='left')
    teams['league_id'] = teams_json['id']
    return teams

def get_league_schedule(league_id, season):
    schedule_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/2021/segments/0/leagues/{league_id}?view=mBoxscore'
    schedule_json = requests.get(schedule_url, cookies={'swid': SWID, 'espn_s2': ESPN_S2}).json()

    schedule = DataFrame([_process_matchup(matchup) for matchup in schedule_json['schedule']])
    schedule['season'] = season
    schedule['league_id'] = league_id
    schedule.rename(columns={'home_id':'team1_id', 'away_id':'team2_id'}, inplace=True)
    return schedule


######################
# HELPER FUNCTIONS
######################

# ROSTER
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

def _process_player(player):
    dict_to_return = {}
    dict_to_return['team_position'] = TEAM_POSITION_MAP[player['lineupSlotId']]
    dict_to_return['espn_id'] = player['playerId']
    dict_to_return['name'] = player['playerPoolEntry']['player']['fullName']
    dict_to_return['player_position'] = PLAYER_POSITION_MAP[player['playerPoolEntry']['player']['defaultPositionId']]
    return dict_to_return

def _add_pos_suffix(df_subset):
    if len(df_subset) > 1:
        suffix = Series(range(1, len(df_subset) + 1), index=df_subset.index)

        df_subset['team_position'] = df_subset['team_position'] + suffix.astype(str)
    return df_subset

def _process_players(entries):
    roster_df = DataFrame([_process_player(player) for player in entries])

    roster_df2 = pd.concat([_add_pos_suffix(roster_df.query(f"team_position == '{pos}'")) for pos in roster_df['team_position'].unique()])
    roster_df2['start'] = ~roster_df2['team_position'].str.startswith(('BE','IR'))
    return roster_df2

def _process_roster(team):
    roster_df = _process_players(team['roster']['entries'])
    team_id = team['id']

    roster_df['team_id'] = team_id
    return roster_df

def _proc_played(played):
    dict_to_return = {}
    dict_to_return['espn_id'] = played['playerId']
    dict_to_return['actual'] = played['playerPoolEntry']['player']['stats'][0]['appliedTotal']

    return dict_to_return

def _proc_played_team(team):
    if 'rosterForMatchupPeriod' in team.keys():
        return DataFrame([_proc_played(player) for player in team['rosterForMatchupPeriod']['entries']])
    else:
        return DataFrame()

def _proc_played_matchup(matchup):
    return pd.concat([_proc_played_team(matchup['home']), _proc_played_team(matchup['away'])], ignore_index=True)

# TEAM
def _process_team(team):
    dict_to_return = {}
    dict_to_return['team_id'] = team['id']
    dict_to_return['owner_id'] = team['primaryOwner']
    return dict_to_return

def _process_member(member):
    dict_to_return = {}
    dict_to_return['owner_id'] = member['id']
    dict_to_return['owner_name'] = member['firstName'] + ' ' + member['lastName']
    return dict_to_return

# SCHEDULE
def _process_matchup(matchup):
    dict_to_return = {}
    dict_to_return['matchup_id'] = matchup['id']
    dict_to_return['home_id'] = matchup['home']['teamId']
    dict_to_return['away_id'] = matchup['away']['teamId']
    dict_to_return['week'] = matchup['matchupPeriodId']
    return dict_to_return

if __name__ == '__main__':
    ############
    # parameters
    ############

    LEAGUE_ID = 1011602

    ESPN_PARAMETERS = {
        'league_id': LEAGUE_ID,
        'swid': SWID,
        'espn_s2': ESPN_S2}

    token = generate_token(LICENSE_KEY)['token']
    lookup = master_player_lookup(token)

    rosters = get_league_rosters(lookup, LEAGUE_ID)
    teams = get_teams_in_league(LEAGUE_ID)
    schedule = get_league_schedule(LEAGUE_ID, 2021)