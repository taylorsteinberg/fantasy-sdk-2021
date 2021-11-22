import hosts.espn_taylor_final as site
import hosts.db as db
from os import path
import datetime as dt
from pandas import DataFrame, Series
import sqlite3
import wdis
import pandas as pd
import numpy as np
from pathlib import Path
from os import path
from utilities import (LICENSE_KEY, generate_token, master_player_lookup, 
                        get_sims, get_players, DB_PATH, OUTPUT_PATH, 
                        schedule_long)
LEAGUE_ID = 1011602
WEEK = 11

##################
################## LEFT OFF HERE - creating the wdis_by_position function
##################
def wdis_by_position(position, sims, roster, opponents_starters):
    wdis_options = _wdis_options_by_position(position, roster)
    current_starters = roster.query("start")

    df = wdis.calculate(sims, current_starters['fantasymath_id'], opponents_starters, set(wdis_options) & set(sims.columns))
    # Add position to df
    df['pos'] = position
    # Set index name to player's name
    df.index.name = 'player'
    # Set index to position and player's name
    df.reset_index(inplace=True)
    df.set_index(['pos', 'player'], inplace=True)
    return df

def _wdis_options_by_position(team_pos, roster):
    is_wdis_elig = ((roster['player_position'].astype(str).apply(lambda x: x in team_pos)) & ~roster['start']) | (roster['team_position'] == team_pos)   
    return list(roster.loc[is_wdis_elig, 'fantasymath_id'])

def get_positions(roster):
    return [pos for pos in roster.query("start")['team_position']]

if __name__ == '__main__':
    # open database connection
    conn = sqlite3.connect(DB_PATH)

    ########################
    # Load team and schedule data from DB
    ########################
    teams = db.read_league('teams', LEAGUE_ID, conn)
    schedule = db.read_league('schedule', LEAGUE_ID, conn)
    league = db.read_league('league', LEAGUE_ID, conn)

    # Get Parameters from league dataframe
    TEAM_ID = league['team_id'][0]
    HOST = league['host'][0]
    LEAGUE_NAME = league['name'][0]
    SCORING = {}
    SCORING['qb'] = league['qb_scoring'][0]
    SCORING['skill'] = league['skill_scoring'][0]
    SCORING['dst'] = league['dst_scoring'][0]

    ########################
    # Get current rosters & available players for the week
    ########################
    token = generate_token(LICENSE_KEY)['token']
    player_lookup = master_player_lookup(token).loc[master_player_lookup(token)['espn_id'].notnull()]

    rosters = site.get_league_rosters(player_lookup, LEAGUE_ID)

    available_players = get_players(token, week=WEEK, **SCORING)
    ########################
    # What we need for WDIS:
    ########################
    # 1. List of our starters
    roster = rosters.query(f"team_id == {TEAM_ID}")
    current_starters = list(roster.loc[(rosters['start']) & (rosters['fantasymath_id'].notnull()), 'fantasymath_id'])
    # valid_roster = roster.loc[roster['fantasymath_id'].apply(lambda x: x in list(available_players['fantasymath_id']))]
    # 2. Opponent's starters and actual scores
    opp_id = schedule_long(schedule).query(f'team_id == {TEAM_ID} & week == {WEEK}')['opp_id'].values[0]
    opp_roster = rosters.query(f"team_id == {opp_id}")
    opp_starters = opp_roster.loc[opp_roster['start'] & opp_roster['fantasymath_id'].notnull(), ['fantasymath_id', 'actual']]
    # valid_opp_starters = opp_starters.loc[opp_starters['fantasymath_id'].apply(lambda x: x in list(available_players['fantasymath_id']))]
    # 3. Sims    
    players_to_sim = pd.concat([roster[['fantasymath_id', 'actual']], opp_starters], ignore_index=True)
    sims = get_sims(token, players_to_sim['fantasymath_id'], week=WEEK, nsims=1000, **SCORING)
    played_players = players_to_sim.loc[players_to_sim['actual'].notnull()]
    for player, pts in zip(played_players['fantasymath_id'], played_players['actual']):
        sims[player] = pts
    invalid_players = list(set(players_to_sim['fantasymath_id']) - set(available_players['fantasymath_id']))
    for player in invalid_players:
        sims[player] = 0

    ########################
    # Analysis - Call wdis_by_pos over all positions
    ########################
    positions = get_positions(roster)
    df = pd.concat([wdis_by_position(pos, sims, roster, opp_starters['fantasymath_id']) for pos in positions])
   
    rec_starters = [df.xs(pos).idxmax()['wp'] for pos in positions]
    # ----------------------------------------------------------------------------------------------------------
    # Adding my own additional analysis for fun
    # ----------------------------------------------------------------------------------------------------------
    roster_stats = pd.concat([roster.set_index('fantasymath_id'),sims[roster['fantasymath_id']].describe().transpose()], axis=1).sort_values(by=['player_position', 'mean'], ascending=False)
    roster_stats.index.name = 'player'
    roster_stats = roster_stats[['mean', 'std']]
    roster_stats.columns = ['pts_mean', 'pts_std']
    ## Merge the summary stats into df
    df = df.join(roster_stats)

    ########################
    # Write output to a file
    ########################
    league_wk_output_dir = path.join(OUTPUT_PATH, f'espn_{LEAGUE_ID}_{LEAGUE_NAME}_2021_{str(WEEK).zfill(2)}')

    Path(league_wk_output_dir).mkdir(exist_ok=True)

    wdis_output_file = path.join(league_wk_output_dir, 'wdis.txt')

    with open(wdis_output_file, 'w') as f:
        print(f"WDIS Analysis, {LEAGUE_NAME}, Week {WEEK}", file=f)
        print("", file=f)
        print(f"Run at {dt.datetime.now()}", file=f)
        print("", file=f)
        print("Recommended Starters:", file=f)
        for starter, pos in zip(rec_starters, positions):
            print(f"{pos}: {starter}", file=f)
        print("", file=f)
        print("Detailed Projections and Win Probability", file=f)
        print(df[['mean', 'wp', 'wrong', 'regret', 'pts_mean', 'pts_std']], file=f)
        print("", file=f)
        if set(current_starters) == set(rec_starters):
            print("Current starters maximize probability of winning.", file=f)
        else:
            print("Not maximizing probability of winning.", file=f)
            print("", file=f)
            print("Start:", file=f)
            print([f"{player}: {roster_stats.loc[player]['pts_mean']}" for player in list(set(rec_starters) - set(current_starters))], file=f)
            print("Instead of:", file=f)
            print([f"{player}: {roster_stats.loc[player]['pts_mean']}" for player in list(set(current_starters) - set(rec_starters))], file=f)