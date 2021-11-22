import hosts.espn_taylor_final as site
import hosts.db as db
import datetime as dt
from textwrap import dedent
from pandas import DataFrame, Series
import sqlite3
from hosts.league_setup import LEAGUE_NAME
import wdis
import pandas as pd
from utilities import (LICENSE_KEY, generate_token, master_player_lookup,
                       get_sims, get_players, DB_PATH, OUTPUT_PATH)

LEAGUE_ID = 1011602
WEEK = 8 # As of 10/26

conn = sqlite3.connect(DB_PATH)

# Load teams, schedule, league and host info from DB
teams = db.read_league('teams', LEAGUE_ID, conn)
schedule = db.read_league('schedule', LEAGUE_ID, conn)
league = db.read_league('league', LEAGUE_ID, conn)
host = league['host'][0]

# Get Parameters from League - Team_Id and Scoring dict
TEAM_ID = league['team_id'][0] # Can also use .iloc[0]['team_id']
SCORING = {}
SCORING['qb'] = league.iloc[0]['qb_scoring']
SCORING['skill'] = league.iloc[0]['skill_scoring']
SCORING['dst'] = league.iloc[0]['dst_scoring']

#######################################################
# GET CURRENT ROSTERS
#######################################################

# Need to get FantasyMath players
token = generate_token(LICENSE_KEY)['token']
fantasy_math = master_player_lookup(token)[['fantasymath_id','position', 'espn_id']]
fantasy_math = fantasy_math.loc[fantasy_math['espn_id'].notnull()]

# Get Rosters
rosters = site.get_league_rosters(fantasy_math,LEAGUE_ID)

################################################################################################
################################################################################################

# The WDIS APP requires: 1) Our starters 2) Their starters 3) WDIS Options and 4) Raw Simulations
## 1 -- Get our roster and list of our current starters
roster = rosters.query(f"team_id == {TEAM_ID}")
current_starters = list(roster.loc[roster['start'] == True & roster['fantasymath_id'].notnull(), 'fantasymath_id'])

## 2 -- Get our opponent's starters
### Need to find our opponent's ID from the schedule and then their starters and their scores

### Convert schedule from wide to long format - It orginally had 1 row for each matchup (now we have 2 per)
def schedule_long(sched):
    sched1 = sched.rename(columns={'team1_id': 'team_id', 'team2_id': 'opp_id'})
    sched2 = sched.rename(columns={'team2_id': 'team_id', 'team1_id': 'opp_id'})
    return pd.concat([sched1, sched2], ignore_index=True)

schedule_team = schedule_long(schedule)

opponent_id = schedule_team.query(f"team_id == {TEAM_ID} & week == {WEEK}")['opp_id'].values[0]
opponent_starters = rosters.loc[(rosters['team_id'] == opponent_id) & (rosters['start'] == True) & (rosters['fantasymath_id'].notnull()), ['fantasymath_id', 'actual']]

### Validate opponent starters
### Ensure we are using valid players by using the get_players function
available_players = get_players(token, season=2021, week=WEEK, **SCORING)
unavailable_players = list(set(opponent_starters['fantasymath_id']) - set(available_players['fantasymath_id']))
valid_opp_starters = set(opponent_starters['fantasymath_id']) - set(unavailable_players)

## 3 -- Get raw sims for ALL our players and our opp's starters
players_to_sim = pd.concat([roster[['fantasymath_id', 'actual']], opponent_starters])


#### Use the set function to ensure that the player ids are found in the available players
sims = get_sims(token, set(players_to_sim['fantasymath_id']) & set(available_players['fantasymath_id']), season=2021, week=WEEK, nsims=1000, **SCORING)

## 4 -- Overwrite sims for players that have already played with their actual points
played_players = players_to_sim.loc[players_to_sim['actual'].notnull()]
for player, score in played_players.values:
    sims[player] = score

## 5 -- The last thing the WDIS function needs is our wdis bench options (current starter + bench candidates)
wdis_options = ['tim-patrick', 'robby-anderson','rashod-bateman']

## Run the simulation
wdis.calculate(sims, current_starters, valid_opp_starters, wdis_options)

################################################################################################

# It is still annoying to have to manually manipulate the WDIS options
# lets write some code that takes our roster, a *position* and calculates probability of winning with starter + eligible bench players
pos = 'WR1'
pos_in_WR1 = roster['player_position'].astype(str).apply(lambda x: x in pos)
roster.loc[pos_in_WR1]
bench_wr1_elig = ((roster['player_position']
                   .astype(str)
                   .apply(lambda x: x in pos) & ~roster['start']) |
                  (roster['team_position'] == pos))
roster.loc[bench_wr1_elig]
wdis_ids = list(roster.loc[bench_wr1_elig, 'fantasymath_id'])

def wdis_options_by_pos(roster, team_pos):
    is_wdis_elig = (roster['player_position'].astype(str).apply(lambda x: x in team_pos) & ~roster['start']) | (roster['team_position'] == team_pos)
    return list(roster.loc[is_wdis_elig, 'fantasymath_id'])

wdis_options_flex = wdis_options_by_pos(roster, 'RB/WR/TE')

df_flex = wdis.calculate(sims, current_starters, valid_opp_starters, wdis_options_flex)

## Put this all in a function
def wdis_by_pos1(pos, sims, roster, opp_starters):
    wdis_options = wdis_options_by_pos(roster, pos)

    starters = list(roster.loc[roster['start'] & roster['fantasymath_id'].notnull(), 'fantasymath_id'])

    return wdis.calculate(sims, starters, opp_starters, set(wdis_options) & set(sims.columns))

wdis_by_pos1('QB', sims, roster, valid_opp_starters)
wdis_by_pos1('RB/WR/TE', sims, roster, valid_opp_starters)

## Get a list of all positions
def positions_from_roster(roster):
    return list(roster.loc[roster['start'] & roster['fantasymath_id'].notnull(), 'team_position'])

positions = positions_from_roster(roster)

for pos in positions:
    print(wdis_by_pos1(pos, sims, roster, valid_opp_starters))

# Revamp the WDIS by Pos function
def wdis_by_pos2(pos, sims, roster, opp_starters):
    wdis_options = wdis_options_by_pos(roster, pos)

    starters = list(roster.loc[roster['start'] & roster['fantasymath_id'].notnull(), 'fantasymath_id'])

    df = wdis.calculate(sims, starters, opp_starters, set(wdis_options) & set(sims.columns))

    rec_start_id = df['wp'].idxmax()

    df['pos'] = pos
    df.index.name = 'player'
    df.reset_index(inplace=True)
    df.set_index(['pos', 'player'], inplace=True)

    return df

wdis_by_pos2('QB', sims, roster, valid_opp_starters)

df_start = pd.concat([wdis_by_pos2(pos, sims, roster, valid_opp_starters) for pos in positions])

## Able to take a subset of a multi-indexed dataframe
df_start.xs('WR1')
df_start.xs('WR1')['wp'].idxmax()

## Create a list of recommended starters by position
rec_starters = [df_start.xs(pos)['wp'].idxmax() for pos in positions]

## Able to use 'zipping' to match up two similar lists
for pos, starter in zip(positions, rec_starters):
    print(f"at {pos}, start {starter}")

##################################################################################################################################
##################################################################################################################################

# Create a directory to output analysis to
f'.output/espn_{LEAGUE_ID}_{LEAGUE_NAME}_2021_{str(WEEK).zfill(2)}/wdis.txt'

# Create the directory if it does not exist yet
from pathlib import Path
from os import path

league_wk_output_dir = path.join(OUTPUT_PATH, f'espn_{LEAGUE_ID}_{LEAGUE_NAME}_2021_{str(WEEK).zfill(2)}')

Path(league_wk_output_dir).mkdir(exist_ok=True, parents=True)

# Print our results to a file and save it to the directory
with open(path.join(league_wk_output_dir, 'wdis.txt'), 'w') as f:
    print(f"WDIS Analysis, {LEAGUE_NAME}, Week {WEEK}", file=f)
    print("", file=f)
    print(f"Run at {dt.datetime.now()}", file=f)
    print("", file=f)
    print("Recommended Starters:", file=f)
    for starter, pos in zip(rec_starters, positions):
        print(f"{pos}: {starter}", file=f)
    print("", file=f)
    print("Detailed Projections and Win Probability", file=f)
    print(df_start[['mean', 'wp', 'wrong', 'regret']], file=f)
    print("", file=f)
    if set(current_starters) == set(rec_starters):
        print("Current starters maximize probability of winning.", file=f)
    else:
        print("Not maximizing probability of winning.", file=f)
        print("", file=f)
        print("Start:", file=f)
        print(set(rec_starters) - set(current_starters), file=f)
        print("Instead of:", file=f)
        print(set(current_starters) - set(rec_starters), file=f)