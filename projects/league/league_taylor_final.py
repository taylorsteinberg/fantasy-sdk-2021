import hosts.espn_taylor_final as site
import projects.integration.auto_wdis_final_taylor as int
import sqlite3
import hosts.db as db
import pandas as pd
import seaborn as sns
from os import path
from pandas import DataFrame, Series
from pathlib import Path
from textwrap import dedent
from utilities import (get_sims, generate_token, LICENSE_KEY, DB_PATH,
                       OUTPUT_PATH, master_player_lookup, get_players,
                       schedule_long)

def lineup_by_team(team_id):
    return starting_rosters.query(f"team_id == {team_id}")['fantasymath_id']

def summarize_matchup(sims_a, sims_b):
    """
    Given two teams of sims (A and B), summarize a matchup with: win probability, over-under, betting line, etc.
    """
    # Start by getting team totals
    total_a = sims_a.sum(axis=1)
    total_b = sims_b.sum(axis=1)

    win_prob_a = (total_a > total_b).mean().round(2)
    win_prob_b = (1 - win_prob_a).round(2)

    over_under = round((total_a + total_b).median(),2)

    line = (total_a - total_b).median()
    line = round(line * 2) / 2

    return {'wp_a': win_prob_a, 'wp_b': win_prob_b, 'over_under': over_under, 'line': line}

def lock_of_week(matchups):
    wp_a = matchups[['team_a', 'wp_a', 'team_b']]
    wp_a.columns = ['team', 'wp', 'opp']
    wp_b = matchups[['team_b', 'wp_b', 'team_a']]
    wp_b.columns = ['team', 'wp', 'opp']
    stacked = pd.concat([wp_a, wp_b ], ignore_index=True)

    lock = stacked.sort_values('wp', ascending=False).iloc[0]
    return lock.to_dict()

def matchup_of_week(matchups):
    # Get the std dev of win probs, lowest will be the closest matchup
    wp_std = matchups[['wp_a', 'wp_b']].std(axis=1)
    # idxmin "index min" returns the index of the lowest value
    closest_matchup_id = wp_std.idxmin()
    return matchups.loc[closest_matchup_id].to_dict()

def summarize_team(sims):
    """
    Calculate summary stats on one set of teams
    """
    totals = sims.sum(axis=1)
    # Dropping count, min, and max since they are not useful
    stats = (totals.describe(percentiles=[.05, .25, .5, .75, .95])[['mean', 'std', '5%', '25%', '50%', '75%', '95%']].to_dict())
    return stats



if __name__ == '__main__':
    # PARAMETERS
    LEAGUE_ID = 1011602
    WEEK = 11

    # Retrieve items from database
    conn = sqlite3.connect(DB_PATH)

    TEAMS, SCHEDULE, LEAGUE = [db.read_league(table, LEAGUE_ID, conn) for table in ['teams', 'schedule', 'league']]
    TEAM_ID = LEAGUE['team_id'][0]
    HOST = LEAGUE['host'][0]
    LEAGUE_NAME = LEAGUE['name'][0]
    SCORING = {}
    SCORING['qb'] = LEAGUE['qb_scoring'][0]
    SCORING['skill'] = LEAGUE['skill_scoring'][0]
    SCORING['dst'] = LEAGUE['dst_scoring'][0]

    # Get Rosters
    token = generate_token(LICENSE_KEY)['token']
    FM_LOOKUP = site.master_player_lookup(token)

    rosters = site.get_league_rosters(FM_LOOKUP, LEAGUE_ID)
    starting_rosters = rosters.query('start')

    available_players = get_players(token, week=WEEK, **SCORING)
    sims = get_sims(token, (set(starting_rosters['fantasymath_id']) & set(available_players['fantasymath_id'])), week=WEEK, nsims=1000, **SCORING)
    ## Update sims for players who have already played this week
    played_players = starting_rosters.loc[(~starting_rosters['actual'].isnull()), ['fantasymath_id', 'actual']]
    for player, score in zip(played_players['fantasymath_id'], played_players['actual']):
        sims[player] = score

    schedule_this_week = SCHEDULE.query(f"week == {WEEK}")

    # Apply summarize_matchup() to every matchup in the data
    matchup_list = []
    for a, b in zip(schedule_this_week['team1_id'], schedule_this_week['team2_id']):
        
        # Gives us Series of starting lineups for each team in matchup
        lineup_a = lineup_by_team(a)
        lineup_b = lineup_by_team(b)

        # Use lineups to grab right sims, feed into summarize_matchup()
        working_matchup_dict = summarize_matchup(sims[lineup_a], sims[lineup_b])

        # Add some other info to working_matchup_dict
        working_matchup_dict['team_a'] = a
        working_matchup_dict['team_b'] = b

        # Add working dict to list of matchups, then loop to next matchup
        matchup_list.append(working_matchup_dict)

    matchup_df = DataFrame(matchup_list)
    
    ## Add owner names to matchup_df
    team_to_owner = {team: owner for team, owner in zip(TEAMS['team_id'], TEAMS['owner_name'])}
    matchup_df[['team_a', 'team_b']] = (matchup_df[['team_a', 'team_b']].replace(team_to_owner))

    # Apply summarize_team() to each team
    team_list = []
    for team_id in TEAMS['team_id']:
        team_lineup = lineup_by_team(team_id)
        team_sims = sims[team_lineup]
        working_team_dict = summarize_team(team_sims)
        working_team_dict['team_id'] = team_id
        team_list.append(working_team_dict)

    team_df = DataFrame(team_list).set_index('team_id')

    # Calculate weekly high and low
    totals_by_team = pd.concat([sims[lineup_by_team(team)].sum(axis=1).to_frame(team) for team in TEAMS['team_id']], axis=1)

    team_df['p_high'] = (totals_by_team.idxmax(axis=1).value_counts(normalize=True))
    team_df['p_low'] = (totals_by_team.idxmin(axis=1).value_counts(normalize=True))

    # Average high and low values
    high_score = totals_by_team.max(axis=1)
    low_score = totals_by_team.min(axis=1)
    avg_weekly_high_low = pd.concat([high_score.describe(percentiles=[.05,.25,.5,.75,.95]),low_score.describe(percentiles=[.05,.25,.5,.75,.95])], axis=1)
    avg_weekly_high_low.columns = [f'Wk{WEEK}_High', f'Wk{WEEK}_Low']
    avg_weekly_high_low.drop(['count'], inplace=True)

    # Add team names to finalize 'team_df'
    team_df = pd.merge(team_df,TEAMS[['team_id', 'owner_name']], right_on='team_id', left_index=True).set_index('owner_name').drop('team_id',axis=1)
    team_df.sort_values('mean', ascending=False, inplace=True)
    team_df[['p_high','p_low']] = (team_df[['p_high','p_low']] * 100).round(2).astype(str)+'%'
    team_df = team_df.round(2)

    # WRITING TO FILE
    league_wk_dir = path.join(OUTPUT_PATH,f'{HOST}_{LEAGUE_ID}_{LEAGUE_NAME}_2021_{str(WEEK).zfill(2)}')
    Path(league_wk_dir).mkdir(exist_ok=True, parents=True)
    output_file = path.join(league_wk_dir, 'league_analysis.txt')
    with open(output_file, 'w') as f:
        print(dedent(
            f"""
            **********************************
            Matchup Projections, Week {WEEK} - 2021
            **********************************
            """), file=f)
        print(matchup_df, file=f)

        print(dedent(
            f"""
            **********************************
            Team Projections, Week {WEEK} - 2021
            **********************************
            """), file=f)
        print(team_df, file=f)

        lock = lock_of_week(matchup_df)
        close = matchup_of_week(matchup_df)
        meh = matchup_df.sort_values('over_under').iloc[0]

        print(dedent(f"""
            Lock of the week:"""), file=f)
        print(f"{lock['team']} over {lock['opp']} - Win Prob: {(lock['wp'] * 100)}%", file=f)
        print(dedent("""
            Matchup of the week:"""), file=f)
        print(f"{close['team_a']} ({close['wp_a']}) vs {close['team_b']} ({close['wp_b']})", file=f)
        print(dedent("""
        Bust of the week:"""), file=f)
        print(f"{meh['team_a']} vs {meh['team_b']} - Over/Under set at: {meh['over_under']}", file=f)

    # PLOTTING
    ## Since we are using seaborn, convert the data to long form
    team_totals_long = totals_by_team.stack().reset_index()
    team_totals_long.columns = ['sim', 'team_id', 'pts']

    schedule_team = schedule_long(SCHEDULE).query(f'week == {WEEK}')
    team_tot_long_w_matchup = pd.merge(team_totals_long, schedule_team[['team_id', 'matchup_id']])

    schedule_this_week['desc'] = schedule_this_week['team1_id'].replace(team_to_owner) + ' v ' + schedule_this_week['team2_id'].replace(team_to_owner)
    team_tot_long_w_desc = pd.merge(team_tot_long_w_matchup,schedule_this_week[['matchup_id', 'desc']])

    g = sns.FacetGrid(team_tot_long_w_desc.replace(team_to_owner), hue='team_id', col='desc', col_wrap=2, aspect=2)
    g = g.map(sns.kdeplot, 'pts', shade=True)
    g.add_legend()
    g.fig.subplots_adjust(top=0.9)
    g.fig.suptitle(f'Team Points Distributions by Matchup - Week {WEEK}')
    g.fig.savefig(path.join(league_wk_dir,'team_dist_by_matchup.png'), bbox_inches='tight', dpi=500)