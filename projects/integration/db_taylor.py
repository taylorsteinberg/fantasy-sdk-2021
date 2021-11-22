import sqlite3
from pandas import DataFrame, Series
import pandas as pd
import hosts.espn_taylor_final as site
from utilities import DB_PATH
from textwrap import dedent

LEAGUE_ID = 1011602
TEAM_ID = 10

# Two tables won't be changing and we can write them to our fantasy database: teams and schedules tables
teams = site.get_teams_in_league(LEAGUE_ID)
schedule = site.get_league_schedule(LEAGUE_ID, 2021)

# Connect to the SQL database
conn = sqlite3.connect(DB_PATH)

# Write teams table to SQL
teams.to_sql('teams', conn, index=False, if_exists='replace') # if_exits='replace' overwrites all data - 'append' adds on

conn.execute(dedent(f"""
    DELETE FROM teams
    WHERE league_id = {LEAGUE_ID};"""))

teams.to_sql('teams', conn, index=False, if_exists='append')

def clear_league_from_table1(league_id, table, conn):
    conn.execute(dedent(f"""
    DELETE FROM {table}
    WHERE league_id = {league_id};"""))

clear_league_from_table1(LEAGUE_ID, 'schedule', conn) # This will throw an: OperationalError: no such table: schedule
clear_league_from_table1(LEAGUE_ID, 'teams', conn)

# Create a function that first checks if a table exists (to avoid the above error) and then deletes it
def clear_league_from_table2(league_id, table, conn):
    # get list of tables in db
    tables_in_db = [x[0] for x in list(conn.execute("SELECT name FROM sqlite_master WHERE type='table';"))]

    # check if table is in list (don't have to delete those that aren't)
    if table in tables_in_db:
        conn.execute(dedent(f"""
        DELETE FROM {table}
        WHERE league_id = {league_id};"""))
clear_league_from_table2(LEAGUE_ID, 'teams', conn)
schedule.to_sql('schedule', conn, index=False, if_exists='append')

# Simplify the clear->write process
def overwrite_league(df, name, conn, league_id):
    clear_league_from_table2(league_id, name, conn)
    df.to_sql(name, conn, index=False, if_exists='append')
overwrite_league(teams, 'teams', conn, LEAGUE_ID)

# Add a function to get the data OUT for a specific league
def read_league(name, league_id, conn):
    return pd.read_sql(dedent(f"""
    SELECT *
    FROM {name}
    WHERE league_id = {league_id};"""), conn)
read_league('teams', LEAGUE_ID, conn)
read_league('schedule', LEAGUE_ID, conn)

# We can also store some other static information about our league and save it in the db
league = DataFrame([{'league_id': LEAGUE_ID, 'team_id': TEAM_ID,
    'host': 'ESPN', 'name': 'Chris Leibowitz League of Champs',
    'qb_scoring': 'pass4', 'skill_scoring': 'ppr0', 'dst_scoring': 'mfl'}])

overwrite_league(league, 'league', conn, LEAGUE_ID)
read_league('league', LEAGUE_ID, conn)