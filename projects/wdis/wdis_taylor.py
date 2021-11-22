import pandas as pd
from os import path
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from pandas.core.tools.datetimes import should_cache
import seaborn as sns
import matplotlib.pyplot as plt
from seaborn.distributions import kdeplot
from seaborn.utils import percentiles
from utilities import generate_token, get_sims, get_players, LICENSE_KEY, WDIS_PATH

# Generate access token
token = generate_token(LICENSE_KEY)['token']

# Parameters
WEEK = 1
SEASON = 2019
NSIMS = 1000
SCORING = {'qb': 'pass6', 'skill': 'ppr', 'dst': 'high'}

team1 = ['drew-brees', 'alvin-kamara', 'sony-michel', 'julio-jones',
        'keenan-allen', 'jared-cook', 'matt-prater', 'lar-dst']

team2 = ['russell-wilson', 'christian-mccaffrey', 'saquon-barkley',
        'corey-davis', 'dante-pettis', 'greg-olsen', 'matt-gay', 
        'buf-dst']

bench = ['lesean-mccoy', 'phillip-lindsay', 'royce-freeman']

# Get a list of all valid players
valid_players = get_players(token, season=SEASON, week=WEEK, **SCORING)

# Obtain simulations for the players in team1, team2, and bench
players = team1 + team2 + bench
sims = get_sims(token, players, season=SEASON, week=WEEK, nsims=NSIMS, **SCORING)

sims.head()

# Coding the WDIS Calculator
sims[team1].head()

sims[team1].sum(axis=1)
sims[team2].sum(axis=1)

team1_wins = sims[team1].sum(axis=1) > sims[team2].sum(axis=1)
team1_wins.mean() # Probability of winning w current lineup

# First attempt at WDIS
def simple_wdis(sims, team1, team2, wdis):
    team1_wdis = team1 + [wdis]
    return (sims[team1_wdis].sum(axis=1) > sims[team2].sum(axis=1)).mean()

team1_no_wdis = ['drew-brees', 'alvin-kamara', 'julio-jones', 'keenan-allen',
                 'jared-cook', 'matt-prater', 'lar-dst'] # NO RB2
wdis = ['sony-michel', 'lesean-mccoy', 'phillip-lindsay', 'royce-freeman']

for player in wdis:
    print(player)
    print(simple_wdis(sims, team1_no_wdis, team2, player))

# Improving WDIS - pass list of WDIS candidates and get all probs at once
def simple_wdis2(sims, team1, team2, wdis):
    return {
        player: (sims[team1 + [player]].sum(axis=1) > sims[team2].sum(axis=1)).mean()
        for player in wdis
    }

simple_wdis2(sims, team1_no_wdis, team2, wdis)

# Improving WDIS again - Take a FULL team1, a list of wdis players, and automatically figures out who you're asking about
def simple_wdis3(sims, team1, team2, wdis):
    # There should be one player who overlaps in WDIS and Team1
    team1_no_wdis = [player for player in team1 if player not in wdis]
    
    return {
        player: (sims[team1_no_wdis + [player]].sum(axis=1) > sims[team2].sum(axis=1)).mean()
        for player in wdis
    }

simple_wdis3(sims, team1, team2, wdis)

# Improving WDIS again - Include checks and sorting: 1) Only one starter overlaps with WDIS 2) WDIS contains at least one player
def simple_wdis4(sims, team1, team2, wdis):
    # There should be one player who overlaps in WDIS and Team1
    team1_no_wdis = [player for player in team1 if player not in wdis]
    # ALTERNATIVE WAY: Use set() functions
    # team1_no_wdis = set(team1) - set(wdis)

    # Checks
    current_starter = [player for player in team1 if player in wdis]
    assert len(current_starter) == 1

    bench_options = [player for player in wdis if player not in team1]
    assert len(bench_options) >= 1

    return Series({
        player: (sims[team1_no_wdis + [player]].sum(axis=1) > 
        sims[team2].sum(axis=1)).mean() 
        for player in wdis}).sort_values(ascending=False)

simple_wdis4(sims, team1, team2, wdis)

############################################################################################################################### 
# Here's where we landed
team1 = ['drew-brees', 'alvin-kamara', 'sony-michel', 'julio-jones',
        'keenan-allen', 'jared-cook', 'matt-prater', 'lar-dst']

team2 = ['russell-wilson', 'christian-mccaffrey', 'saquon-barkley',
        'corey-davis', 'dante-pettis', 'greg-olsen', 'matt-gay', 
        'buf-dst']

current_starter = 'sony-michel'
bench_options = ['lesean-mccoy', 'phillip-lindsay', 'royce-freeman']
wdis = ['sony-michel', 'lesean-mccoy', 'phillip-lindsay', 'royce-freeman']
team_sans_starter = list(set(team1) - set([current_starter]))

# Overall Score for Team - with Sony Michel
sims[team1].sum(axis=1).describe()
# Would like to have these summary stats side by side for each player
## This gets the summary stats for each player in list form
[sims[team_sans_starter + [player]].sum(axis=1).describe() for player in wdis]
## Use pd.concat to put them side by side
stats = pd.concat([sims[team_sans_starter + [player]].sum(axis=1).describe() for player in wdis],axis=1)
stats.columns = wdis
stats
## Use transpose to flip so players are rows and columns are the summary stats - Hint: can use .T
## Also drop the 'count', 'min', and 'max' columns
stats.T.drop(['count', 'min', 'max'],axis=1)

# Calculate probability of a bench player outscoring current starter
# 1) Get highest bench score each week (sim)
sims[bench_options].max(axis=1)
# 2) See how often that best bench score beats the starter
(sims[bench_options].max(axis=1) > sims[current_starter]).mean()

# Calculate how often starting 'current starter' over best bench player will cause us to lose
team1_w_starter = sims[team_sans_starter].sum(axis=1) + sims[current_starter]
team1_w_best_backup = (sims[team_sans_starter].sum(axis=1) + sims[bench_options].max(axis=1))

team2_total = sims[team2].sum(axis=1)
# The calculation is: How often do we win with our best backup AND lose with our starter?
regret_col = ((team1_w_starter < team2_total) & (team1_w_best_backup > team2_total))

# Create functions to see these probabilities assuming we start other bench players
def sumstats(starter):
    team_w_starter = sims[team_sans_starter].sum(axis=1) + sims[starter]
    stats_series = team_w_starter.describe(percentiles=[.05,.25,.5,.75,.95]).drop(['count','min','max'])
    stats_series.name = starter
    return stats_series

sumstats('sony-michel')

def win_prob(starter):
    team_w_starter = sims[team_sans_starter].sum(axis=1) + sims[starter]
    team2_total = sims[team2].sum(axis=1)
    return (team_w_starter > team2_total).mean()

win_prob('sony-michel')

def wrong_prob(starter, bench):
    return (sims[bench].max(axis=1) > sims[starter]).mean()

wrong_prob('sony-michel',['lesean-mccoy', 'phillip-lindsay', 'royce-freeman'])

def regret_prob(starter, bench):
    team_w_starter = sims[team_sans_starter].sum(axis=1) + sims[starter]
    team_w_best_backup = sims[team_sans_starter].sum(axis=1) + sims[bench].max(axis=1)
    team2_total = sims[team2].sum(axis=1)
    return ((team_w_starter < team2_total) & (team_w_best_backup > team2_total)).mean() 

regret_prob('sony-michel',['lesean-mccoy', 'phillip-lindsay', 'royce-freeman'])

# Create a function that given a list of WDIS candidates goes through and automatically makes all the permutations of starter and bench
def starter_bench_scenarios(wdis):
    return [{
            'starter': player,
            'bench': [p for p in wdis if p != player]
            } for player in wdis]

scenarios = starter_bench_scenarios(wdis)

# Create a df of summary stats for all WDIS players
df = pd.concat([sumstats(player) for player in wdis],axis=1).T
# Add columns for the probabilities we'be previously calculated (ex: win prob)
## Add win probability
wps = [win_prob(player) for player in wdis]
df['win_prob'] = wps

## Add wrong probability - let's do all of the scenarios at once
wrg = [wrong_prob(scen['starter'], scen['bench']) for scen in scenarios]
df['wrong'] = wrg

## Add regret probability - use the ** trick to fill in parameters
reg = [regret_prob(**scen) for scen in scenarios]
df['regret'] = reg

## Final Output
df

########################################################################
# Create an upgraded version of WDIS
def wdis_plus(sims, team1, team2, wdis):

    # Validity Checks
    ## Only 1 starter in WDIS
    current_starter = set(team1) & set(wdis)
    assert len(current_starter) == 1
    ## At least 1 other bench option
    bench_options = set(wdis) - set(team1)
    assert len(bench_options) >= 1

    team_sans_starter = set(team1) - set(current_starter)

    scenarios = starter_bench_scenarios(wdis)
    team2_total = sims[team2].sum(axis=1)

    def sumstats(starter):
        team_w_starter = sims[team_sans_starter].sum(axis=1) + sims[starter]
        team_info = (team_w_starter.describe(percentiles=[.05, .25, .5, .75, .95]).drop(['count', 'min', 'max']))
        return team_info

    def win_prob(starter):
        team_w_starter = sims[team_sans_starter].sum(axis=1) + sims[starter]
        return (team_w_starter > team2_total).mean()

    def prob_wrong(starter, bench):
        return (sims[bench].max(axis=1) > sims[starter]).mean()
    
    def regret_prob(starter, bench):
        team_w_starter = sims[team_sans_starter].sum(axis=1) + sims[starter]
        team_w_best_backup = sims[team_sans_starter].sum(axis=1) + sims[bench].max(axis=1)
        return ((team_w_starter < team2_total) & (team_w_best_backup > team2_total)).mean()
    
    # Start with a DF of summary stats
    df = pd.concat([sumstats(player) for player in wdis], axis=1)
    df.columns = wdis
    df = df.T
    
    # Add win_prob, wrong, and regret to DF
    df['win_prob'] = [win_prob(player) for player in wdis]
    df['wrong'] = [prob_wrong(**scen) for scen in scenarios]
    df['regret'] = [regret_prob(**scen) for scen in scenarios]

    return df.sort_values('win_prob', ascending=False)

wdis_plus(sims, team1, team2, wdis)

########################################################################
# Run all available (hypothetical) kickers through the model
fa_kickers = ['aldrick-rosas', 'austin-seibert', 'cairo-santos',
              'zane-gonzalez', 'chris-boswell', 'kaare-vedvik',
              'eddy-pineiro', 'daniel-carlson', 'dustin-hopkins']
# Get sims for free agent kickers and add them to sims
k_sims = get_sims(token,fa_kickers, week=WEEK, season=SEASON, nsims=1000, **SCORING)
sims_plus = pd.concat([sims, k_sims], axis=1)
# Create a WDIS list of kickers
wdis_k = ['matt-prater'] + fa_kickers
# Save results in a df
df_k = wdis_plus(sims_plus, team1, team2, wdis_k)

########################################################################
# PLOTTING
########################################################################
# Start by summing up totals
points_wide = pd.concat([sims[team1].sum(axis=1),sims[team2].sum(axis=1)],axis=1)
points_wide.columns = ['team1', 'team2']
points_wide.head()

# Convert the 'wide data' to 'long'
## Key Sequence: Stack -> Reset Index -> Rename Columns
points_long = points_wide.stack().reset_index()
points_long.columns = ['sim', 'team', 'points']
points_long.head()

# Create Density plot
g = sns.FacetGrid(points_long, hue='team', aspect=4)
g = g.map(sns.kdeplot, 'points', shade=True)
g.add_legend()
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle('Total Team Fantasy Points Distributions')
g.fig.savefig(path.join(WDIS_PATH, 'wdis_dist_by_team1.png'),
              bbox_inches='tight', dpi=500)

# Create this plot for ALL WDIS options at once
## Create a df of total team points for each player and add in opponent total
points_wide = pd.concat([sims[team_sans_starter].sum(axis=1) + sims[player] for player in wdis], axis=1)
points_wide.columns = wdis
points_wide['opp'] = sims[team2].sum(axis=1)
## Convert this to long format to then plot
points_long = points_wide.stack().reset_index()
points_long.columns = ['sim', 'team', 'points']

g = sns.FacetGrid(points_long, hue='team', aspect=4)
g = g.map(sns.kdeplot, 'points', shade=True)
g.add_legend()
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle('Total Team Fantasy Points Distributions - WDIS Options')
g.fig.savefig(path.join(WDIS_PATH, 'wdis_dist_by_team2.png'),
              bbox_inches='tight', dpi=500)

# Create a function to plot
def wdis_plots(sims, team1, team2, wdis):

    # Validity Checks
    ## Check for only one starter
    current_starter = set(team1) & set(wdis)
    assert len(current_starter) == 1

    ## Check at least one bench option
    bench_options = set(wdis) - set(team1)
    assert len(bench_options) >= 1

    team_sans_starter = list(set(team1) - set(current_starter))

    # Get team total for all wdis options and opponent
    points_wide = pd.concat([sims[team_sans_starter].sum(axis=1) + sims[player] for player in wdis], axis=1)
    points_wide.columns = wdis
    points_wide['opp'] = sims[team2].sum(axis=1)

    points_long = points_wide.stack().reset_index()
    points_long.columns = ['sim', 'team', 'points']
    
    # Create dist plots
    g = sns.FacetGrid(points_long, hue='team', aspect=4)
    g = g.map(sns.kdeplot, 'points', shade=True)
    g.add_legend()
    g.fig.subplots_adjust(top=0.9)
    g.fig.suptitle('Total Team Fantasy Points Distributions - WDIS Options')

    return g

# Since the plots of individual players are so close, show them individually
pw = sims[wdis].stack().reset_index()
pw.columns = ['sim', 'player', 'points']

g = sns.FacetGrid(pw, hue='player', aspect=2)
g = g.map(sns.kdeplot, 'points', shade=True)
g.add_legend()
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle(f'WDIS Projections')
g.fig.savefig(path.join(WDIS_PATH, f'player_wdis_dist_{WEEK}.png'),
              bbox_inches='tight', dpi=500)