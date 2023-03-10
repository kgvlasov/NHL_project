import requests
import json
import pandas as pd
from datetime import datetime
from python import connections
from python import execute
api_addr = 'https://statsapi.web.nhl.com/'

def get_info(addition_addr):
    request = requests.get(api_addr+addition_addr)
    result = request.json()
    return result

def get_data():
    teams = pd.DataFrame.from_dict(get_info('api/v1/teams')['teams'])
    teams['id'] = teams['id'].astype(int)
    teams['firstYearOfPlay'] = teams['firstYearOfPlay'].astype(int)
    teams['active'] = teams['active'].astype(int)
    statsSingleSeason = pd.DataFrame()
    regularSeasonStatRankings = pd.DataFrame()

    for i in teams['link']:
        stats = pd.json_normalize(get_info(i+'/stats')['stats'])
        statsSingleSeason = pd.concat([statsSingleSeason,pd.json_normalize(stats[stats['type.displayName']=='statsSingleSeason']['splits'][0])])
        statsSingleSeason['displayName'] = stats['type.displayName'][0]
        statsSingleSeason['type.gameType.id'] = stats['type.gameType.id'][0]
        statsSingleSeason['type.gameType.description'] = stats['type.gameType.description'][0]
        statsSingleSeason['type.gameType.postseason'] = stats['type.gameType.postseason'][0]
        statsSingleSeason['load_dttm_utc'] = datetime.utcnow().replace(microsecond=0)
        regularSeasonStatRankings = pd.concat([regularSeasonStatRankings,pd.json_normalize(stats[stats['type.displayName'] == 'regularSeasonStatRankings']['splits'][1])])
        regularSeasonStatRankings['displayName'] = stats['type.displayName'][1]
        regularSeasonStatRankings['type.gameType'] = stats['type.gameType'][1]

        regularSeasonStatRankings = regularSeasonStatRankings.replace(['st','nd','rd','th'], '', regex=True)
        regularSeasonStatRankings['load_dttm_utc'] = datetime.utcnow().replace(microsecond=0)

    teams.columns = [x.replace('.','_') for x in teams.columns]
    statsSingleSeason.columns = [x.replace('.', '_') for x in statsSingleSeason.columns]
    regularSeasonStatRankings.columns = [x.replace('.', '_') for x in regularSeasonStatRankings.columns]
    statsSingleSeason['type_gameType_postseason'] = statsSingleSeason['type_gameType_postseason'].astype(int)

    statsSingleSeason= statsSingleSeason.astype({'stat_ptPctg': float,'stat_goalsPerGame': float,'stat_goalsAgainstPerGame': float,'stat_evGGARatio': float,'stat_powerPlayPercentage': float,'stat_powerPlayGoals': float,'stat_powerPlayGoalsAgainst': float,'stat_powerPlayOpportunities': float,'stat_penaltyKillPercentage': float,'stat_shotsPerGame': float,'stat_shotsAllowed': float,'stat_winScoreFirst': float,'stat_winOppScoreFirst': float,'stat_winLeadFirstPer': float,'stat_winLeadSecondPer': float,'stat_winOutshootOpp': float,'stat_winOutshotByOpp': float,'stat_faceOffsTaken': float,'stat_faceOffsWon': float,'stat_faceOffsLost': float,'stat_faceOffWinPercentage': float,'stat_shootingPctg': float,'stat_savePctg': float})
    regularSeasonStatRankings= regularSeasonStatRankings.astype({'stat_wins': int,'stat_losses': int,'stat_ot': int,'stat_pts': int,'stat_ptPctg': int,'stat_goalsPerGame': int,'stat_goalsAgainstPerGame': int,'stat_evGGARatio': int,'stat_powerPlayPercentage': int,'stat_powerPlayGoals': int,'stat_powerPlayGoalsAgainst': int,'stat_powerPlayOpportunities': int,'stat_penaltyKillOpportunities': int,'stat_penaltyKillPercentage': int,'stat_shotsPerGame': int,'stat_shotsAllowed': int,'stat_winScoreFirst': int,'stat_winOppScoreFirst': int,'stat_winLeadFirstPer': int,'stat_winLeadSecondPer': int,'stat_winOutshootOpp': int,'stat_winOutshotByOpp': int,'stat_faceOffsTaken': int,'stat_faceOffsWon': int,'stat_faceOffsLost': int,'stat_faceOffWinPercentage': int,'stat_savePctRank': int,'stat_shootingPctRank': int })

    print(statsSingleSeason.info())
    print(regularSeasonStatRankings.info())

    return teams,statsSingleSeason,regularSeasonStatRankings

def load_data():
    teams,season_stats,season_ranks = get_data()
    clickhouse_client = connections.get_clickhouse_client()
    clickhouse_client.execute("INSERT INTO nhl_stats.teams VALUES ",teams.to_dict('records'))
    clickhouse_client.execute("INSERT INTO nhl_stats.season_stats VALUES ",season_stats.to_dict('records'))
    clickhouse_client.execute("INSERT INTO nhl_stats.season_ranks VALUES ",season_ranks.to_dict('records'))
    print('Success')
