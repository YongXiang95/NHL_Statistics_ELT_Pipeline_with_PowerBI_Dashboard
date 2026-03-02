# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## This notebook contains all the constants defined for the project

# MARKDOWN ********************

# ### Expected list of files in historical landing folder

# CELL ********************

EXPECTED_FILES_LIST = [
    'game.csv',
    'game_goalie_stats.csv',
    'game_goals.csv',
    'game_officials.csv',
    'game_penalties.csv',
    'game_plays.csv',
    'game_plays_players.csv',
    'game_scratches.csv',
    'game_shifts.csv',
    'game_skater_stats.csv',
    'game_teams_stats.csv',
    'player_info.csv',
    'team_info.csv'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# folder paths
LANDING_FOLDER = 'Files/historical/landing'
QUARANTINE_FOLDER = 'Files/historical/quarantine'
PROCESSED_FOLDER = 'Files/historical/processed'
LOG_FOLDER = 'Files/bronze_logs'

# display text colors
RED     = "\033[91m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
BLUE    = "\033[94m"
RESET   = "\033[0m"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Bronze spark schemas

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType 

# store the expected data structure for each header in a constant
TABLE_STRUCTURE = {
    'team_info': 
        { 
            'team_id': IntegerType(),
            'franchiseId': IntegerType(),
            'shortName': StringType(),
            'teamName': StringType(), 
            'abbreviation': StringType(), 
            'link': StringType() 
        }, 
    'game_skater_stats': 
        { 
            'game_id': IntegerType(), 
            'player_id': IntegerType(), 
            'team_id': IntegerType(), 
            'timeOnIce': IntegerType(), 
            'assists': IntegerType(), 
            'goals': IntegerType(), 
            'shots': IntegerType(), 
            'hits': IntegerType(), 
            'powerPlayGoals': IntegerType(), 
            'powerPlayAssists': IntegerType(), 
            'penaltyMinutes': IntegerType(), 
            'faceOffWins': IntegerType(), 
            'faceoffTaken': IntegerType(), 
            'takeaways': IntegerType(), 
            'giveaways': IntegerType(), 
            'shortHandedGoals': IntegerType(), 
            'shortHandedAssists': IntegerType(), 
            'blocked': IntegerType(), 
            'plusMinus': IntegerType(), 
            'evenTimeOnIce': IntegerType(), 
            'shortHandedTimeOnIce': IntegerType(), 
            'powerPlayTimeOnIce':IntegerType() 
        },
    'game_goalie_stats': 
        { 
            'game_id': IntegerType(), 
            'player_id': IntegerType(), 
            'team_id': IntegerType(), 
            'timeOnIce': IntegerType(), 
            'assists': IntegerType(), 
            'goals': IntegerType(), 
            'pim': IntegerType(), 
            'shots': IntegerType(), 
            'saves': IntegerType(), 
            'powerPlaySaves': IntegerType(), 
            'shortHandedSaves': IntegerType(), 
            'evenSaves': IntegerType(), 
            'shortHandedShotsAgainst': IntegerType(), 
            'evenShotsAgainst': IntegerType(), 
            'powerPlayShotsAgainst': IntegerType(), 
            'decision': StringType(), 
            'savePercentage': FloatType(), 
            'powerPlaySavePercentage': FloatType(), 
            'evenStrengthSavePercentage': FloatType() 
        }, 
    'game': 
        { 
            'game_id':StringType(), 
            'season':StringType(), 
            'type': StringType(), 
            'date_time_GMT':TimestampType(), 
            'away_team_id':IntegerType(), 
            'home_team_id':IntegerType(), 
            'away_goals':IntegerType(), 
            'home_goals':IntegerType(), 
            'outcome':StringType(), 
            'home_rink_side_start':StringType(), 
            'venue':StringType(), 
            'venue_link':StringType(), 
            'venue_time_zone_id':StringType(), 
            'venue_time_zone_offset':IntegerType(), 
            'venue_time_zone_tz':StringType() 
        }, 
    'game_plays': 
        { 
            'play_id': StringType(), 
            'game_id': StringType(), 
            'team_id_for': IntegerType(), 
            'team_id_against': IntegerType(), 
            'event': StringType(), 
            'secondaryType': StringType(), 
            'x': FloatType(), 
            'y': FloatType(), 
            'period': IntegerType(), 
            'periodType': StringType(), 
            'periodTime': IntegerType(), 
            'periodTimeRemaining': FloatType(), 
            'dateTime': TimestampType(), 
            'goals_away': IntegerType(), 
            'goals_home': IntegerType(), 
            'description': StringType(), 
            'st_x': FloatType(), 
            'st_y': FloatType() 
        }, 
    'game_plays_players': 
        { 
            'play_id': StringType(), 
            'game_id': StringType(), 
            'player_id': IntegerType(), 
                'playerType': StringType(), 
        }, 
    'game_shifts': 
        { 
            'game_id': IntegerType(), 
            'player_id': IntegerType(), 
            'period': IntegerType(), 
            'shift_start': IntegerType(), 
            'shift_end': IntegerType() 
        }, 
    'game_teams_stats': 
        { 
            'game_id': IntegerType(), 
            'team_id': IntegerType(), 
            'HoA': StringType(), 
            'won': StringType(), 
            'settled_in': StringType(), 
            'head_coach': StringType(), 
            'goals': IntegerType(), 
            'shots': IntegerType(), 
            'hits': IntegerType(), 
            'pim': IntegerType(), 
            'powerPlayOpportunities': IntegerType(), 
            'powerPlayGoals': IntegerType(), 
            'faceOffWinPercentage': FloatType(), 
            'giveaways': IntegerType(), 
            'takeaways': IntegerType(), 
            'blocked': IntegerType(), 
            'startRinkSide': StringType() 
        }, 
    'player_info': 
        { 
            'player_id': IntegerType(), 
            'firstName': StringType(), 
            'lastName': StringType(), 
            'nationality': StringType(), 
            'birthCity': StringType(), 
            'primaryPosition': StringType(), 
            'birthDate': TimestampType(), 
            'birthStateProvince': StringType(), 
            'height': StringType(), 
            'height_cm': FloatType(), 
            'weight': IntegerType(), 
            'shootsCatches': StringType() 
        }   
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
