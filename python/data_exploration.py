#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

#%%
spark = SparkSession.builder.appName('nba_stats_spark').getOrCreate()

#%%
base_path = 'C:/Users/Kepler/Documents/nba_data/csv'
player_file = base_path + '/player.csv'

df_player = spark.read.csv(player_file, header=True, inferSchema=True)

# %%
df_player.show()
# %%
df_lebron = df_player.filter(df_player['full_name']=='LeBron James')

df_lebron.show()

# %%
base_path = 'C:/Users/Kepler/Documents/nba_data/csv'
game_file = base_path + '/game.csv'

df_game = spark.read.csv(game_file, header=True, inferSchema=True)
# %%
df_game.show()

# %%
df_lakers_game = df_game.filter(
    (col('team_abbreviation_home') == 'LAL') | (col('team_abbreviation_away') == 'LAL')
)
# %%
df_lakers_game.count()
# %%
