#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 11:36:09 2024

@author: pauladam
"""
# -*- coding: utf-8 -*-
"""
@author: Chad.Birger
"""

import pandas as pd
import datetime as dt
import requests
import time
import sqlite3
from urllib.error import HTTPError
from sqlalchemy import create_engine, text
import pyodbc


username = 'paultadam5_outlook.com#EXT#@paultadam5outlook.onmicrosoft.com'
password = 'CloudSAb948c5f2'
hostname = 'ncaa-hoops.database.windows.net'
database_name = 'ncaa-hoops'
driver= '{SQL Server}'


conn_str = f'DRIVER={driver};SERVER=tcp:{hostname};PORT=1433;DATABASE={database_name};UID={username};PWD={password}'
engine = create_engine(f"mssql+pyodbc://?odbc_connect={conn_str}",use_setinputsizes=False)
connection = engine.connect()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + dt.timedelta(n)

season_start = dt.date(2023, 11, 6)
today = dt.date.today()
game_id = []
for single_date in daterange(season_start, today):
    date_val = single_date.strftime("%Y/%m/%d")
    mm = "{:02d}".format(single_date.month)
    dd = "{:02d}".format(single_date.day)
    YYYY = single_date.year
    #URL = "https://www.ncaa.com/scoreboard/basketball-men/d1/" + str(date_val) + "/all-conf"
    URL = "https://stats.ncaa.org/season_divisions/18221/livestream_scoreboards?utf8=%E2%9C%93&season_division_id=&game_date="+ str(mm) + '%2F' + str(dd) + '%2F'+ str(YYYY) + '&conference_id=0&tournament_id=&commit=Submit'
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    page = requests.get(URL, headers=headers)
    games_today = []
    cur = 0
    today_schedule = page.text
    while cur >= 0:
        today_schedule = today_schedule[cur:]
        g = today_schedule.find('<tr id="contest_')
        if g < 0:
            break
        games_today.append(today_schedule[g+16: g+23])
        cur = g+24
    game_id.append(games_today)
### Remove duplicate game_id values
gameId = []    
for row in game_id:
    gameId.append(set(row))

game_ids = []
for row in gameId:
    game_ids.append(list(row))
    
#The following is an example to pull one-game worth of data: 
game = game_ids[69][4]
url = 'https://stats.ncaa.org/contests/' +str(game)+'/box_score' 
df_box_score = pd.read_html(url)
time.sleep(1)
headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
page = requests.get(url, headers=headers)
game_info = page.text
g = game_info.find('<a href="/game/play_by_play/')
if g < 0:
    print("not found")
game2 = game_info[g+28: g+35]

url2 = 'https://stats.ncaa.org/game/play_by_play/' +str(game2)
df_pxp = pd.read_html(url2)
time.sleep(1)
url3 =  'https://stats.ncaa.org/game/period_stats/' +str(game2)
df_period_stats = pd.read_html(url3)
time.sleep(1)


#=====================================================================
game_list=[]
game_list2= []
game_results_box_score = []
game_results_pxp = []
game_results_period_stats = []
no_games = []
for row in game_ids:
    for gameId in row:
        try:
            url =  'https://stats.ncaa.org/contests/' + str(gameId) + '/box_score'
            #url = 'ncaa.com/game/' + str(gameId)
            df_box_score = pd.read_html(url)
            time.sleep(3)
            headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
            page = requests.get(url, headers=headers)
            game_info = page.text
            g = game_info.find('<a href="/game/play_by_play/')
            if g < 0:
                print("not found")
            game2 = game_info[g+28: g+35]
            url2 = 'https://stats.ncaa.org/game/play_by_play/' +str(game2)
            df_pxp = pd.read_html(url2)
            time.sleep(3)
            url3 =  'https://stats.ncaa.org/game/period_stats/' +str(game2)
            df_period_stats = pd.read_html(url3)
            time.sleep(3)
            game_results_box_score.append(df_box_score)
            game_results_pxp.append(df_pxp)
            game_results_period_stats.append(df_period_stats)
            game_list.append(gameId)
        except ValueError:
            no_games.append(gameId)
            pass
#=====================================================================





#=====================================================================





#=====================================================================
game_list=[]
game_list2= []
game_results_box_score = []

game_results_pxp = []

game_results_period_stats = []

no_games = []

for row in game_ids:
    for gameId in row:
        print(gameId)
        try:
            url = 'https://stats.ncaa.org/contests/' + str(gameId) + '/box_score'
    
    #url = 'ncaa.com/game/' + str(gameId)
    
            df_box_score = pd.read_html(url)
            
            time.sleep(3)
            
            headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.37"}
            
            page = requests.get(url, headers=headers)
            
            game_info = page.text
            
            g = game_info.find('<a href="/game/play_by_play/')
            
            if g < 0:
            
                print("not found")
            
            game2 = game_info[g+28: g+35]
            
            url2 = 'https://stats.ncaa.org/game/play_by_play/' +str(game2)
                    
            df_pxp = pd.read_html(url2)
            
            time.sleep(3)
            
            url3 = 'https://stats.ncaa.org/game/period_stats/' +str(game2)
            
            df_period_stats = pd.read_html(url3)
            
            time.sleep(3)

#since we are creating sql tables, we no longer need the lists

#this way seems better for memory

#game_results_box_score.append(df_box_score)

#game_results_pxp.append(df_pxp)

#game_results_period_stats.append(df_period_stats)

        game_list.append(gameId)
        create_tables(gameId,game2,df_box_score,df_pxp,df_period_stats)
        print('success')
        except ValueError:
        print('\tVerror')
        no_games.append(gameId)
        pass
        except HTTPError as err:
        print('HttpError')
        no_games.append(gameId)
        pass

connection.close()

engine.dispose()

#I think .close and .displose is right, haven't tested it, shouldn't matter








#=====================================================================
# df_box_score.to_csv('df_box_score.csv', index=False)
# df_period_stats.to_csv('df_period_stats.csv', index=False)
# df_pxp.to_csv('df_pxp.csv', index=False)
# game_id.to_csv('game_id.csv', index=False)
# game_ids.to_csv('game_ids.csv', index=False)
# gameId.to_csv('gameId.csv', index=False)
# games_today.to_csv('games_today.csv', index=False)

# for i, df in enumerate(df_box_score):
#     df.to_csv(f'df_box_score_{i}.csv', index=False)

# for i, df in enumerate(df_period_stats):
#     df.to_csv(f'df_period_stats_{i}.csv', index=False)

# for i, df in enumerate(df_pxp):
#     df.to_csv(f'df_pxp_{i}.csv', index=False)
    
# #Running into issues getting these into csv
# for i, df in enumerate(game_id):
#     df.to_csv(f'game_id_{i}.csv', index=False)

# for i, df in enumerate(game_ids):
#     df.to_csv(f'game_ids_{i}.csv', index=False)

# for i, df in enumerate(gameId):
#     df.to_csv(f'gameId_{i}.csv', index=False)

# for i, df in enumerate(games_today):
#     df.to_csv(f'games_today_{i}.csv', index=False)





# #============================================================================


# # Create or connect to an SQLite database file
# conn = sqlite3.connect('ncaaHoops.db')

# # Assume df_box_score, df_period_stats, etc. are lists of DataFrames
# # Iterate over each DataFrame in the lists and store them in the database
# for i, df_list in enumerate([df_box_score, df_period_stats, df_pxp, game_id, game_ids, gameId, games_today]):
#     for j, df in enumerate(df_list):
#         table_name = f'table_{i+1}_{j+1}'  # Create a unique table name for each DataFrame
#         df.to_sql(table_name, conn, index=False)  # Store the DataFrame in the database as a table

# # Commit changes and close connection
# conn.commit()
# conn.close()

# print("DataFrames have been stored in the SQLite database.")

# #============================================================================

# try:
#     conn = sqlite3.connect('ncaaHoops.db')

#     # Assume df_box_score, df_period_stats, etc. are lists of DataFrames
#     # Iterate over each DataFrame in the lists and store them in the database
#     for i, df_list in enumerate([df_box_score, df_period_stats, df_pxp]):
#         for j, df in enumerate(df_list):
#             table_name = f'table_{i+1}_{j+1}'  # Create a unique table name for each DataFrame
#             df.to_sql(table_name, conn, index=False)  # Store the DataFrame in the database as a table

#     # Commit changes and close connection
#     conn.commit()
#     conn.close()

#     print("DataFrames have been stored in the SQLite database.")

# except Exception as e:
#     print("An error occurred:", e)







# # , game_id, game_ids, gameId, games_today



