#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 15:32:50 2024

@author: pauladam
"""

import pandas as pd
import datetime as dt
import requests
import time
from urllib.error import HTTPError

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


from sqlalchemy import create_engine, text
import pyodbc
print(pyodbc.drivers())

#print("connected")
username = 'ncaa-hoops'
password = 'paulZachChadServer!'
hostname = 'ncaa-hoops.database.windows.net'
database_name = 'ncaa-hoops'
#driver= '{SQL Server}'
driver =  '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER=tcp:{hostname};PORT=1433;DATABASE={database_name};UID={username};PWD={password}'
engine = create_engine(f"mssql+pyodbc://?odbc_connect={conn_str}",use_setinputsizes=False)

connection = engine.connect()


def create_tables(gameId:int,game2:int,bx:list[pd.DataFrame],pxp:list[pd.DataFrame],ps:list[pd.DataFrame]) -> None:
    for i,df in enumerate(bx):
        df.to_sql(f'b{i}x{gameId}',connection)
    for i,df in enumerate(pxp):
        df.to_sql(f'p{i}xp{game2}',connection)
    for i,df in enumerate(ps):
        df.to_sql(f'p{i}s{game2}',connection)

game_list=[]
game_list2= []
game_results_box_score = []
game_results_pxp = []
game_results_period_stats = []
no_games = []
for row in game_ids:
    for gameId in row:
        print("gameID:", gameId)
        try:
            url =  'https://stats.ncaa.org/contests/' + str(gameId) + '/box_score'
            df_box_score = pd.read_html(url)
            time.sleep(3)
            headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.37"}
            page = requests.get(url, headers=headers)
            game_info = page.text
            g = game_info.find('<a href="/game/play_by_play/')
            if g < 0:
                print("not found")
            game2 = game_info[g+28: g+35]
            print("game2:", game2)
            url2 = 'https://stats.ncaa.org/game/play_by_play/' +str(game2)
            df_pxp = pd.read_html(url2)
            time.sleep(3)
            url3 =  'https://stats.ncaa.org/game/period_stats/' +str(game2)
            df_period_stats = pd.read_html(url3)
            time.sleep(3)
            game_list.append(gameId)
            create_tables(gameId,game2,df_box_score,df_pxp,df_period_stats)
            print('success')
        except ValueError as er:
            print('\tVerror')
            print(er)
            no_games.append(gameId)
            pass
        except HTTPError as err:
            print('HttpError')
            no_games.append(gameId)
            pass
'''        except Exception as err:
            print('reg',err)
            no_games.append(gameId)
            pass'''

connection.close()
engine.dispose()



