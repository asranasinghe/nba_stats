import pandas as pd
import numpy as np
import sqlite3
import os


def alter_db(db_path, command):

  try:

    sqliteConnection = sqlite3.connect(db_path)
    cursor = sqliteConnection.cursor()

    cursor.execute(command)
    sqliteConnection.commit()


  except sqlite3.Error as e:
      print(f"Database error: {e}")
      return None

  finally:
    cursor.close()
    sqliteConnection.close()


def query_db(db_path, command):

  try:

    sqliteConnection = sqlite3.connect(db_path)
    cursor = sqliteConnection.cursor()

    cursor.execute(command)
    result = cursor.fetchall()

    return result


  except sqlite3.Error as e:
      print(f"Database error: {e}")
      return None

  finally:
    cursor.close()
    sqliteConnection.close()


def combine_team_games(df, keep_method='home'):
    '''Combine a TEAM_ID-GAME_ID unique table into rows by game. Slow.

        Parameters
        ----------
        df : Input DataFrame.
        keep_method : {'home', 'away', 'winner', 'loser', ``None``}, default 'home'
            - 'home' : Keep rows where TEAM_A is the home team.
            - 'away' : Keep rows where TEAM_A is the away team.
            - 'winner' : Keep rows where TEAM_A is the losing team.
            - 'loser' : Keep rows where TEAM_A is the winning team.
            - ``None`` : Keep all rows. Will result in an output DataFrame the same
                length as the input DataFrame.
                
        Returns
        -------
        result : DataFrame
    '''
    # Join every row to all others with the same game ID.
    joined = pd.merge(df, df, suffixes=['_H', '_A'],
                      on=['SEASON_ID', 'GAME_ID', 'GAME_DATE'])
    # Filter out any row that is joined to itself.
    result = joined[joined.TEAM_ID_H != joined.TEAM_ID_A]
    # Take action based on the keep_method flag.
    if keep_method is None:
        # Return all the rows.
        pass
    elif keep_method.lower() == 'home':
        # Keep rows where TEAM_A is the home team.
        result = result[result.MATCHUP_H.str.contains(' vs. ')]
    elif keep_method.lower() == 'away':
        # Keep rows where TEAM_A is the away team.
        result = result[result.MATCHUP_H.str.contains(' @ ')]
    elif keep_method.lower() == 'winner':
        result = result[result.WL_H == 'W']
    elif keep_method.lower() == 'loser':
        result = result[result.WL_H == 'L']
    else:
        raise ValueError(f'Invalid keep_method: {keep_method}')
    return result
    