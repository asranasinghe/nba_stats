import pandas as pd
import numpy as np
import sqlite3
import os


def create_database(db_path):
    """Creates an SQLite database at the specified path."""
    try:
        conn = sqlite3.connect(db_path)
        print(f"Database created successfully at: {db_path}")
        conn.close()
    except sqlite3.Error as e:
        print(f"Error creating database: {e}")

def create_table_from_df(db_path, table_name, df):
    """Creates a table from a Pandas DataFrame.

    Args:
        db_path: Path to the database.
        table_name: Name of the table.
        df: The Pandas DataFrame.
    """
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)  # 'replace' drops table if it exists
        conn.close()
        print(f"Table '{table_name}' created from DataFrame successfully.")
    except sqlite3.Error as e:
        print(f"Error creating table from DataFrame: {e}")

def update_data(db_path, table_name, column_to_update, new_value, where_clause):
    """Updates data in a table.

    Args:
        db_path: Database path.
        table_name: Table name.
        column_to_update: The column to update.
        new_value: The new value for the column.
        where_clause: The WHERE clause.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {table_name} SET {column_to_update} = ? WHERE {where_clause}", (new_value,))
        conn.commit()
        conn.close()
        print(f"Data updated in '{table_name}' successfully.")
    except sqlite3.Error as e:
        print(f"Error updating data: {e}")

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
    