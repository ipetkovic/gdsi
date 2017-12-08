import os.path
from collections import defaultdict
import datetime
import pyodbc


_players_table = 'Igrac'
_matches_table = 'Susret'
_match_players_table = 'IgracSusret'


def load_database_demo():
    database = pyodbc.connect(DRIVER='{ODBC Driver 13 for SQL Server}',
                              SERVER='mssql3.mojsite.com,1555',
                              DATABASE='dankogr_TenisLigaDemo',
                              UID='dankogr_tenisr', PWD='xSjNQNPlGLq4')
    return database


def load_database_zg():
    database = pyodbc.connect(DRIVER='{ODBC Driver 13 for SQL Server}',
                              SERVER='mssql3.mojsite.com,1555',
                              DATABASE='dankogr_TenisLiga',
                              UID='dankogr_tenisr', PWD='xSjNQNPlGLq4')
    return database


def commit(database):
    database.commit()


def table_exists(database, name):
    cursor = database.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
           WHERE table_type = \'BASE TABLE\' and
                 table_name = ?
        """, (name, ))
    return len(cursor.fetchall())


def get_players_singles_list(database):
    player_list = tuple()
    if table_exists(database, _players_table):
        cursor = database.cursor()
        cursor.execute("""
            SELECT IgracId, Ime + ' ' + Prezime
            FROM Igrac
            WHERE IgracPar = 0; """)
        player_list = cursor.fetchall()

    return player_list


def get_players_doubles_list(database):
    player_list = tuple()
    if table_exists(database, _players_table):
        cursor = database.cursor()
        cursor.execute("""
            SELECT IgracId, Ime + ' ' + Prezime
            FROM Igrac
            WHERE IgracPar = 1; """)
        player_list = cursor.fetchall()

    return player_list


def _get_match_data_keys_idx():
    return {
        'match_id': 0,
        'date': 1,
        'is_winner': 2,
        'not_played': 3,
        'player_id': 4,
        'set1': 5,
        'set2': 6,
        'set3': 7
    }


def _is_score_invalid(winner_idx, score):
    invalid_score = False
    player1_set_winner = map(lambda x: x[0] > x[1], score)
    player1_sets_num = player1_set_winner.count(True)
    player2_sets_num = player1_set_winner.count(False)
    if winner_idx == 0:
        invalid_score = player2_sets_num >= player1_sets_num
    else:
        invalid_score = player1_sets_num >= player2_sets_num

    if not invalid_score:
        for match_set in (score[0], score[1]):
            winner_games = max(match_set)
            loser_games = min(match_set)
            games_num = winner_games + loser_games
            games_diff = winner_games - loser_games

            if (games_num < 6 or games_num > 13 or
                    games_diff > 6 or (games_diff == 1 and
                                       winner_games != 7)):
                invalid_score = True
                break

    return invalid_score


def _form_match_data(rows):
    keys_idx = _get_match_data_keys_idx()
    score = None
    if not rows[0][keys_idx['not_played']]:
        set1 = (rows[0][keys_idx['set1']],
                rows[1][keys_idx['set1']])
        set2 = (rows[0][keys_idx['set2']],
                rows[1][keys_idx['set2']])
        set3 = (rows[0][keys_idx['set3']],
                rows[1][keys_idx['set3']])
        score = (set1, set2)
        if set3[0] is not None and set3[1] is not None:
            score = (set1, set2, set3)

    winner_idx = 0
    if rows[1][keys_idx['is_winner']]:
        winner_idx = 1

    if score is not None and _is_score_invalid(winner_idx, score):
        print(score)
        score = None

    assert(rows[0][keys_idx['match_id']] ==
           rows[1][keys_idx['match_id']])
    assert(rows[0][keys_idx['date']] ==
           rows[1][keys_idx['date']])
    assert(not(rows[0][keys_idx['is_winner']] and
               rows[1][keys_idx['is_winner']]))
    assert(rows[0][keys_idx['not_played']] ==
           rows[1][keys_idx['not_played']])

    return {
        'id': rows[0][keys_idx['match_id']],
        'date': rows[0][keys_idx['date']],
        'not_played': rows[0][keys_idx['not_played']],
        'winner_idx': winner_idx,
        'player1_id': rows[0][keys_idx['player_id']],
        'player2_id': rows[1][keys_idx['player_id']],
        'score': score
    }


def iter_matches(database):
    if table_exists(database, _matches_table):
        cursor = database.cursor()
        cursor.execute("""
            SELECT x.*, Igrac.IgracPar
            FROM (
                SELECT Susret.SusretId, Susret.Datum,
                       SusretIgrac.Pobjednik, Susret.NijeOdigran,
                       SusretIgrac.IgracId, SusretIgrac.Set1,
                       SusretIgrac.Set2, SusretIgrac.Set3
                FROM Susret
                INNER JOIN SusretIgrac ON
                    SusretIgrac.SusretId = Susret.SusretId
            ) x
            INNER JOIN Igrac ON x.IgracId = Igrac.IgracId
            ORDER BY x.Datum ASC, x.SusretId ASC;""")

        import ipdb; ipdb.set_trace()
        rows = cursor.fetchmany(2)
        while rows:
            match_data = _form_match_data(rows)
            yield match_data
            rows = cursor.fetchmany(2)


def elo_table_delete_records(database):
    if table_exists(database, 'EloPovijest'):
        cursor = database.cursor()
        cursor.execute('DELETE FROM EloPovijest;')


def elo_table_insert_rows(database, rows):
    cursor = database.cursor()
    cursor.fast_executemany = True
    cursor.executemany("""
        INSERT INTO EloPovijest
            (SusretId, IgracId, IgracElo)
        VALUES (?, ?, ?);""", rows)
