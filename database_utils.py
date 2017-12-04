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


def _fetch_matches(database, is_doubles):
    if table_exists(database, _matches_table):
        cursor = database.cursor()
        cursor.execute("""
            SELECT match_ext.SusretId, match_ext.Datum, match_ext.NijeOdigran
            FROM(
                SELECT x.SusretId, x.Datum, x.NijeOdigran, max(x.IgracId)
                 AS IgracId
                 FROM(
                    SELECT Susret.SusretId, Susret.Datum,
                           Susret.NijeOdigran, SusretIgrac.IgracId
                    FROM Susret
                    INNER JOIN SusretIgrac
                    ON SusretIgrac.SusretId = Susret.SusretId
                 ) x
                 GROUP BY x.SusretId, x.Datum, x.NijeOdigran
            ) match_ext
            INNER JOIN Igrac
            ON match_ext.IgracId = Igrac.IgracId
            WHERE IgracPar = ?
            ORDER BY match_ext.Datum ASC, match_ext.SusretId ASC;""",
                       (is_doubles, ))
        return cursor.fetchall()


def _get_match_result(database, match_id, match_played):
    cursor = database.cursor()
    if table_exists(database, 'SusretIgrac'):
        cursor.execute('SELECT * FROM SusretIgrac where SusretId = ?',
                       (match_id, ))
        result = cursor.fetchall()
        assert (len(result) == 2)

        keys = {
            'match_id': 1,
            'player_id': 2,
            'is_winner': 3,
            'set_1': 4,
            'set_2': 5,
            'set_3': 6
        }

        player_1, player_2 = result
        winner_idx = 0
        if player_2[keys['is_winner']]:
            winner_idx = 1

        score = None
        if match_played:
            set1 = (player_1[keys['set_1']], player_2[keys['set_1']])
            set2 = (player_1[keys['set_2']], player_2[keys['set_2']])
            set3 = (player_1[keys['set_3']], player_2[keys['set_3']])
            score = (set1, set2)
            if set3[0] is not None and set3[1] is not None:
                score = (set1, set2, set3)

        return {
            'winner_id': result[winner_idx][keys['player_id']],
            'player1_id': player_1[keys['player_id']],
            'player2_id': player_2[keys['player_id']],
            'score': score
        }


def _iter_matches(database, is_doubles):
    match_data = _fetch_matches(database, False)
    for match in match_data:
        keys = {
            'id': 0,
            'date': 1,
            'not_played': 2
        }
        result = _get_match_result(database, match[keys['id']],
                                   not match[keys['not_played']])
        match_data = {
            'id': match[keys['id']],
            'date': match[keys['date']],
            'not_played': match[keys['not_played']]
        }
        yield result.update(match_data)


def iter_matches_singles(database):
    return _iter_matches(database, False)


def iter_matches_doubles(database):
    return _iter_matches(database, True)
