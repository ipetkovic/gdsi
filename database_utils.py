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


def load_database_st():
    database = pyodbc.connect(DRIVER='{ODBC Driver 13 for SQL Server}',
                              SERVER='mssql3.mojsite.com,1555',
                              DATABASE='dankogr_TenisLigaST',
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


def get_opponent(database, match_id, player_id):
    cursor = database.cursor()
    cursor.execute("""
        SELECT Igrac.IgracId, Igrac.Ime, Igrac.Prezime
        FROM SusretIgrac
        INNER JOIN Igrac on SusretIgrac.IgracId = Igrac.IgracId
        WHERE SusretId = ? and SusretIgrac.IgracId != ?;""",
                   match_id, player_id)

    opponent = cursor.fetchone()
    return {'id': opponent[0], 'name': opponent[1] + ' ' + opponent[2]}


def players_table_get(database):
    cursor = database.cursor()
    cursor.execute("""
        SELECT IgracId, Ime + ' ' + Prezime
        FROM Igrac
        ORDER BY IME ASC, PREZIME ASC;""")
    table = cursor.fetchall()
    return table


def get_match_score(database, match_id):
    cursor = database.cursor()
    cursor.execute("""
        SELECT Set1, Set2, Set3
        FROM SusretIgrac
        WHERE SusretId = ?;""", (match_id, ))
    rows = cursor.fetchall()
    assert(len(rows) == 2)
    set1 = (rows[0][0], rows[1][0])
    set2 = (rows[0][1], rows[1][1])
    set3 = (rows[0][2], rows[1][2])
    score = (set1, set2)
    if None not in set3:
        score = (set1, set2, set3)

    return score


def get_player_elo(database, player_id):
    cursor = database.cursor()
    cursor.execute("""
        SELECT TOP 1 IgracElo
        FROM EloPovijest
        INNER JOIN Susret ON EloPovijest.SusretId = Susret.SusretId
        WHERE IgracId = ?
        ORDER BY Datum DESC, Susret.SusretId DESC;""", (player_id, ))
    elo = cursor.execute()

    return elo


def get_elo_table_max_id(database):
    cursor = database.cursor()
    cursor.execute('select MAX(SusretId) FROM EloPovijest;')
    return cursor.fetchone()[0]


def _iter_player_by_rank(database, is_doubles):
    max_id = get_elo_table_max_id(database)
    cursor = database.cursor()
    cursor.execute("""
        SELECT jaksa.IgracId, jaksa.Ime, jaksa.Prezime, jaksa.SusretId,
               jaksa.IgracElo
        FROM (
            SELECT MAX(mislav.SusretId) as SusretId, IgracId
            FROM (
                SELECT frane.IgracId, frane.Datum,
                       frane.SusretId
                FROM (
                    SELECT stipe.IgracId, stipe.Datum, stipe.SusretId
                    FROM (
                        SELECT MAX(t.Datum) as Datum, IgracId
                        FROM (
                            SELECT *
                            FROM Susret
                            WHERE NijeOdigran = 0 AND SusretId <= ?
                        ) t
                        INNER JOIN SusretIgrac
                        ON t.SusretId = SusretIgrac.SusretId
                        GROUP BY IgracId
                    ) last
                    INNER JOIN (
                        SELECT IgracId, Datum, Susret.SusretID
                        FROM SusretIgrac
                        INNER JOIN Susret
                        ON SusretIgrac.SusretId = Susret.SusretId
                    ) stipe
                    ON last.IgracId = stipe.IgracId AND
                       last.Datum = stipe.Datum
                ) frane
                INNER JOIN EloPovijest
                ON frane.IgracId = EloPovijest.IgracId AND
                   frane.SusretId = EloPovijest.SusretId
            ) mislav
            GROUP BY IgracId
        ) zvonko
        INNER JOIN (
            SELECT Igrac.IgracId, Igrac.Ime, Igrac.Prezime,
                   EloPovijest.SusretId, IgracElo, IgracPar
            FROM Igrac
            INNER JOIN EloPovijest
            ON Igrac.IgracId = EloPovijest.IgracId
        ) jaksa
        ON zvonko.SusretId = jaksa.SusretId AND
           zvonko.IgracId = jaksa.IgracId
        WHERE IgracPar = ?
        ORDER BY jaksa.IgracElo DESC;""", (max_id, is_doubles))
    for player in cursor:
        yield player


def iter_player_by_rank_singles(database):
    return _iter_player_by_rank(database, False)


def iter_player_by_rank_doubles(database):
    return _iter_player_by_rank(database, True)
