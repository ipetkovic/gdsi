import pyodbc
import elo


_players_table = 'Igrac'
_matches_table = 'Susret'
_match_players_table = 'IgracSusret'


def load_database_demo():
    database = pyodbc.connect(DRIVER='{ODBC Driver 13 for SQL Server}',
                              SERVER='mssql3.mojsite.com,1555',
                              DATABASE='dankogr_TenisLigaDemo',
                              UID='dankogr_tenisr', PWD='xSjNQNPlGLq4',
                              MARS_Connection='Yes')
    return database


def load_database_zg():
    database = pyodbc.connect(DRIVER='{ODBC Driver 13 for SQL Server}',
                              SERVER='mssql3.mojsite.com,1555',
                              DATABASE='dankogr_TenisLiga',
                              UID='dankogr_tenisr', PWD='xSjNQNPlGLq4',
                              MARS_Connection='Yes')
    return database


def load_database_st():
    database = pyodbc.connect(DRIVER='{ODBC Driver 13 for SQL Server}',
                              SERVER='mssql3.mojsite.com,1555',
                              DATABASE='dankogr_TenisLigaST',
                              UID='dankogr_tenisr', PWD='xSjNQNPlGLq4',
                              MARS_Connection='Yes')
    return database


def close_database(database):
    database.close()


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

        # SezonaId == 7 -> friendly match
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
                WHERE SezonaId != 7
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


def get_player_name_from_id(database, player_id):
    cursor = database.cursor()
    cursor.execute("""
        SELECT Ime, Prezime
        FROM Igrac
        WHERE IgracId = ?;""", (player_id, ))
    name, surname = cursor.fetchone()
    if surname is not None:
        name = '%s %s' % (name, surname)

    return name


def players_table_get(database):
    cursor = database.cursor()
    cursor.execute("""
        SELECT IgracId, Ime + ' ' + Prezime
        FROM Igrac
        ORDER BY IME ASC, PREZIME ASC;""")
    table = cursor.fetchall()
    return table


def _get_match_score(player_id, keys, rows):
    assert(len(rows) == 2)

    assert (rows[0][keys['player_id']] == player_id or
            rows[1][keys['player_id']] == player_id)
    if player_id == rows[0][keys['player_id']]:
        first_row = rows[0]
        second_row = rows[1]
    else:
        first_row = rows[1]
        second_row = rows[0]

    set1 = (first_row[keys['set1']], second_row[keys['set1']])
    set2 = (first_row[keys['set2']], second_row[keys['set2']])
    set3 = (first_row[keys['set3']], second_row[keys['set3']])
    score = []
    if None not in set1:
        score.append(set1)
        if None not in set2:
            score.append(set2)
            if None not in set3:
                score.append(set3)

    return score


def get_player_elo(database, player_id):
    cursor = database.cursor()
    cursor.execute("""
        SELECT TOP 1 IgracElo
        FROM EloPovijest
        INNER JOIN Susret ON EloPovijest.SusretId = Susret.SusretId
        WHERE IgracId = ?
        ORDER BY Datum DESC, Susret.SusretId DESC;""", (player_id, ))
    result = cursor.fetchone()
    if result:
        player_elo = float(result[0])
    else:
        player_elo = elo.get_start_elo()

    return player_elo


def get_elo_table_max_id(database):
    cursor = database.cursor()
    cursor.execute('select MAX(SusretId) FROM EloPovijest;')
    return cursor.fetchone()[0]


def _form_player_match_data(player_id, rows):
    keys = {
        'match_id': 0,
        'date': 1,
        'player_id': 2,
        'player_name': 3,
        'player_surname': 4,
        'set1': 5,
        'set2': 6,
        'set3': 7,
        'elo_pre': 8,
        'elo_post': 9,
        'not_played': 10
    }

    assert (rows[0][keys['player_id']] == player_id or
            rows[1][keys['player_id']] == player_id)

    if rows[0][keys['player_id']] == player_id:
        player_data = rows[0]
        opponent_data = rows[1]
    else:
        player_data = rows[1]
        opponent_data = rows[0]

    opponent_name = opponent_data[keys['player_name']]
    opponent_surname = opponent_data[keys['player_surname']]
    if opponent_surname:
        opponent_name = '%s %s' % (opponent_name, opponent_surname)

    return {
        'match_id': player_data[keys['match_id']],
        'date': player_data[keys['date']],
        'score': _get_match_score(player_id, keys, rows),
        'player_elo_pre': player_data[keys['elo_pre']],
        'player_elo_post': player_data[keys['elo_post']],
        'opponent_id': opponent_data[keys['player_id']],
        'opponent_name': opponent_name,
        'opponent_elo_pre': opponent_data[keys['elo_pre']],
        'opponent_elo_post': opponent_data[keys['elo_post']],
        'match_played': not player_data[keys['not_played']]
    }


def iter_player_matches(database, player_id):
    cursor = database.cursor()
    cursor.execute("""
        WITH
            MATCH AS (
                SELECT EloPovijest.SusretId,
                       Datum,
                       IgracId,
                       IgracElo,
                       NijeOdigran
                FROM EloPovijest
                INNER JOIN Susret
                ON EloPovijest.SusretId = Susret.SusretId
            ),

            CTE AS (
                SELECT ROW_NUMBER()
                    OVER (
                        PARTITION BY IgracId
                        ORDER BY Datum ASC, SusretId ASC
                    ) AS Rnr,
                    SusretId,
                    Datum,
                    IgracId,
                    IgracElo,
                    NijeOdigran
                FROM MATCH
            ),

            MERGED AS (
                SELECT cur.SusretId,
                       cur.Datum,
                       cur.IgracId,
                       Igrac.Ime,
                       Igrac.Prezime,
                       SusretIgrac.Set1,
                       SusretIgrac.Set2,
                       SusretIgrac.Set3,
                       COALESCE(prev.IgracElo, 1400) AS IgracEloPrijeMeca,
                       cur.IgracElo AS IgracEloNakonMeca,
                       cur.NijeOdigran
                FROM CTE AS cur
                LEFT JOIN CTE AS prev
                ON prev.IgracId = cur.IgracId AND prev.Rnr = cur.Rnr - 1
                INNER JOIN Igrac
                ON cur.IgracId = Igrac.IgracId
                INNER JOIN SusretIgrac
                ON cur.IgracId = SusretIgrac.IgracId AND
                   cur.SusretId = SusretIgrac.SusretId
            )

        SELECT mrgd.*
        FROM (
            SELECT cur.SusretId
            FROM MERGED AS cur
            WHERE cur.IgracId = ?
        ) player_matches
        INNER JOIN
        MERGED as mrgd
        ON player_matches.SusretId = mrgd.SusretId
        ORDER BY mrgd.Datum ASC, mrgd.SusretId ASC""", (player_id, ))

    rows = cursor.fetchmany(2)
    while rows:
        match_data = _form_player_match_data(player_id, rows)
        yield match_data
        rows = cursor.fetchmany(2)


def _get_players_by_rank(database, is_doubles):
    max_id = get_elo_table_max_id(database)
    cursor = database.cursor()
    cursor.execute("""
        WITH
            match_data AS (
                SELECT ROW_NUMBER() OVER (
                    PARTITION BY EloPovijest.IgracId
                    ORDER BY Datum ASC, EloPovijest.SusretId ASC
                    ) AS Rnr,
                    EloPovijest.IgracId,
                    CASE WHEN Ime IS NULL AND Prezime IS NULL
                         THEN ''
                         WHEN Ime IS NOT NULL AND Prezime IS NULL
                         THEN Ime
                         WHEN Ime IS NULL AND Prezime IS NOT NULL
                         THEN Prezime
                         WHEN Ime IS NOT NULL AND Prezime IS NOT NULL
                         THEN Ime + ' ' + Prezime
                    END AS Ime,
                    IgracElo
                FROM EloPovijest
                INNER JOIN Susret ON EloPovijest.SusretId = Susret.SusretId
                INNER JOIN Igrac ON EloPovijest.IgracId = Igrac.IgracId
                WHERE NijeOdigran = 0 AND
                      Susret.SusretId <= ? AND
                      IgracPar = ?
            )

        SELECT ROW_NUMBER() OVER (
            ORDER BY IgracElo DESC
            ) AS idx,
            all_matches.IgracId,
            Ime,
            IgracElo
        FROM match_data AS all_matches
        INNER JOIN (
            SELECT MAX(match_data.Rnr) as Rnr, IgracId
            FROM match_data
            GROUP BY IgracId
        ) AS last_match
        ON all_matches.Rnr = last_match.Rnr AND
           all_matches.IgracId = last_match.IgracId
        ORDER BY idx ASC;""", (max_id, is_doubles))

    return cursor.fetchall()


def get_players_by_rank_singles(database):
    return _get_players_by_rank(database, False)


def get_players_by_rank_doubles(database):
    return _get_players_by_rank(database, True)
