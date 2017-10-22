import os.path
from collections import defaultdict
import datetime
import sqlite3


_DATABASE_ZG_NAME = 'gdsi_zg.db'
_DATABASE_ST_NAME = 'gdsi_st.db'


def load_database_zg():
    database_path = os.path.join(os.path.dirname(__file__), _DATABASE_ZG_NAME)
    db = sqlite3.Connection(database_path)
    return db


def load_database_st():
    database_path = os.path.join(os.path.dirname(__file__), _DATABASE_ST_NAME)
    db = sqlite3.Connection(database_path)
    return db


def get_date_from_timestamp(stamp):
    return datetime.datetime.fromtimestamp(stamp/1000)


def _table_exists(cursor, name):
    cursor.execute("select count(*) from sqlite_master "
                   "where type='table' and name=(?)", (name,))
    result = cursor.fetchone()
    return result[0] != 0


def players_table_create(database):
    cursor = database.cursor()
    cursor.execute(('create table players ('
                    'id integer primary key, '
                    'name text not null)'))


def players_table_insert_row(database, player_id, name):
    cursor = database.cursor()
    cursor.execute('insert into players values(?, ?)', (player_id, name))


def players_table_get_max_id(database):
    cursor = database.cursor()
    cursor.execute('select max(id) from players')
    result = cursor.fetchone()[0]
    return result


def players_table_exists(database):
    cursor = database.cursor()
    return _table_exists(cursor, 'players')


def players_table_get(database):
    table = []
    if players_table_exists(database):
        cursor = database.cursor()
        cursor.execute('select id, name from players order by name;')
        table = cursor.fetchall()
    return table


_matches_table_columns_idx = {
    'id': 0,
    'league_id': 1,
    'season_id': 2,
    'winner_id': 3,
    'player1_id': 4,
    'player2_id': 5,
    'not_played': 6,
    'not_played_reason': 7,
    'date': 8,
    'location': 9,
    'reported_id': 10,
    'reserved_id': 11,
    'quality_balls': 12,
    'player1_set1': 13,
    'player1_set2': 14,
    'player1_set3': 15,
    'player2_set1': 16,
    'player2_set2': 17,
    'player2_set3': 18
}


def matches_table_create(database):
    cursor = database.cursor()
    cursor.execute((
        'create table matches ('
        'id integer primary key, '
        'league_id integer, '
        'season_id integer, '
        'winner_id integer, '
        'player1_id integer, '
        'player2_id integer, '
        'not_played integer, '
        'not_played_reason text, '
        'date integer, '
        'location text, '
        'reported_id integer, '
        'reserved_id integer, '
        'quality_balls integer, '
        'player1_set1 integer, '
        'player1_set2 integer, '
        'player1_set3 integer, '
        'player2_set1 integer, '
        'player2_set2 integer, '
        'player2_set3 integer, '
        'foreign key(winner_id) references players(id), '
        'foreign key(player1_id) references players(id), '
        'foreign key(player2_id) references players(id), '
        'foreign key(reported_id) references players(id), '
        'foreign key(reserved_id) references players(id))'))
    cursor.fetchone()


_matches_table_insert_query = \
  'insert into matches values({})'.format(','.join('?' * 19))


def matches_table_insert_row(database, data):
    cursor = database.cursor()
    cursor.execute(_matches_table_insert_query, data)


def matches_table_get_max_id(database):
    cursor = database.cursor()
    cursor.execute('select max(id) from matches')
    result = cursor.fetchone()[0]
    return result


def matches_table_exists(database):
    cursor = database.cursor()
    return _table_exists(cursor, 'matches')


def iterate_match_by_date(database, start_match_id):
    cursor = database.cursor()
    cursor.execute(('select * from matches where id >= ? '
                    'order by date asc, id asc'), (start_match_id,))
    for match in cursor:
        match_id = match[_matches_table_columns_idx['id']]
        match_played = not match[_matches_table_columns_idx['not_played']]
        if match_played:
            set1 = (match[_matches_table_columns_idx['player1_set1']],
                    match[_matches_table_columns_idx['player2_set1']])
            set2 = (match[_matches_table_columns_idx['player1_set2']],
                    match[_matches_table_columns_idx['player2_set2']])
            set3 = (match[_matches_table_columns_idx['player1_set3']],
                    match[_matches_table_columns_idx['player2_set3']])
            score = (set1, set2)
            if set3[0] is not None and set3[1] is not None:
                score = (set1, set2, set3)
        else:
            score = None

        data = {
            'winner_id': match[_matches_table_columns_idx['winner_id']],
            'player1_id': match[_matches_table_columns_idx['player1_id']],
            'player2_id': match[_matches_table_columns_idx['player2_id']],
            'score': score
        }

        yield (match_id, data)


def elo_table_create(database):
    cursor = database.cursor()
    cursor.execute((
        'create table elo ('
        'match_id integer primary key, '
        'player1_elo integer, '
        'player2_elo integer, '
        'foreign key(match_id) references matches(id))'
    ))
    cursor.fetchone()


def elo_table_insert_row(database, match_id, player1_elo, player2_elo):
    cursor = database.cursor()
    cursor.execute('insert into elo values(?, ?, ?)',
                   (match_id, player1_elo, player2_elo))


def elo_table_get_max_id(database):
    cursor = database.cursor()
    cursor.execute('select max(match_id) from elo')
    result = cursor.fetchone()[0]
    return result


def elo_table_exists(database):
    cursor = database.cursor()
    return _table_exists(cursor, 'elo')


def _get_player_elo_from_match(database, match_id, player_id):
    cursor = database.cursor()
    cursor.execute((
        'select player1_id, player2_id '
        '    from matches '
        '    where id == ?;'),
        (match_id, )
    )
    player1_id, player2_id = cursor.fetchone()
    if player1_id == player_id:
        query = 'player1_elo'
    elif player2_id == player_id:
        query = 'player2_elo'
    else:
        assert False, "Wrong player ID"

    cursor.execute(
        'select %s'
        '    from elo '
        '    where match_id == ?;' % query, (str(match_id), )
    )
    player_elo = cursor.fetchone()[0]
    return player_elo


def get_opponent_id(database, match_id, player_id):
    cursor = database.cursor()
    cursor.execute((
        'select player1_id, player2_id '
        '    from matches '
        '    where id == ?;'),
        (match_id, )
    )
    player1_id, player2_id = cursor.fetchone()
    if player1_id == player_id:
        opponent_id = player2_id
    elif player2_id == player_id:
        opponent_id = player1_id
    else:
        assert False, "Wrong match/player ID"

    return opponent_id


def get_player_elo_before_match(database, match_id, player_id):
    player_id_str = str(player_id)
    cursor = database.cursor()
    cursor.execute((
        'select id'
        '    from matches '
        '    where (id < ?) and (player1_id == ? or player2_id == ?) '
        '    order by date desc, id desc limit 1;'),
        (match_id, player_id_str, player_id_str)
    )
    result = cursor.fetchone()
    if result:
        last_match_id = result[0]
        elo = _get_player_elo_from_match(database, last_match_id, player_id)
    else:
        elo = 1400

    return elo


def get_player_elo_after_match(database, match_id, player_id):
    return _get_player_elo_from_match(database, match_id, player_id)


def elo_table_get_player_elo(database, player_id):
    player_id_str = str(player_id)
    cursor = database.cursor()
    cursor.execute((
        'select id'
        '    from matches '
        '    where player1_id == ? or player2_id == ? '
        '    order by date desc, id desc limit 1;'),
        (player_id_str, player_id_str)
    )
    result = cursor.fetchone()
    if result:
        match_id = result[0]
        elo = _get_player_elo_from_match(database, match_id, player_id)
    else:
        elo = 1400

    return elo


def elo_table_iter_player_by_rank(database):
    cursor = database.cursor()
    cursor.execute((
        'SELECT player1_id, name, player1_elo from ('
        '    SELECT match_id, date, player1_id, player1_elo '
        '    FROM matches '
        '    JOIN elo on match_id = matches.id '
        '    WHERE not_played == 0 '
        '    UNION '
        '    SELECT match_id, date, player2_id, player2_elo '
        '    FROM matches '
        '    JOIN elo on match_id = matches.id '
        '    WHERE not_played == 0 '
        '    ORDER BY match_id asc'
        ') '
        'JOIN players '
        'WHERE player1_id == players.id '
        'GROUP BY player1_id '
        'ORDER BY player1_elo DESC;'
     ))
    for player in cursor:
        yield player


def elo_table_get_players_data(database):
    cursor = database.cursor()
    players_data = defaultdict(lambda: (0, 0))
    cursor.execute((
        'select player1_id, player1_elo, player2_id, player2_elo '
        'from matches join elo on match_id = matches.id '
        'where not_played == 0 '
        'order by date asc, match_id asc;'
    ))
    for match_data in cursor:
        player1_id = match_data[0]
        player1_elo = match_data[1]
        player2_id = match_data[2]
        player2_elo = match_data[3]
        players_data[player1_id] = (players_data[player1_id][0] + 1,
                                    player1_elo)
        players_data[player2_id] = (players_data[player2_id][0] + 1,
                                    player2_elo)
    return players_data


def iter_player_matches(database, player_id):
    cursor = database.cursor()
    cursor.execute((
        'select id, player1_id, player1_elo, '
        'player2_id, player2_elo '
        'from matches join elo on match_id = matches.id '
        'where player1_id == ? or player2_id == ? '
        'order by date asc'
    ), (player_id, player_id))
    for match in cursor:
        match_id = match[0]
        elo = match[2] if player_id == match[1] else match[4]
        yield (match_id, elo)


def get_player_name_from_id(database, player_id):
    cursor = database.cursor()
    cursor.execute('select name from players where id == ?;', (player_id, ))
    retval = cursor.fetchone()
    if retval:
        return retval[0]


def get_match_info(database, match_id, key):
    cursor = database.cursor()
    cursor.execute('select %s from matches where id == ?' % key, (match_id, ))
    retval = cursor.fetchone()
    if retval:
        return retval[0]


def get_num_matches_played(database, player_id):
    import ipdb; ipdb.set_trace()
    cursor = database.cursor()
    cursor.execute((
        'SELECT count(*) '
        'from matches '
        'WHERE (not_played == 0) and '
        '    (player1_id == ? or player2_id == ?);'
        ), (player_id, ))
    return cursor.fetchone()[0]
