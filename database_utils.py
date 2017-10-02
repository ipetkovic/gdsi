from collections import defaultdict
import datetime
import sqlite3


_DATABASE_NAME = 'gdsi.db'


def load_database():
    db = sqlite3.Connection(_DATABASE_NAME)
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
