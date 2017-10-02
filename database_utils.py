import datetime


def get_date_from_timestamp(stamp):
    return datetime.datetime.fromtimestamp(stamp/1000)


def _check_if_table_exists(cursor, name):
    cursor.execute("select count(*) from sqlite_master "
                   "where type='table' and name=(?)", (name,))
    result = cursor.fetchone()
    return result[0] != 0


def _create_elo_table(database):
    cursor = database.cursor()
    cursor.execute((
        'create table elo ('
        'match_id integer primary key, '
        'player1_elo integer, '
        'player2_elo integer, '
        'foreign key(match_id) references matches(id))'
    ))
    cursor.fetchone()
