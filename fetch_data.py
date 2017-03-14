import zeep
import json
import sqlite3
import re


DATE_PATTERN = re.compile('Date\((\d)+\)')

def _get_database_name():
    return 'gdsi.db'


def _check_if_table_exists(cursor, name):
    cursor.execute("select count(*) from sqlite_master "
                   "where type='table' and name=(?)", (name,))
    result = cursor.fetchone()
    return result[0] != 0


def _get_max_id(cursor, table_name):
    cursor.execute('select max(id) from {}'.format(table_name))
    result = cursor.fetchone()[0]
    return result


def _create_player_table(cursor):
    cursor.execute(('create table players (id integer primary key, '
                   'name text not null)'))
    cursor.fetchone()


def _get_players_data_from_site(soap_client, player_id_start):
    return json.loads(soap_client.service.Igrac(player_id_start))


def _update_player_table(db, soap_client):
    import ipdb; ipdb.set_trace()
    cursor = db.cursor()
    if not _check_if_table_exists(cursor, 'players'):
        _create_player_table(cursor)
        next_id = 0
    else:
        result = _get_max_id(cursor, 'players')
        next_id = result + 1 if result else 0

    players_data = _get_players_data_from_site(soap_client, next_id)
    for player_data in players_data:
        cursor.execute('insert into players values(?, ?)',
                       (player_data['IgracId'], player_data['ImePrezime']))
    import ipdb; ipdb.set_trace()
    db.commit()


def _create_matches_table(cursor):
    cursor.execute('create table matches '
                   '(id integer primary key, season_id integer, '
                   'league_id integer, date integer, '
                   'quality_balls integer, location text, '
                   'not_played integer,  report_id integer, '
                   'reserved_id integer, '
                   'foreign key(report_id) references players(id), '
                   'foreign key(reserved_id) references players(id))')
    cursor.fetchone()


def _get_matches_data_partial(soap_client, match_id_start):
    return json.loads(soap_client.service.SusretJSON(match_id_start))


def _get_matches_data_from_site(soap_client, match_id_start):
    partial_data = _get_matches_data_partial(soap_client, match_id_start)
    match_data = []
    while (partial_data):
        match_data += partial_data
        next_id = match_data[-1]['SusretId'] + 1
        partial_data = _get_matches_data_partial(soap_client, next_id)

    return match_data

        
        for match_data in matches_data:
            pass
        match_data = map(lambda x: (x['SusretId'], x['SezonaId'], x['LigaId'], x['Datum'], x['DomacinLoptice'], x['Lokacija'], x['NijeOdigran'], x['PrijavioId'], x['TkoJeRezervirao']), match)
        matches_data = _get_matches_data_partial(soap_client, match_id_start)

    return 


def _update_matches_table(db, soap_client):
    import ipdb; ipdb.set_trace()
    cursor = db.cursor()
    if not _check_if_table_exists(cursor, 'matches'):
        _create_matches_table(cursor)
        next_id = 0
    else:
        result = _get_max_id(cursor, 'matches')
        next_id = result + 1 if result else 0

    matches_data = _get_matches_data_from_site(soap_client, next_id)
    for match_data in matches_data:

    result = re.match(DATE_PATTERN, match_data['Datum'])
    assert(result)
    date = result.groups()[0]
    row = (match_data['SusretId'], match_data['SezonaId'],
           match_data['LigaId'], date,
           int(match_data['DomacinLoptice'] == 'Da'),
           match_data['Lokacija'], match_data['NijeOdigran'],
           match_data['PrijavioId'], match_data['TkoJeRezervirao'])
    cursor.execute('insert into matches values(?)', row)

def run():
    wsdl = 'http://tenisliga.com/ws/public.asmx?WSDL'
    soap_client = zeep.Client(wsdl=wsdl)
    db = sqlite3.Connection(_get_database_name())
    _update_player_table(db, soap_client)
    _update_matches_table(db, soap_client)


if __name__ == '__main__':
    run()
