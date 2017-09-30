#!/usr/bin/python

import zeep
import json
import sqlite3
import re


_DATE_PATTERN = re.compile('/Date\((\d+)\)/')


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


def _get_players_info(soap_client, player_id_start):
    return json.loads(soap_client.service.Igrac(player_id_start))


def _update_player_table(db, soap_client):
    cursor = db.cursor()
    if not _check_if_table_exists(cursor, 'players'):
        _create_player_table(cursor)
        next_id = 0
    else:
        result = _get_max_id(cursor, 'players')
        next_id = result + 1 if result else 0

    players_data = _get_players_info(soap_client, next_id)
    for player_data in players_data:
        cursor.execute('insert into players values(?, ?)',
                       (player_data['IgracId'], player_data['ImePrezime']))
    db.commit()


def _create_matches_table(cursor):
    cursor.execute(('create table matches (id integer primary key, '
                    'league_id integer, season_id integer, winner_id integer, '
                    'player1_id integer, player2_id integer, '
                    'not_played integer, not_played_reason text, '
                    'date integer, location text, '
                    'reported_id integer, reserved_id integer, '
                    'quality_balls integer, '
                    'player1_set1 integer, player1_set2 integer, '
                    'player1_set3 integer, player2_set1 integer, '
                    'player2_set2 integer, player2_set3 integer, '
                    'foreign key(winner_id) references players(id), '
                    'foreign key(player1_id) references players(id), '
                    'foreign key(player2_id) references players(id), '
                    'foreign key(reported_id) references players(id), '
                    'foreign key(reserved_id) references players(id))'))
    cursor.fetchone()


def _get_match_info_partial(client, start_id):
    match_info = {
        'metadata': json.loads(client.service.SusretJSON(start_id - 1)),
        'result': json.loads(client.service.SusretIgracJSON(start_id - 1))
    }
    return match_info


def _form_match_data(metadata, players):
    result = re.match(_DATE_PATTERN, metadata['Datum'])
    assert(result)
    date = int(result.group(1))
    if players[0]['Pobjednik']:
        winner_id = players[0]['IgracId']
    else:
        winner_id = players[1]['IgracId']
    if metadata['DomacinLoptice'] is not None:
        quality_balls = metadata['DomacinLoptice']
    else:
        quality_balls = None

    match_data = (
        metadata['SusretId'],
        metadata['LigaId'],
        metadata['SezonaId'],
        winner_id,
        players[0]['IgracId'],
        players[1]['IgracId'],
        int(metadata['NijeOdigran']),
        metadata['RazlogNeigranja'],
        date,
        metadata['Lokacija'],
        metadata['PrijavioId'],
        metadata['TkoJeRezervirao'],
        quality_balls,
        players[0]['Set1'],
        players[0]['Set2'],
        players[0]['Set3'],
        players[1]['Set1'],
        players[1]['Set2'],
        players[1]['Set3']
    )

    return match_data


def _get_match_data(client, match_id_start):
    match_info = _get_match_info_partial(client, match_id_start)
    while len(match_info['metadata']):
        match_num = len(match_info['metadata'])
        assert(len(match_info['result']) == 2 * match_num)
        for idx in xrange(match_num):
            metadata = match_info['metadata'][idx]
            players = match_info['result'][2*idx:2*(idx + 2)]
            assert (metadata['SusretId'] == players[0]['SusretId'])
            assert (metadata['SusretId'] == players[1]['SusretId'])
            match_data = _form_match_data(metadata, players)
            yield _form_match_data(metadata, players)

        next_id = match_data[0] + 1
        match_info = _get_match_info_partial(client, next_id)


def _update_matches_table(db, client):
    cursor = db.cursor()
    if not _check_if_table_exists(cursor, 'matches'):
        _create_matches_table(cursor)
        next_id = 0
    else:
        result = _get_max_id(cursor, 'matches')
        next_id = result + 1 if result else 0
    insert_query = 'insert into matches values({})'.format(','.join('?' * 19))
    for data in _get_match_data(client, next_id):
        cursor.execute(insert_query, data)
    db.commit()


def run():
    wsdl = 'http://tenisliga.com/ws/public.asmx?WSDL'
    soap_client = zeep.Client(wsdl=wsdl)
    db = sqlite3.Connection(_get_database_name())
    _update_player_table(db, soap_client)
    _update_matches_table(db, soap_client)


if __name__ == '__main__':
    run()
