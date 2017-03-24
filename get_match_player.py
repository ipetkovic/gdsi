#!/usr/bin/python
import re
import json
import sqlite3
import zeep

_DATE_PATTERN = re.compile('/Date\((\d+)\)/')


def _create_match_player_table(cursor):
    cursor.execute('create')

def _get_match_info_partial(client, start_id):
    match_info = {
        'metadata': json.loads(client.service.SusretJSON(start_id - 1)),
        'result': json.loads(client.service.SusretIgracJSON(start_id - 1))
    }
    return match_info


def _get_match_data_keys():
    return (
        ('id', 'integer primary key'),
        ('league_id', 'integer'),
        ('season_id', 'integer'),
        ('winner_id', 'integer'),
        ('player1_id', 'integer'),
        ('player2_id', 'integer'),
        ('not_played', 'integer'),
        ('not_played_reason', 'text'),
        ('date', 'integer'),
        ('location', 'text'),
        ('reported_id', 'integer')
        ('reserved_id', 'integer')
        ('quality_balls', 'integer')
        ('player1_set1', 'integer'),
        ('player1_set2', 'integer'),
        ('player1_set3', 'integer'),
        ('player2_set1', 'integer'),
        ('player2_set2', 'integer'),
        ('player2_set3', 'integer')
    )

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
        metadata['NijeOdigran'],
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

    pass


def _update_match_player_table(db, client):
    for data in _get_match_data(client, 0):
        pass

def run():
    wsdl = 'http://tenisliga.com/ws/public.asmx?WSDL'
    client = zeep.Client(wsdl)
    db = sqlite3.Connection('gdsi.db')
    _update_match_player_table(db, client)


if __name__ == '__main__':
    run()
