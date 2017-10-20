#!/usr/bin/python

import zeep
import json
import re

import database_utils as db_utils


_DATE_PATTERN = re.compile('/Date\((\d+)\)/')
_WSDL_LINK = 'http://tenisliga.com/ws/public.asmx?WSDL'


def _get_players_info(soap_client, player_id_start):
    return json.loads(soap_client.service.Igrac(player_id_start))


def _update_player_table(db, soap_client):
    if not db_utils.players_table_exists(db):
        db_utils.players_table_create(db)
        next_id = 0
    else:
        result = db_utils.players_table_get_max_id(db)
        next_id = result + 1 if result else 0

    players_data = _get_players_info(soap_client, next_id)
    for player_data in players_data:
        db_utils.players_table_insert_row(db, player_data['IgracId'],
                                          player_data['ImePrezime'])
    db.commit()


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

    # this should match the order of matches table columns
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
    if not db_utils.matches_table_exists(db):
        db_utils.matches_table_create(db)
        next_id = 0
    else:
        result = db_utils.matches_table_get_max_id(db)
        next_id = result + 1 if result else 0
    for data in _get_match_data(client, next_id):
        db_utils.matches_table_insert_row(db, data)
    db.commit()


def run():
    soap_client = zeep.Client(wsdl=_WSDL_LINK)
    db = db_utils.load_database()
    import ipdb; ipdb.set_trace()
    _update_player_table(db, soap_client)
    _update_matches_table(db, soap_client)


if __name__ == '__main__':
    run()
