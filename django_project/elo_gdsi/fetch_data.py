#!/usr/bin/python

import zeep
import json
import re

import database_utils as db_utils
import elo


_DATE_PATTERN = re.compile('/Date\((\d+)\)/')
_WSDL_ZG_LINK = 'http://tenisliga.com/ws/public.asmx?WSDL'
_WSDL_ST_LINK = 'http://st.tenisliga.com/ws/public.asmx?WSDL'


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


def _create_elo_table(database):
    if db_utils.elo_table_exists(database):
        result = db_utils.elo_table_get_max_id(database)
        start_match_id = result + 1 if result else 0
    else:
        db_utils.elo_table_create(database)
        start_match_id = 0

    # players is dict (keys: player_id) whos values are
    #   tuple in form (matches_played, current_elo)
    players = db_utils.elo_table_get_players_data(database)
    # we want default elo for new player to be _get_start_elo()
    players.default_factory = lambda: (0, elo.get_start_elo())

    for retval in db_utils.iterate_match_by_date(database, start_match_id):
        match_id, match_data = retval
        player1_id = match_data['player1_id']
        player2_id = match_data['player2_id']
        player1_matches_num, player1_elo = players[player1_id]
        player2_matches_num, player2_elo = players[player2_id]

        new_elos = elo.calc_players_elo(player1_matches_num, player1_elo,
                                        player2_matches_num, player2_elo,
                                        match_data)
        player1_elo, player2_elo = new_elos

        match_played = match_data['score'] is not None
        players[player1_id] = (player1_matches_num + int(match_played),
                               player1_elo)
        players[player2_id] = (player2_matches_num + int(match_played),
                               player2_elo)
        db_utils.elo_table_insert_row(database, match_id,
                                      player1_elo, player2_elo)
    database.commit()


def run():
    soap_client_zg = zeep.Client(wsdl=_WSDL_ZG_LINK)
    db_zg = db_utils.load_database_zg()
    _update_player_table(db_zg, soap_client_zg)
    _update_matches_table(db_zg, soap_client_zg)
    _create_elo_table(db_zg)

    soap_client_st = zeep.Client(wsdl=_WSDL_ST_LINK)
    db_st = db_utils.load_database_st()
    _update_player_table(db_st, soap_client_st)
    _update_matches_table(db_st, soap_client_st)
    _create_elo_table(db_st)


if __name__ == '__main__':
    run()
