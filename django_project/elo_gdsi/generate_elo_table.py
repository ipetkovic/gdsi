#!/usr/bin/python

from collections import defaultdict

import database_utils as db_utils
import elo


def _generate_elo_table(db):
    db_utils.elo_table_delete_records(db)

    players = defaultdict(lambda: (0, elo.get_start_elo()))
    elo_table_rows = []
    for match_data in db_utils.iter_matches(db):
        player1_id = match_data['player1_id']
        player2_id = match_data['player2_id']
        player1_matches_num, player1_elo = players[player1_id]
        player2_matches_num, player2_elo = players[player2_id]
        winner_idx = match_data['winner_idx']

        # ignore games which are not fully played (set or sets missing)
        score = match_data['score']
        # if score and not all(game for game_set in match_data['score']
        #                      for game in game_set):
        #     score = None

        player1_elo, player2_elo = elo.calc_players_elo(player1_matches_num,
                                                        player1_elo,
                                                        player2_matches_num,
                                                        player2_elo,
                                                        score,
                                                        winner_idx)
        match_played = score is not None
        players[player1_id] = (player1_matches_num + int(match_played),
                               player1_elo)
        players[player2_id] = (player2_matches_num + int(match_played),
                               player2_elo)
        elo_table_rows.append((match_data['id'], player1_id, player1_elo))
        elo_table_rows.append((match_data['id'], player2_id, player2_elo))

    db_utils.elo_table_insert_rows(db, elo_table_rows)
    db_utils.commit(db)


def run():
    print('Generating ELO table for ZG...')
    db = db_utils.load_database_zg()
    _generate_elo_table(db)
    db_utils.close_database(db)

    print('Generating ELO table for ST...')
    db = db_utils.load_database_st()
    _generate_elo_table(db)
    db_utils.close_database(db)

if __name__ == '__main__':
    run()
