#!/usr/bin/python

import database_utils as db_utils


def _get_start_elo():
    return 1400


def _get_probs_pre(elo1, elo2):
    player1_win_prob = 1 / (1 + 10**((elo2 - elo1)/400.0))
    return (player1_win_prob, 1 - player1_win_prob)


def _get_probs_post(match_data):
    player1_wins = match_data['winner_id'] == match_data['player1_id']
    flatten_result = sum(match_data['score'], ())
    gems_num = sum(flatten_result)

    if len(match_data['score']) == 3:
        prob_post = 0.5
    elif gems_num < 13:
        prob_post = 1
    elif gems_num < 14:
        prob_post = 0.95
    elif gems_num < 15:
        prob_post = 0.9
    elif gems_num < 17:
        prob_post = 0.85
    elif gems_num < 19:
        prob_post = 0.8
    elif gems_num < 21:
        prob_post = 0.75
    elif gems_num < 23:
        prob_post = 0.7
    elif gems_num < 25:
        prob_post = 0.6
    elif gems_num < 27:
        prob_post = 0.55

    probs_post = (prob_post, 1 - prob_post)
    if not player1_wins:
        probs_post = (1 - prob_post, prob_post)

    return probs_post


def _calc_players_elo(player1_matches_num, player1_elo,
                      player2_matches_num, player2_elo, match_data):
    if match_data['score']:
        player1_prob_pre, player2_prob_pre = _get_probs_pre(player1_elo,
                                                            player2_elo)
        player1_prob_post, player2_prob_post = _get_probs_post(match_data)

        player1_K = max(50, 300 - player1_matches_num * 2.5)
        player2_K = max(50, 300 - player2_matches_num * 2.5)

        player1_elo_reliable = player1_matches_num > 5
        player2_elo_reliable = player2_matches_num > 5

        if player2_elo_reliable or not player1_elo_reliable:
            player_progress = player1_prob_post - player1_prob_pre
            player1_new_elo = player1_elo + player1_K * player_progress
        else:
            player1_new_elo = player1_elo

        if player1_elo_reliable or not player2_elo_reliable:
            player_progress = player2_prob_post - player2_prob_pre
            player2_new_elo = player2_elo + player2_K * player_progress
        else:
            player2_new_elo = player2_elo
    else:
        player1_new_elo = player1_elo
        player2_new_elo = player2_elo

    return player1_new_elo, player2_new_elo


def _calc_elos(database):
    import ipdb; ipdb.set_trace()
    cursor = database.cursor()
    if db_utils.elo_table_exists(database):
        result = db_utils.elo_table_get_max_id(database)
        start_match_id = result + 1 if result else 0
    else:
        db_utils.elo_table_create(database)

    # players is dict (keys: player_id) whos values are
    #   tuple in form (matches_played, current_elo)
    players = db_utils.elo_table_get_players_data(database)
    # we want default elo for new player to be _get_start_elo()
    players.default_factory = lambda: (0, _get_start_elo())

    for retval in db_utils.iterate_match_by_date(database, start_match_id):
        match_id, match_data = retval
        player1_id = match_data['player1_id']
        player2_id = match_data['player2_id']
        player1_matches_num, player1_elo = players[player1_id]
        player2_matches_num, player2_elo = players[player2_id]

        new_elos = _calc_players_elo(player1_matches_num, player1_elo,
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
    db = db_utils.load_database()
    _calc_elos(db)


if __name__ == '__main__':
    run()
