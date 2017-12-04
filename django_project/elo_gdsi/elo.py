def get_start_elo():
    return 1400


def get_bet_coeffs(elo1, elo2):
    win_prob1, win_prob2 = _get_probs_pre(elo1, elo2)
    return (1/win_prob1, 1/win_prob2)


def get_win_probs(elo1, elo2):
    return _get_probs_pre(elo1, elo2)


def _get_probs_pre(elo1, elo2):
    player1_win_prob = 1 / (1 + 10**((elo2 - elo1)/400.0))
    return (player1_win_prob, 1 - player1_win_prob)


def _get_probs_post(winner_idx, score):
    assert (winner_idx == 0 or winner_idx == 1)
    flatten_result = sum(score, ())
    gems_num = sum(flatten_result)

    if len(score) == 3:
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
    if winner_idx == 1:
        probs_post = (1 - prob_post, prob_post)

    return probs_post


def calc_players_elo(player1_matches_num, player1_elo,
                     player2_matches_num, player2_elo,
                     score, winner_idx):
    if score:
        player1_prob_pre, player2_prob_pre = _get_probs_pre(player1_elo,
                                                            player2_elo)
        player1_prob_post, player2_prob_post = _get_probs_post(winner_idx,
                                                               score)

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
