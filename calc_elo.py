import sqlite3
from collections import defaultdict


def _check_if_table_exists(cursor, name):
    cursor.execute("select count(*) from sqlite_master "
                   "where type='table' and name=(?)", (name,))
    result = cursor.fetchone()
    return result[0] != 0


def _create_elo_table(cursor):
    cursor.execute((
        'create table elo ('
        'match_id integer'
        'player1_elo integer, '
        'player2_elo integer, '
        'foreign key(match_id) references matches(id))'
    ))
    cursor.fetchone()


def _iterate_match(database):
    cursor = database.cursor()
    cursor.execute('select * from matches order by date asc, id asc')
    for match in cursor:
        match_id = match[0]
        match_played = not match[6]
        if match_played:
            score = [
                (match[13], match[16]),
                (match[14], match[17]),
            ]
            if match[15]:
                score.append((match[15], match[18]))
        else:
            score = None

        data = {
            'winner_id': match[3],
            'player1_id': match[4],
            'player2_id': match[5],
            'score': score
        }

        yield (match_id, data)


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
    _create_elo_table(cursor)
    players = defaultdict(lambda: (0, _get_start_elo()))
    for match_id, match_data in _iterate_match(database):
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
        cursor.execute('insert into elo values(?, ?, ?)',
                       (match_id, player1_elo, player2_elo))
    database.commit()
    import ipdb; ipdb.set_trace()


def run():
    import ipdb; ipdb.set_trace()
    db = sqlite3.Connection('gdsi.db')
    _calc_elos(db)


if __name__ == '__main__':
    run()
