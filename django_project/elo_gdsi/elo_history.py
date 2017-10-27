import django
import pandas

import database_utils as db_utils


def _data_frame_serialize(data_frame):
    serie = '[\n'
    for row in data_frame.iterrows():
        row_data = row[1]
        row_string = []
        for key in row_data.keys():
            val = row_data[key]
            if isinstance(val, basestring):
                val = '\'{}\''.format(val.encode('utf-8'))
            key = key.encode('utf-8')
            row_string.append(' {}: {}'.format(key, val))
        serie += '    {{{}}},\n'.format(', '.join(row_string))
    serie += '\n]'
    return serie


def _get_match_result(database, match_id, player_id):
    player1_id = db_utils.get_match_info(database, match_id, 'player1_id')
    if player_id == player1_id:
        player_string = 'player1'
        opponent_string = 'player2'
    else:
        player_string = 'player2'
        opponent_string = 'player1'

    player_set1 = db_utils.get_match_info(database, match_id,
                                          '%s_set1' % player_string)
    opponent_set1 = db_utils.get_match_info(database, match_id,
                                            '%s_set1' % opponent_string)
    player_set2 = db_utils.get_match_info(database, match_id,
                                          '%s_set2' % player_string)
    opponent_set2 = db_utils.get_match_info(database, match_id,
                                            '%s_set2' % opponent_string)
    player_set3 = db_utils.get_match_info(database, match_id,
                                          '%s_set3' % player_string)
    opponent_set3 = db_utils.get_match_info(database, match_id,
                                            '%s_set3' % opponent_string)

    set1 = '%d-%d' % (player_set1, opponent_set1)
    set2 = '%d-%d' % (player_set2, opponent_set2)

    result = '%s %s' % (set1, set2)
    if player_set3 and opponent_set3:
        set3 = '%d-%d' % (player_set3, opponent_set3)
        result = '%s %s' % (result, set3)

    return result


def _append_match_info(database, data_frame, match_id, player_id):
    date = db_utils.get_match_info(database, match_id, 'date')
    # date = db_utils.get_date_from_timestamp(date_timestamp)

    player_elo_before = db_utils.get_player_elo_before_match(database,
                                                             match_id,
                                                             player_id)
    player_elo_after = db_utils.get_player_elo_after_match(database,
                                                           match_id,
                                                           player_id)
    opponent_id = db_utils.get_opponent_id(database, match_id, player_id)
    opponent_name = db_utils.get_player_name_from_id(database, opponent_id)
    opponent_elo_before = db_utils.get_player_elo_before_match(database,
                                                               match_id,
                                                               opponent_id)
    opponent_elo_after = db_utils.get_player_elo_after_match(database,
                                                             match_id,
                                                             opponent_id)

    match_result = _get_match_result(database, match_id, player_id)
    data_frame.loc[len(data_frame)] = [
        date, opponent_name, match_result, player_elo_before,
        player_elo_after, opponent_elo_before, opponent_elo_after
    ]


def get_elo_history_request(database, player_id):
    column_names = (
        u'x', u'opponent', u'result', u'elo_pre',
        u'y', u'opponent_elo_pre',
        u'opponent_elo_post'
    )

    data_frame = pandas.DataFrame(columns=column_names)
    labels = []
    for idx, retval in enumerate(db_utils.iter_player_matches(database,
                                                              player_id)):
        match_id, player_elo = retval
        match_played = not db_utils.get_match_info(database, match_id,
                                                   'not_played')
        if match_played:
            _append_match_info(database, data_frame, match_id, player_id)
            label = data_frame.tail(1).T
            labels.append(label.to_html())

    template = django.template.loader.get_template('elo_history_graph.html')
    player_name = db_utils.get_player_name_from_id(database, player_id)
    render_data = {
        'elo_data': _data_frame_serialize(data_frame),
        'player_name': player_name.encode('utf-8')
    }
    html_content = template.render(render_data)
    return django.http.HttpResponse(html_content)
