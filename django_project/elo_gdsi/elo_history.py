import django
import pandas
import datetime

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
    match_score = db_utils.get_match_score(database, match_id, player_id)

    return ' '.join(map(lambda x: '%d-%d' % (x[0], x[1]), match_score))


def _append_match_info(database, data_frame, match_id,
                       date, match_score, player_id, player_elo_before,
                       player_elo_after,
                       opponent_id, opponent_name,
                       opponent_elo_before,
                       opponent_elo_after):
    match_result = ' '.join(map(lambda x: '%d-%d' % (x[0], x[1]),
                                match_score))
    timestamp = (date - datetime.datetime(1970, 1, 1)).total_seconds() * 1000
    data_frame.loc[len(data_frame)] = [
        timestamp, opponent_name, match_result, player_elo_before,
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
    for match_info in db_utils.iter_player_matches(database, player_id):
        if match_info['match_played']:
            _append_match_info(database, data_frame, match_info['match_id'],
                               match_info['date'], match_info['score'],
                               player_id, match_info['player_elo_pre'],
                               match_info['player_elo_post'],
                               match_info['opponent_id'],
                               match_info['opponent_name'],
                               match_info['opponent_elo_pre'],
                               match_info['opponent_elo_post'])
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
