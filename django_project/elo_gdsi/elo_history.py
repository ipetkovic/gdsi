import django
import pandas
from matplotlib import pyplot
import mpld3
from mpld3.plugins import PointHTMLTooltip

import database_utils as db_utils

_CSS = """
table
{
  border-collapse: collapse;
}
th
{
  color: #ffffff;
  background-color: #000000;
}
td
{
  background-color: #cccccc;
}
table, th, td
{
  font-family:Arial, Helvetica, sans-serif;
  border: 1px solid black;
  text-align: right;
}
"""

_FIGURE, _AXES = pyplot.subplots()


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
    date_timestamp = db_utils.get_match_info(database, match_id, 'date')
    date = db_utils.get_date_from_timestamp(date_timestamp)

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
        u'Datum', u'Protivnik', u'Rezultat', u'ELO prije me\u010da',
        u'ELO nakon me\u010da', u'Protivnikov ELO prije me\u010da',
        u'Protivnikov ELO nakon me\u010da'
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

    if _AXES.lines:
        _AXES.lines.pop()
        plugins = mpld3.plugins.get_plugins(_FIGURE)
        plugins.pop(-1)

    if len(data_frame):
        dates = data_frame.ix[:, 0]
        elos = data_frame.ix[:, 4]
        points = _AXES.plot(dates, elos, color='b',
                            markerfacecolor='r', marker='o',
                            markersize=10)

        tooltip = PointHTMLTooltip(points[0], labels, voffset=10,
                                   hoffset=10, css=_CSS)
        mpld3.plugins.connect(_FIGURE, tooltip)
        _AXES.relim()
        _AXES.autoscale_view()

    html_content = mpld3.fig_to_html(_FIGURE)
    return django.http.HttpResponse(html_content)
