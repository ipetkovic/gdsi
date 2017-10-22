import django
from elo_gdsi import database_utils as db_utils
from elo_gdsi import calc_elo
from elo_gdsi.database_utils import elo_table_get_player_elo as get_player_elo
from elo_gdsi.database_utils import elo_table_iter_player_by_rank
from elo_gdsi.database_utils import iter_player_matches
from elo_gdsi.database_utils import get_match_info
from elo_gdsi.database_utils import get_date_from_timestamp

import pandas
from matplotlib import pyplot
import mpld3

import unicodecsv

_DATABASE = {
    'ZG': db_utils.load_database_zg(),
    'ST': db_utils.load_database_st()
}

figure, ax = pyplot.subplots()

# Define some CSS to control our custom labels
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


def _get_choices(city):
    # you place some logic here
    return db_utils.players_table_get(_DATABASE[city])


class PostForm(django.forms.Form):
    def __init__(self, city, *args, **kwargs):
        super(PostForm, self).__init__(*args, **kwargs)
        choices = _get_choices(city)
        self.fields['player1'] = django.forms.ChoiceField(choices=choices)
        self.fields['player2'] = django.forms.ChoiceField(choices=choices)


def _post_form_upload(request, city):
    if request.method == 'GET':
        form = PostForm(city)
        form_dict = {'form': form}
        if city == 'ZG':
            template = 'elo_calc_zg.html'
        else:
            template = 'elo_calc_st.html'

        return django.shortcuts.render(request, template, form_dict)
    else:
        form = PostForm(city, request.POST)
        if form.is_valid():
            import ipdb; ipdb.set_trace()
            player1_id = int(form.cleaned_data['player1'])
            player2_id = int(form.cleaned_data['player2'])
            player1_elo = get_player_elo(_DATABASE[city], player1_id)
            player2_elo = get_player_elo(_DATABASE[city], player2_id)
            player1_coeff, player2_coeff = calc_elo.get_bet_coeffs(player1_elo,
                                                                   player2_elo)
            response_text = (
                'Player1,Player2\n{},{}'
            ).format(player1_coeff, player2_coeff)
            response = django.http.HttpResponse(response_text,
                                                content_type='text/plain')
            return response


def post_form_upload_zg(request):
    return _post_form_upload(request, 'ZG')


def post_form_upload_st(request):
    return _post_form_upload(request, 'ST')


def _get_rank_list(request, city):
    import ipdb; ipdb.set_trace()
    response = django.http.HttpResponse(content_type='text/plain; charset=utf-8')
    writer = unicodecsv.writer(response)
    for idx, player_data in enumerate(elo_table_iter_player_by_rank(_DATABASE[city])):
        player_id = player_data[0]
        player_name = player_data[1]
        player_elo = player_data[2]

        writer.writerow((idx + 1, player_id, player_name, player_elo))
    return response


def get_rank_list_zg(request):
    return _get_rank_list(request, 'ZG')


def get_rank_list_st(request):
    return _get_rank_list(request, 'ST')


def _get_elo_probs(request, city):
    header = (
        'Player1_Id,Player2_Id,Player1_ELO,Player2_ELO,'
        'Player1_prob,Player2_prob,Player1_odds,Player2_odds'
    )
    response_data = '{header}\n'.format(header=header)

    player1_id = request.GET.get('p1')
    player2_id = request.GET.get('p2')
    import ipdb; ipdb.set_trace()
    if player1_id and player2_id:
        player1_id = int(player1_id)
        player2_id = int(player2_id)
        player1_elo = get_player_elo(_DATABASE[city], player1_id)
        player2_elo = get_player_elo(_DATABASE[city], player2_id)
        result = calc_elo.get_win_probs(player1_elo, player2_elo)
        player1_win_prob, player2_win_prob = result
        result = calc_elo.get_bet_coeffs(player1_elo, player2_elo)
        player1_odds, player2_odds = result

        response_format = (
            '{header}\n{p1_id},{p2_id},{p1_elo},{p2_elo},'
            '{p1_win_prob},{p2_win_prob},{p1_odds},{p2_odds}'
        )
        response_data = response_format.format(header=header,
                                               p1_id=player1_id,
                                               p2_id=player2_id,
                                               p1_elo=player1_elo,
                                               p2_elo=player2_elo,
                                               p1_win_prob=player1_win_prob,
                                               p2_win_prob=player2_win_prob,
                                               p1_odds=player1_odds,
                                               p2_odds=player2_odds)

    response = django.http.HttpResponse(response_data,
                                        content_type='text/plain')
    return response


def get_elo_probs_zg(request):
    return _get_elo_probs(request, 'ZG')


def get_elo_probs_st(request):
    return _get_elo_probs(request, 'ST')


def _get_match_result(match_id, player_id):
    player1_id = get_match_info(_DATABASE[city], match_id, 'player1_id')
    if player_id == player1_id:
        player_string = 'player1'
        opponent_string = 'player2'
    else:
        player_string = 'player2'
        opponent_string = 'player1'

    player_set1 = get_match_info(_DATABASE[city], match_id, '%s_set1' % player_string)
    opponent_set1 = get_match_info(_DATABASE[city], match_id, '%s_set1' % opponent_string)
    player_set2 = get_match_info(_DATABASE[city], match_id, '%s_set2' % player_string)
    opponent_set2 = get_match_info(_DATABASE[city], match_id, '%s_set2' % opponent_string)
    player_set3 = get_match_info(_DATABASE[city], match_id, '%s_set3' % player_string)
    opponent_set3 = get_match_info(_DATABASE[city], match_id, '%s_set3' % opponent_string)

    set1 = '%d-%d' % (player_set1, opponent_set1)
    set2 = '%d-%d' % (player_set2, opponent_set2)

    result = '%s %s' % (set1, set2)
    if player_set3 and opponent_set3:
        set3 = '%d-%d' % (player_set3, opponent_set3)
        result = '%s %s' % (result, set3)

    return result



def plot_elo_history(request):
    player_id = request.GET.get('id')
    if player_id:
        player_id = int(player_id)
        column_names = [
            'Datum', 'Protivnik', 'Rezultat', 'ELO prije meca',
            'ELO nakon meca', 'Protivnikov ELO prije meca',
            'Protivnikov ELO nakon meca'
        ]
        data_frame = pandas.DataFrame(columns=column_names)
        labels = []
        player_elo_old = 1400
        for idx, (match_id, elo) in enumerate(iter_player_matches(_DATABASE[city], player_id)):
            match_played = not get_match_info(_DATABASE[city], match_id,
                                              'not_played')
            if match_played:
                date_timestamp = get_match_info(_DATABASE[city], match_id, 'date')
                date = get_date_from_timestamp(date_timestamp)

                opponent_id = db_utils.get_opponent_id(_DATABASE[city], match_id,
                                                       player_id)
                opponent_name = db_utils.get_player_name_from_id(_DATABASE[city],
                                                                 opponent_id)
                # opponent_name = opponent_name.encode('utf-8')
                opponent_elo_before = db_utils.get_player_elo_before_match(_DATABASE[city],
                                                                           match_id,
                                                                           opponent_id)
                opponent_elo_after = db_utils.get_player_elo_after_match(_DATABASE[city],
                                                                         match_id,
                                                                         opponent_id)

                match_result = _get_match_result(match_id, player_id)
                data_frame.loc[idx] = [
                    date, opponent_name, match_result, player_elo_old, elo,
                    opponent_elo_before, opponent_elo_after
                ]
                player_elo_old = elo
                
                label = data_frame.ix[[idx], :].T
                labels.append(label.to_html())

    
        if ax.lines:
            ax.lines.pop()
            plugins = mpld3.plugins.get_plugins(figure)
            plugins.pop(-1)
        points = ax.plot(data_frame.ix[:,0], data_frame.ix[:,4], color='b', markerfacecolor='r', marker='o', markersize=10)

        tooltip = mpld3.plugins.PointHTMLTooltip(points[0], labels,
                                                 voffset=10, hoffset=10, css=_CSS)
        mpld3.plugins.connect(figure, tooltip)# recompute the ax.dataLim
        ax.relim()
        ax.autoscale_view()
        html_content = mpld3.fig_to_html(figure)
        return django.http.HttpResponse(html_content)
