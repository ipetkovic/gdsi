import django
from django.http import HttpResponse
import unicodecsv

import elo_gdsi.elo
from elo_gdsi import database_utils as db_utils
from elo_gdsi import elo_history
from elo_gdsi.database_utils import get_player_elo
from elo_gdsi.database_utils import get_players_by_rank_singles
from elo_gdsi.database_utils import get_players_by_rank_doubles


_LOAD_DATABASE = {
    'ZG': lambda: db_utils.load_database_zg(),
    'ST': lambda: db_utils.load_database_st()
}


def _get_choices(database):
    return db_utils.players_table_get(database)


class PostForm(django.forms.Form):
    def __init__(self, database, *args, **kwargs):
        super(PostForm, self).__init__(*args, **kwargs)
        choices = _get_choices(database)
        self.fields['player1'] = django.forms.ChoiceField(choices=choices)
        self.fields['player2'] = django.forms.ChoiceField(choices=choices)


class elo_history_form(django.forms.Form):
    def __init__(self, database, *args, **kwargs):
        super(elo_history_form, self).__init__(*args, **kwargs)
        choices = _get_choices(database)
        self.fields['id'] = django.forms.ChoiceField(choices=choices)


def _post_form_upload(request, city):
    database = _LOAD_DATABASE[city]()
    if request.method == 'GET':
        form = PostForm(database)
        form_dict = {'form': form}
        if city == 'ZG':
            template = 'elo_calc_zg.html'
        else:
            template = 'elo_calc_st.html'

        response = django.shortcuts.render(request, template, form_dict)
    else:
        form = PostForm(database, request.POST)
        if form.is_valid():
            player1_id = int(form.cleaned_data['player1'])
            player2_id = int(form.cleaned_data['player2'])
            player1_elo = get_player_elo(database, player1_id)
            player2_elo = get_player_elo(database, player2_id)
            result = elo_gdsi.elo.get_bet_coeffs(player1_elo, player2_elo)
            player1_coeff, player2_coeff = result
            response_text = (
                'Player1,Player2\n{},{}'
            ).format(player1_coeff, player2_coeff)
            response = HttpResponse(response_text, content_type='text/plain')

    db_utils.close_database(database)
    return response


def post_form_upload_zg(request):
    return _post_form_upload(request, 'ZG')


def post_form_upload_st(request):
    return _post_form_upload(request, 'ST')


def _get_rank_list(request, is_doubles, city):
    response = HttpResponse(content_type='text/plain; charset=utf-8')
    writer = unicodecsv.writer(response)
    db = _LOAD_DATABASE[city]()
    if is_doubles:
        rows = get_players_by_rank_doubles(db)
    else:
        rows = get_players_by_rank_singles(db)
    db_utils.close_database(db)
    writer.writerows(rows)

    return response


def get_rank_list_zg(request):
    return _get_rank_list(request, False, 'ZG')


def get_rank_list_doubles_zg(request):
    return _get_rank_list(request, True, 'ZG')


def get_rank_list_st(request):
    return _get_rank_list(request, False, 'ST')


def get_rank_list_doubles_st(request):
    return _get_rank_list(request, True, 'ST')


def _get_elo_probs(request, city):
    header = (
        'Player1_Id,Player2_Id,Player1_ELO,Player2_ELO,'
        'Player1_prob,Player2_prob,Player1_odds,Player2_odds'
    )
    response_data = '{header}\n'.format(header=header)

    player1_id = request.GET.get('p1')
    player2_id = request.GET.get('p2')
    if player1_id and player2_id:
        player1_id = int(player1_id)
        player2_id = int(player2_id)
        database = _LOAD_DATABASE[city]()
        player1_elo = get_player_elo(database, player1_id)
        player2_elo = get_player_elo(database, player2_id)
        db_utils.close_database(database)
        result = elo_gdsi.elo.get_win_probs(player1_elo, player2_elo)
        player1_win_prob, player2_win_prob = result
        result = elo_gdsi.elo.get_bet_coeffs(player1_elo, player2_elo)
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

    response = HttpResponse(response_data, content_type='text/plain')
    return response


def get_elo_probs_zg(request):
    return _get_elo_probs(request, 'ZG')


def get_elo_probs_st(request):
    return _get_elo_probs(request, 'ST')


def _plot_elo_history(request, city):
    template_html = {
        'ZG': 'elo_history_zg.html',
        'ST': 'elo_history_st.html'
    }
    database = _LOAD_DATABASE[city]()

    if request.method == 'GET':
        player_id = request.GET.get('id')
        if player_id is None:
            form_dict = {'form': elo_history_form(database)}
            response = django.shortcuts.render(request, template_html[city],
                                               form_dict)
        else:
            player_id = int(player_id)
            response = elo_history.get_elo_history_request(database,
                                                           player_id)
    db_utils.close_database(database)
    return response


def plot_elo_history_zg(request):
    return _plot_elo_history(request, 'ZG')


def plot_elo_history_st(request):
    return _plot_elo_history(request, 'ST')
