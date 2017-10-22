import django
import unicodecsv

from elo_gdsi import database_utils as db_utils
from elo_gdsi import calc_elo
from elo_gdsi import elo_history
from elo_gdsi.database_utils import elo_table_get_player_elo as get_player_elo
from elo_gdsi.database_utils import elo_table_iter_player_by_rank


_DATABASE = {
    'ZG': db_utils.load_database_zg(),
    'ST': db_utils.load_database_st()
}


def _get_choices(city):
    # you place some logic here
    return db_utils.players_table_get(_DATABASE[city])


class PostForm(django.forms.Form):
    def __init__(self, city, *args, **kwargs):
        super(PostForm, self).__init__(*args, **kwargs)
        choices = _get_choices(city)
        self.fields['player1'] = django.forms.ChoiceField(choices=choices)
        self.fields['player2'] = django.forms.ChoiceField(choices=choices)


class elo_history_form(django.forms.Form):
    def __init__(self, city, *args, **kwargs):
        super(elo_history_form, self).__init__(*args, **kwargs)
        choices = _get_choices(city)
        self.fields['Player'] = django.forms.ChoiceField(choices=choices)


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


def plot_elo_history_zg(request):
    import ipdb; ipdb.set_trace()
    if request.method == 'GET':
        form_dict = {'form': elo_history_form('ZG')}
        return django.shortcuts.render(request, 'elo_calc_zg.html', form_dict)
    else:
        form = elo_history_form('ZG', request.POST)
        if form.is_valid():
            player_id = int(form.cleaned_data['Player'])
            return elo_history.get_elo_history_request(_DATABASE['ZG'],
                                                       player_id)


def plot_elo_history_st(request):
    if request.method == 'GET':
        form_dict = {'form': elo_history_form('ST')}
        return django.shortcuts.render(request, 'elo_calc_st.html', form_dict)
    else:
        form = elo_history_form('ST', request.POST)
        if form.is_valid():
            player_id = int(form.cleaned_data['Player'])
            return elo_history.get_elo_history_request(_DATABASE['ST'],
                                                       player_id)
