import django
from backhand import database_utils
from django.shortcuts import render


def get_choices():
    # you place some logic here
    db = database_utils.load_database()
    return database_utils.players_table_get_names(db)
 

class PostForm(forms.Form):
    def __init__(self, *args, **kwargs):
        super(PostForm, self).__init__(*args, **kwargs)
        choices = get_choices()
        self.fields['player1'] = forms.ChoiceField(choices=choices)
        self.fields['player2'] = forms.ChoiceField(choices=choices)


def post_form_upload(request):
    if request.method == 'GET':
        form = PostForm()
        form_dict = {'form': form}
        return django.shortcuts.render(request, 'elo_calc.html', form_dict)
    else:
        form = PostForm(request.POST) # Bind data from request.POST into a PostForm
        if form.is_valid():
            response = HttpResponse(mimetype='text/plain')
            return response



