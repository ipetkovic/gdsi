from django.conf.urls import patterns, include, url
from django.shortcuts import render
from django.contrib import admin
import interface

admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'django_project.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^elo_calc$', interface.post_form_upload, name='elo_calc'),
    url(r'^elo_calc.html$', interface.post_form_upload, name='elo_calc'),
#    url(r'^elo_update$',elo_update.elo_up, name='elo_update'),
    url(r'^elo_probs$', interface.elo_probs_request, name='elo_probs'),
    url(r'^elo_gdsi/', interface.write_rank_list, name='elo_gdsi'),
    url(r'^elo_history/', interface.plot_elo_history, name='elo_history'),
#    url(r'^admin/', include(admin.site.urls)),
)
