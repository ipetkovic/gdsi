from django.conf.urls import patterns, include, url
from django.shortcuts import render
from django.contrib import admin
import interface

admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'django_project.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^elo_calc_zg$', interface.post_form_upload_zg, name='elo_calc_zg'),
    url(r'^elo_calc_st$', interface.post_form_upload_st, name='elo_calc_st'),
    url(r'^elo_calc_zg.html$', interface.post_form_upload_zg,
        name='elo_calc_zg'),
    url(r'^elo_calc_st.html$', interface.post_form_upload_st,
        name='elo_calc_st'),
#    url(r'^elo_update$',elo_update.elo_up, name='elo_update'),
    url(r'^elo_probs_zg$', interface.get_elo_probs_zg, name='elo_probs_zg'),
    url(r'^elo_probs_st$', interface.get_elo_probs_st, name='elo_probs_st'),
    url(r'^elo_gdsi_zg/', interface.get_rank_list_zg, name='elo_gdsi_zg'),
    url(r'^elo_gdsi_st/', interface.get_rank_list_st, name='elo_gdsi_st'),

    url(r'^elo_history/', interface.plot_elo_history, name='elo_history'),
#    url(r'^admin/', include(admin.site.urls)),
)
