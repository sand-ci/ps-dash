import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
import pandas as pd
import numpy as np

import view.templates as tmpl
import model.queries as qrs
import utils.helpers as hp
from model.DataLoader import PrtoblematicPairsDataLoader

class ProblematicPairsPage(object):

    indx_dict = {'all': 'All', 'ps_packetloss': 'Packet loss', 'ps_owd': 'One-way delay',
             'ps_retransmits': 'Retransmits', 'ps_throughput': 'Throughput'}

    problem_types = {'threshold_reached':'The most problematic pairs', 'has_bursts':'Pairs showing bursts',
            'all_packets_lost': 'Pairs with 100% packets lost'}


    def __init__(self):
        self.obj = PrtoblematicPairsDataLoader()
        self.problems = self.getDf()
        self.threshold_reached = self.problems[(self.problems['threshold_reached'] == 1) & (self.problems['all_packets_lost'] != 1) & ((self.problems['src_not_in'] == 0) | (self.problems['dest_not_in'] == 0))].sort_values(by=['zscore', 'doc_count'], ascending=False)
        self.has_bursts = self.problems[(self.problems['has_bursts'] == 1) & ((self.problems['src_not_in'] == 0) | (self.problems['dest_not_in'] == 0))].sort_values(by=['max_hash_zscore', 'doc_count'], ascending=False)
        self.all_packets_lost = self.problems[(self.problems['all_packets_lost'] == 1) & ((self.problems['src_not_in'] == 0) | (self.problems['dest_not_in'] == 0))].sort_values(by=['all_packets_lost', 'doc_count'], ascending=False)


    def getDf(self):
        obj = PrtoblematicPairsDataLoader()
        df = obj.df.copy()
        df.fillna('N/A', inplace=True)
        df['value'] = np.where(df['idx']=='ps_packetloss', df['value']*100, df['value'])
        df['value'] = np.where(df['idx']=='ps_throughput', round(df['value']/1e+6, 2), df['value'])
        df = df.round(3)
        return df


    def getPairs(self, pair_type, row):
        if pair_type == 'host':
            source = row[row[['host_src', 'src']].first_valid_index()]
            destination = row[row[['host_dest', 'dest']].first_valid_index()]
            return str(source +' --> '+ destination)
        elif pair_type == 'site':
            src_site = row['site_src']
            dest_site = row['site_dest']
            if src_site != src_site:
                src_site = 'N/A'
            if dest_site != dest_site:
                dest_site = 'N/A'
            return src_site +' --> '+ dest_site


    def generateTable(self, idx, df, num_rows):
        asc = False
        if idx == 'ps_throughput':
            asc = True

        df = df[df['idx']==idx][['host_src', 'src', 'site_src', 'host_dest', 'dest', 'site_dest', 'measures', 'value']].sort_values('value', ascending=asc)
        display_columns = {'host_src': 'host_src', 'src': 'src', 'site_src': 'site_src', 'host_dest': 'host_dest',
                           'dest': 'dest', 'site_dest': 'site_dest', 'measures': 'measures', 'value': hp.getValueUnit(idx)}
        return  dash_table.DataTable(
                    id={'type': 'problem-table','index': idx},
                    columns=[{"name": v, "id": k} for k,v in display_columns.items()],
                    data=df.to_dict("rows"),
                    style_header=tmpl.host_table_header,
                    style_cell={'font-family':'sans-serif', 'font-size': '11px'},
                    style_cell_conditional=tmpl.gen_info_table_cell,
                    sort_action='native',
                    filter_action='native',
                    page_size=num_rows
                )


    def addSectionHeader(self, item):
        return dbc.Row([
                    html.Div(id={
                                'type': 'memory-output',
                                'index': item
                            }),
                    dbc.Col([
                        html.H2(self.indx_dict[item], className="index-title"),
                    ], width=3),
                    dbc.Col([
                        dbc.Row([
                        html.H3('Source: ', className='input-type'),
                        dcc.Input(
                            id={
                                'type': 'input-src',
                                'index': item
                            },
                            type="text",
                            className='input-value',
                            placeholder="Click on a row to select values",
                            style={'font-size':'12px'},
                        ),
                        html.H3('Destination: ', className='input-type'),
                        dcc.Input(
                            id={
                                'type': 'input-dest',
                                'index': item
                            },
                            type="text",
                            className='input-value',
                            placeholder="Click on a row to select values",
                            style={'font-size':'12px'},
                        ),
                        dbc.Button('Plot'.upper(),
                                    id={
                                        'type': 'plot',
                                        'index': item
                                    }, className='plot-input-button', n_clicks=0)
                            ], align="center", justify="end", )
                     ], width=9)
                ], align="center", justify="between", className='problems-tab-header')


    def showProblems(self, idx, df):
        if idx == 'all':
            return dbc.Row([
                        dbc.Col(
                            html.Div([
                                self.addSectionHeader(item),
                                self.generateTable(item, df, 10)
                            ], className='idx-tab'), width=12, className='boxwithshadow') for item in hp.INDECES
                   ], justify="center")
        else:
            return dbc.Row([
                        dbc.Col([self.addSectionHeader(idx),
                                 self.generateTable(idx, df, 9999)])
                   ], justify="center", className='idx-tab boxwithshadow')


    def createLayout(self):
        return html.Div([
                dbc.Row([
                    dbc.Col(width=2, className='blank-menu-item'),
                    dbc.Col(
                        dcc.Tabs(id='tabs-prob-types', className='prob-types', value='high_sigma',
                                 children=[
                                    dcc.Tab(label=v.upper(), value=k) for k, v in self.problem_types.items()
                                ])
                    , width={"size": 9}, className='horizontal-tabs')
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Tabs(
                            id='tabs-indeces', className='indeces-types', value='all',
                            children=[
                                    dcc.Tab(label=v.upper(), value=k) for k, v in self.indx_dict.items()],
                                             vertical=True, style={'float': 'left'}),
                        dbc.Row([
                            dbc.Col([
                                dcc.Interval(
                                        id='update-interval-component',
                                        interval=60*60000, # 1hour
                                        n_intervals=0,
                                        max_intervals=-1,
                                    ),
                                html.Div(id="last-updated", className='last-updated')
                            ], align="center"),
                        ], align="center"),
                    ], width={"size": 2}, className='side-menu'),
                    dbc.Col([
                        dcc.Loading(
                            id="loading-problems",
                            type="default",
                            className="loading-problems",
                            children=html.Div("Loading may take a few minutes. Refresh the page to get the data from an hour ago.",
                                              id="tabs-content", className="problems-tabs-content")
                        ),
                    ], width=9)
                ])
            ])