from datetime import datetime
from functools import lru_cache

import numpy as np
import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import pandas as pd
from flask import request

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet



def title(q=None):
    return f"Alarms for {q}"



def description(q=None):
    return f"List of latest alarms for site {q}"



dash.register_page(
    __name__,
    path_template="/site/<q>",
    title=title,
    description=description,
)


@timer
# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(site, dateFrom, dateTo, frames, pivotFrames, alarms4Site, alarmsInst):
    
    catdf = qrs.getSubcategories()
    catdf = catdf.groupby('category')['event'].apply(list)

    # reorder the categories
    catdf = pd.concat([catdf.loc[['Network']], catdf.loc[['Infrastructure']], catdf.loc[~catdf.index.isin(['Network', 'Infrastructure'])]]).reset_index()
    
    if site:
        out = []

        if alarms4Site['cnt'].sum() > 0:
            for cat, events in catdf.values:
                category_list = []

                for event in sorted(events):

                    if event in pivotFrames.keys():
                        eventDf = pivotFrames[event]
                        # find all cases where selected site was pinned in tag field
                        ids = eventDf[(eventDf['tag'] == site) & ((eventDf['to'] >= dateFrom) & (eventDf['to'] <= dateTo))]['id'].values

                        tagsDf = frames[event]
                        dfr = tagsDf[tagsDf.index.isin(ids)]
                        
                        if len(dfr)>0:
                            dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)
                            
                            button = ''
                            if event == 'path changed between sites':
                                button = dbc.Col(
                                            dbc.Button('View path changed alarms in a new tab'.upper(), id='',
                                                    href=f'{request.host_url}paths-site/{site}?dateFrom={dateFrom}&dateTo={dateTo}',
                                                    target='_blank',
                                                    className='btn btn-secondary w-100 p-1 load-pairs-button'),
                                        lg=4, md=6)

                            if len(ids):
                                element = html.Div([
                                    dbc.Row([
                                        dbc.Col(html.H2(event.upper()), lg=8, md=6, align="center"),
                                        button
                                    ], className='mb-1', justify="between"),
                                    dbc.Row(
                                        dash_table.DataTable(
                                            data=dfr.to_dict('records'),
                                            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
                                            markdown_options={"html": True},
                                            id='tbl',
                                            page_size=20,
                                            style_cell={
                                                'padding': '2px',
                                                'font-size': '14px',
                                                'whiteSpace': 'pre-line'
                                            },
                                            style_header={
                                                'backgroundColor': 'white',
                                                'fontWeight': 'bold'
                                            },
                                            style_data={
                                                'height': 'auto',
                                                'lineHeight': '15px',
                                                'overflowX': 'auto'
                                            },
                                            style_table={'overflowY': 'auto', 'overflowX': 'auto'},
                                            filter_action="native",
                                            sort_action="native",
                                        ),
                                    )
                                ], className='single-table mb-1')

                                category_list.append(element)
                    
                
                if len(category_list) == 0:
                    category_list.append(html.H2(f'No alarms in that category'))

                out.append(
                    dbc.Row(
                        dbc.Card([
                            dbc.CardHeader(html.H4(f'{cat.upper()} ALARMS', className="card-title")),
                            dbc.CardBody([
                                html.Div(category_list)
                            ], className="text-dark p-1"),
                        ], className="mb-4 site-alarms-tables boxwithshadow"
                    ), className="mb-1 g-0 align-items-start"))
                    
                    
        else:
            out = dbc.Row(
                    html.H3('No alarms for this site in the past day', className="mb-4 site-alarms-tables")
                )

    return out


@lru_cache(maxsize=None)
def loadAllTests(pq):
    measures = pq.readFile('parquet/raw/measures.parquet')
    return measures


def SitesOverviewPlots(site_name, pq):
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')

    alltests = loadAllTests(pq)

    units = {
    'ps_packetloss': 'packet loss',
    'ps_throughput': 'MBps',
    'ps_owd': 'ms'
    }

    colors = ['#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#9467bd',
            '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1',
            '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            '#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#9467bd',
            '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1',]

    
    fig = go.Figure()
    fig = make_subplots(rows=3, cols=2, subplot_titles=("Аs source: Throughput", "As destination: Throughput",
                                                        "Аs source: Packet loss", "As destination: Packet loss",
                                                        "Аs source: One-way delay", "As destination: One-way delay"))

    direction = {1: 'src', 2: 'dest'}

    alltests['dt'] = pd.to_datetime(alltests['from'])

    # convert throughput bites to MB
    alltests.loc[alltests['idx']=='ps_throughput', 'value'] = alltests[alltests['idx']=='ps_throughput']['value'].apply(lambda x: round(x/1e+6, 2))

    # extract the data relevant for the given site name
    ips = metaDf[(metaDf['site']==site_name) | (metaDf['netsite']==site_name)]['ip'].values
    ip_colors = {ip: color for ip, color in zip(ips, colors)}


    legend_names = set()

    for col in [1,2]:
        measures = alltests[alltests[direction[col]].isin(ips)].copy().sort_values('from', ascending=False)

        for i, ip in enumerate(ips):
            # The following code sets the visibility to True only for the first occurrence of an IP
            first_time_seen = ip not in legend_names
            not_on_legend = True
            legend_names.add(ip)

            throughput = measures[(measures[direction[col]]==ip) & (measures['idx']=='ps_throughput')]
            
            if not throughput.empty:
                showlegend = first_time_seen==True and not_on_legend==True
                not_on_legend = False
                fig.add_trace(
                    go.Scattergl(
                        x=throughput['dt'],
                        y=throughput['value'],
                        mode='markers',
                        marker=dict(
                            color=ip_colors[ip]),
                        name=ip,
                        yaxis="y1",
                        legendgroup=ip,
                        showlegend=showlegend),
                    row=1, col=col
                )
            else: fig.add_trace(
                    go.Scattergl(
                        x=throughput['dt'],
                        y=throughput['value']),
                    row=1, col=col
                )
            
            packetloss = measures[(measures[direction[col]]==ip) & (measures['idx']=='ps_packetloss')]
            if not packetloss.empty:
                showlegend = first_time_seen==True and not_on_legend==True
                not_on_legend = False
                fig.add_trace(
                    go.Scattergl(
                        x=packetloss['dt'],
                        y=packetloss['value'],
                        mode='markers',
                        marker=dict(
                            color=ip_colors[ip]),
                        name=ip,
                        yaxis="y1",
                        legendgroup=ip,
                        showlegend=showlegend),
                    row=2, col=col
                )
            else: fig.add_trace(
                    go.Scattergl(
                        x=packetloss['dt'],
                        y=packetloss['value']),
                    row=2, col=col
                )

            owd = measures[(measures[direction[col]]==ip) & (measures['idx']=='ps_owd')]
            if not owd.empty:
                showlegend = first_time_seen==True and not_on_legend==True
                not_on_legend = False
                fig.add_trace(
                    go.Scattergl(
                        x=owd['dt'],
                        y=owd['value'],
                        mode='markers',
                        marker=dict(
                            color=ip_colors[ip]),
                            
                        name=ip,
                        yaxis="y1",
                        legendgroup=ip,
                        showlegend=showlegend),
                    row=3, col=col
                )
            else: fig.add_trace(
                    go.Scattergl(
                        x=owd['dt'],
                        y=owd['value']),
                    row=3, col=col
                )


    fig.update_layout(
            margin=dict(t=20, b=20, l=0, r=0),
            showlegend=True,
            legend_orientation='h',
            legend_title_text=f'IP addresses for site {site_name}',
            legend=dict(
                traceorder="normal",
                font=dict(
                    family="sans-serif",
                    size=14,
                ),
                x=0,
                y=1.2,
                xanchor='left',
                yanchor='top'
            ),
            height=900,
            autosize=True,
            width=None,
        )


    # Update yaxis properties
    fig.update_yaxes(title_text=units['ps_throughput'], row=1, col=col)
    fig.update_yaxes(title_text=units['ps_packetloss'], row=2, col=col)
    fig.update_yaxes(title_text=units['ps_owd'], row=3, col=col)
    fig.layout.template = 'plotly_white'
    # py.offline.plot(fig)

    return fig





def layout(q=None, **other_unknown_query_strings):
  pq = Parquet()
  alarmsInst = Alarms()
  # that period should match on /home page
  dateFrom, dateTo = hp.defaultTimeRange(2)
  frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)

  alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
  alarmCnt = alarmCnt[alarmCnt['site'] == q]

  if q:
    print('URL query:', q)
    print(f'Number of alarms: {len(alarmCnt)}')
    print()
  
    return html.Div(
            dbc.Row([
                dbc.Row([
                    dbc.Col(
                        children=html.H1(f'{q}'), 
                    id='selected-site', className='cls-selected-site', align="center"),
                    dbc.Col([
                        html.H2(f'Alarms reported in the past 24 hours', className='pb-1'),         
                        dbc.Button('Download as image', id='download-image', className='btn btn-secondary load-pairs-button dowload-page-button'),
                    ], className='cls-selected-site w-100')
                ], align="center", justify="center", className='boxwithshadow mb-1 g-0'),

                html.Div(id='datatables',
                            children=generate_tables(q, dateFrom, dateTo, frames, pivotFrames, alarmCnt, alarmsInst),
                            ),
                dbc.Row(
                    dbc.Card([
                        dbc.CardHeader(html.H3(f'{q.upper()} network measurements',
                                                className="card-title text-center")),
                        dbc.CardBody([
                            html.Div(
                                dcc.Graph(id="site-plots-in-out", 
                                figure=SitesOverviewPlots(q, pq),
                                config={'displayModeBar': False},# remove Plotly buttons so that they don't ovelap with the legend
                                className="site-plots site-inner-cont p-05")
                            )
                        ], className="text-dark p-1")
                    ], className="boxwithshadow"),
                    className="mb-4 site-alarms-tables page-cont mb-1 g-0"
                ),
                html.Br(),
                
            ], className='g-0', align="start", style={"padding": "0.5% 1.5%"}), className='main-cont')
