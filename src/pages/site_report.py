"""
This file contains the code for the page "site-report" in ps-dash.
It summarises all the alarms per site.
"""


from datetime import datetime, timedelta
from itertools import combinations

import numpy as np
import dash
import dash_bootstrap_components as dbc

from dash import Dash, dash_table, dcc, html
from dash.dependencies import Input, Output, State, ALL, MATCH
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import pandas as pd

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet
from utils.utils import defineStatus, explainStatuses, create_heatmap, createDictionaryWithHistoricalData, generate_graphs, extractAlarm, getSitePairs, getRawDataFromES
from utils.components import siteMeasurements, pairDetails, loss_delay_kibana, bandwidth_increased_decreased, throughput_graph_components, asnAnomalesPerSiteVisualisation
import functools
from collections import Counter



def title(q=None):
    return f"Report for {q}"



def description(q=None):
    return f"Weekly Site Overview for {q}"



dash.register_page(
    __name__,
    path_template="/site_report/<q>",
    title=title,
    description=description,
)

# site = None
alarmsInst = Alarms()

def layout(q=None, **other_unknown_query_strings):
    # global site
    pq = Parquet()
    site = q
    alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    alarmCnt = alarmCnt[alarmCnt['site'] == site]
    if q is not None:
        metaData = qrs.getSiteMetadata(q)
        print(metaData)
        if (metaData is None) and alarmCnt.empty:
            return html.Div(
                        className="boxwithshadow",
                        style={
                            'padding': '20px',
                            'text-align': 'center',
                            'margin': '10px 0'
                        },
                        children=[
                            html.Div(
                                html.I(className="fa fa-times-circle",
                                    style={'color': 'red', 'font-size': '48px'}),
                                style={'margin-bottom': '15px'}
                            ),
                            html.H4("No Data Found", style={'margin-bottom': '10px'}),
                            html.P(f"The site name {q} is missing in meta data. It is impossible to generate a report on the site.")
                        ]
                    )
    
    now = datetime.now()
    current_hour = pd.Timestamp.now().hour
    fromDate = (now - timedelta(days=6)).replace(hour=current_hour, minute=0, second=0, microsecond=0)
    toDate = now.replace(hour=current_hour, minute=0, second=0, microsecond=0) #as in queries we extract based on created_at, to get all the alarms we need to get up to now and after that filter the data by 'to' to get a week of data and end report with date 48hours earlier
    # toDay = toDay_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # "2024-02-26 23:59:59"
    
    global pivotFrames

    full_dates = pd.date_range(start=fromDate, end=toDate).to_list()
    
    
    
    
    # dateFrom, dateTo = hp.defaultTimeRange(2)
    # now = hp.defaultTimeRange(days=2, datesOnly=True)
    start_date, end_date = hp.defaultTimeRange(days=7)
    alarmsInst = Alarms()
    frames, pivotFrames = alarmsInst.loadData(start_date, end_date)
    print(f"fromDay: {start_date}, toDay: {end_date}")
    
    
    
    site_alarms = pd.DataFrame(columns=["to", "alarm group", "alarm name", "hosts", "IP version", "Details", 'cnt'])
    site_alarms_num = 0
    subcategories = qrs.getSubcategories()
    keys_to_remove = [
                        'path changed between sites',
                        'path changed',
                        'ASN path anomalies'
                    ]
    for key in keys_to_remove:
        frames.pop(key, None)

    # prepare table with alarms
    for frame in frames:
        df = frames[frame]
        # df = df[(pd.to_datetime(df['to']).dt.date >= fromDay_date.date()) & (pd.to_datetime(df['to']).dt.date <= toDay_date.date())]
        site_df = df[df['tag'].apply(lambda x: q in x)]
        
        if not site_df.empty:
            print(frame)
            print("----------------")
            # print(site_df.head(5))
            site_df['to'] = pd.to_datetime(site_df['to'], errors='coerce')
            
            # drop time
            site_df['to'] = site_df['to'].dt.normalize()  # or .dt.floor('D')
            site_df['to'] = site_df['to'].dt.strftime('%Y-%m-%d')
            # if frame == "hosts not found":
            #     site_df['hosts'] = None
            #     site_df['hosts list'] = None
            #     for i, row in site_df.iterrows():
            #         s = set()
            #         hosts_list = row['hosts_not_found'].values()
            #         for item in hosts_list:
            #             if item is not None:
            #                 if isinstance(item, np.ndarray):  # Handle numpy arrays
            #                     s.update(tuple(item))  # Convert to tuple first
            #                 elif isinstance(item, (list, set, tuple)):
            #                     s.update(item)
            #                 else:
            #                     s.add(item)
            #         hosts_list = s
            #         # print("HOSTS LIST")
            #         # print(hosts_list)
            #         site_df.at[i, 'hosts list'] = hosts_list
            #         site_df.at[i, 'hosts'] = hosts_list
            site_df = alarmsInst.formatDfValues(site_df, frame, False, True)
            if 'hosts' in site_df.columns:
                site_df['hosts'] = site_df['hosts'].apply(lambda x: html.Div([html.Div(item) for item in x.split('\n')]) if isinstance(x, str) else x)
            site_alarms_num += len(site_df)
            site_df.reset_index(drop=True, inplace=True)
            for i, alarm in site_df.iterrows():
                add_hosts = 'hosts'
                if 'host' in alarm:
                    add_hosts = 'host'
                new_row = {
                    # 'from': alarm.get('from', '"from" field not found'),
                    'to': alarm.get('to', '"to" field not found'),
                    'alarm group': subcategories[subcategories['event'] == frame]['category'].values[0],
                    'alarm name': frame,
                    'IP version': alarm.get('IP version', None),  # Added IP version if available
                    'hosts': alarm[add_hosts] if add_hosts in alarm else None,
                    'Details': alarm.get('alarm_link', None),  # Added Details if available
                    'cnt': alarm.get('total_paths_anomalies', 1)
                }
                dest_site_fields = ['sites', 'cannotBeReachedFrom', 'to_dest_loss', 'from_src_loss', 'src_sites', 'dest_sites', 'dest_netsite', 'dest_site', 'src_site', 'src_netsite']  # ordered by priority if multiple exist
                destination_sites = []
                for field in dest_site_fields:
                    if field in alarm.keys():
                        destination_sites = destination_sites + [html.Div(item) for item in alarm[field].split('\n')]
                new_row['Involved Site(s)'] = destination_sites
                site_alarms = pd.concat([site_alarms, pd.DataFrame([new_row])], ignore_index=True)

    print(f"Total alarms collected: {site_alarms_num}")
     # extract meta data like CPU, country and hosts
    meta_df = qrs.getMetaData()
    meta_df['host_ip'] = meta_df.apply(
        lambda row: (row['host'], 'ipv6' if row['ipv6'] else 'ipv4'),
        axis=1
    )
    meta_df_site = meta_df[meta_df['site'] == q]
    hosts_ip = meta_df_site["host_ip"].tolist()
    if len(hosts_ip) < 1:
        hosts_ip = [("hosts information not available", '-')]

    country = meta_df_site["country"].values[0] if len(meta_df_site) > 0 else None
    site_meta_data = qrs.getSiteMetadata(q)
    if site_meta_data is not None:
        cpus, cpu_cores = site_meta_data.iloc[0]['cpus'], site_meta_data.iloc[0]['cpu_cores']
    else:
        cpus, cpu_cores = None, None
    if site_alarms_num == 0:
        return html.Div([
                dbc.Col([
                    
                    
                    html.Div(
                        className="boxwithshadow",
                        style={
                            'padding': '20px',
                            'width': "99%",
                            'justify-self': 'center',
                            'margin-top': '15px',
                            'margin-bottom': '15px',
                            },
                        children=[
                                dbc.Col([
                                        html.H3(f"{q} Daily Status", style={"padding-left": "3%", "padding-top": "3%", 'margin-bottom': '0px',"background-color": "#ffffff"}),
                                        html.Div(
                                            dcc.Graph(
                                                id="site-status-alarms",
                                                figure=create_status_chart_explained(site_alarms, start_date, end_date, full_dates),
                                                config={'displayModeBar': False},
                                                style={
                                                    'width': '100%',
                                                    'height': '100px'
                                                }
                                            ), style={'margin-top': '0px',"background-color": "#ffffff"}
                                        ),
                                        dcc.Store(id='site-statuses', data={}),
                                        dbc.Modal(
                                            [
                                                dbc.ModalHeader(dbc.ModalTitle("How was the status determined?")),
                                                dbc.ModalBody(id="how-status-modal-body-report"),
                                                dbc.ModalFooter(
                                                    dbc.Button("Close", id="close-how-status-modal-report", className="ml-auto", n_clicks=0)
                                                ),
                                            ],
                                            id="how-status-modal-report",
                                            size="lg",
                                            is_open=False,
                                        )
                                ])
                                
                        ]
                    ),
                    html.Div(
                        style={
                            'width': "99%",
                            'justify-self': 'center',
                            'margin-bottom': '10px',
                            },
                        children=[
                                dbc.Row([   
                                    dbc.Col(
                                        html.Div(
                                            className="boxwithshadow page-cont p-2 h-100",
                                            children=[
                                                html.H3(f"Number of Alarms", style={"color": "white", "padding-left": "5%", "padding-top": "5%"}),
                                                html.H1(
                                                    '0',
                                                    style={
                                                        "color": "white", 
                                                        "font-size": "1000%",
                                                        "display": "flex",          # Enables Flexbox
                                                        "justify-content": "center", # Centers horizontally
                                                        "align-items": "center",     # Centers vertically
                                                        "height": "100%",            # Takes full available height
                                                        "margin": "0",               # Removes default margins
                                                        "padding-bottom": "20px"    # Optional: Adjusts spacing
                                                    }
                                                )
                                            ],
                                            style={
                                                "background-color": "#00245a",
                                                "display": "flex",              # Flexbox for the container
                                                "flex-direction": "column",     # Stacks children vertically
                                            }
                                        ),
                                        width=3
                                    ),
                            dbc.Col(html.Div(
                                    className="boxwithshadow page-cont p-2 h-100",
                                    style={
                                        "padding": "20px",
                                        'text-align': 'center',
                                        "display": "flex",              # Flexbox for the container
                                        "flex-direction": "column",     # Stacks children vertically
                                        'justify-content': 'center'
                                    },
                                    children=[
                                        html.Div(
                                            html.I(className="fas fa-check-circle", 
                                                style={'color': 'green', 'font-size': '48px'}),
                                            style={'margin-bottom': '15px'}
                                        ),
                                        html.H4("No Alarms Detected", style={'margin-bottom': '10px'}),
                                        html.P(f"There were no alarms during the last 7 days at {q}")
                                    ]
                            ), width=5),
                            # metadata card
                            dbc.Col(
                                html.Div(className="boxwithshadow page-cont p-2 h-100", style={"background-color": "#ffffff", "align-content":"center", 'justify-items':'stretch'}, children=[
                                    # html.H3("Summary", style={"padding-top": "5%", "padding-left": "5%"}),
                                    dbc.Row(
                                        [
                                        dbc.Col(
                                            html.Div(
                                                [   
                                                    html.H4(f"Country: \t{country}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    html.H4(f"CPUs: \t{cpus}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    html.H4(f"CPU Cores: \t{cpu_cores}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    ],
                                                 style={"height": "100%", "display": "flex", "flex-direction": "column"}
                                                ),
                                                    width=6,
                                                    style={
                                                        "border-right": "1px solid #ddd",  # Thin vertical line
                                                        "padding-right": "20px",
                                                        "padding-top": "20px",
                                                        "align-content":"center"
                                                        
                                                    }
                                        ),
                                        dbc.Col([
                                            html.H4(f"{site} hosts:", style={"padding-left": "10%", "padding-top": "20px"}),
                                            html.Div([
                                                
                                                html.Ul(
                                                    [
                                                        html.Li(
                                                            [
                                                                html.Span(f"{host} ", className="font-weight-bold"),
                                                                html.Span(
                                                                    f"({ip_ver})",
                                                                    className="badge badge-pill badge-success" if ip_ver == "ipv6" 
                                                                    else "badge badge-pill badge-primary"
                                                                )
                                                            ],
                                                            className=""
                                                        )
                                                        for host, ip_ver in hosts_ip  
                                                    ],
                                                    className="list-unstyled", style={"padding-top": "5%"},
                                            )
                                            ], style={
                                                            'height': '200px', 
                                                            'overflow-y': 'auto',  # Enable vertical scrolling
                                                            'width': '100%', # Thin vertical line
                                                            "padding-right": "20px",
                                                            "padding-left": "10%",
                                                            "justify-self": "end"
                                                        }
                                            )
                                        ], width=6)
                                        
                                    ], className="align-items-stretch",  # Makes columns equal height
                                ),
                                
                            
                            ]), width=4, style={"background-color": "#b9c4d4;", "height": "100%"})
                        ], className="my-3", style={"height": "300px"})
                        ]),
                    
                # general websites' measurements
                html.Div(id='site-measurements',
                        children=siteMeasurements(q, pq),
                        style={
                            'width': "99%",
                            'justify-self': 'center',
                            'margin-bottom': '10px',
                            }),
                                
                ])
        ])
        
    
    return html.Div(children = [
        html.Div(id='scroll-trigger', style={'display': 'none'}),
        dcc.Store(id='fromDay', data=fromDate.strftime('%Y-%m-%dT%H:%M:%S.000Z')),
        dcc.Store(id='toDay', data=toDate.strftime('%Y-%m-%dT%H:%M:%S.000Z')),
        dcc.Store(id='now', data=now.strftime('%Y-%m-%dT%H:%M:%S.000Z')),
        dcc.Store(id='alarm-storage', data={}),
        dcc.Store(id='site', data=site),
        dbc.Row([
            dbc.Row(
                dbc.Col([
                    
                        dbc.Row(children=[
                            dbc.Col([
                                html.Div(
                                    className="boxwithshadowhidden",
                                    id="status-container",
                                    style={"background-color": "#ffffff"},
                                    children=[
                                        # header with toggle button
                                        html.Div(
                                                html.I(
                                                    className="fas fa-question-circle",
                                                    id="how-status-modal-trigger",
                                                    n_clicks=0,
                                                    style={
                                                        "position": "absolute",
                                                        "top": "10px",
                                                        "right": "10px",
                                                        "cursor": "pointer",
                                                        "font-size": "20px",
                                                        "color": "#6c757d",
                                                        "z-index": "1000"  # Ensure it stays above other elements
                                                    }
                                                ),
                                                style={
                                                    "position": "relative",  # Needed for absolute positioning of child
                                                    'margin-bottom': '0px'
                                                }
                                            ),
                                        dbc.Row([
                                            dbc.Col(html.H3(f"{q} Daily Status", style={"padding-left": "3%", "padding-top": "3%", 'margin-bottom': '0px',"background-color": "#ffffff"})),
                                            dbc.Col(
                                                dbc.Button(
                                                        "Show Details ⬇",
                                                        id="toggle-alarms-button",
                                                        color="link",
                                                        style={"font-size": "0.8rem", "padding-top": "3%","background-color": "#ffffff"}
                                                    ),
                                                )
                                                 ], style={'margin-bottom': '0px', "background-color": "#ffffff"}),
                                        
                                        # Status graph
                                        html.Div(
                                            dcc.Graph(
                                                id="site-status-alarms",
                                                figure=create_status_chart_explained(site_alarms, start_date, end_date, full_dates),
                                                config={'displayModeBar': False},
                                                style={
                                                    'width': '100%',
                                                    'height': '350px'
                                                }
                                            ), style={'margin-top': '0px',"background-color": "#ffffff"}
                                        ),
                                        dcc.Store(id='site-statuses', data={}),
                                        dbc.Modal(
                                            [
                                                dbc.ModalHeader(dbc.ModalTitle("How was the status determined?")),
                                                dbc.ModalBody(id="how-status-modal-body-report"),
                                                dbc.ModalFooter(
                                                    dbc.Button("Close", id="close-how-status-modal-report", className="ml-auto", n_clicks=0)
                                                ),
                                            ],
                                            id="how-status-modal-report",
                                            size="lg",
                                            is_open=False,
                                        )
                                    ])
                                ])
                            ], className="mb-1 pl-1"),
                        
                        # second row of stats: number of alarms, alarms types and categories distribution, metadata
                        dbc.Row([
                            dbc.Col(
                                html.Div(
                                    className="boxwithshadow page-cont p-2 h-100",
                                    children=[
                                        html.H3(f"Number of Alarms", style={"color": "white", "padding-left": "5%", "padding-top": "5%"}),
                                        html.H1(
                                            # f"{site_alarms_num if site_alarms_num > 0 else 0}", 
                                            id="num-alarms",
                                            style={
                                                "color": "white", 
                                                "font-size": "1000%",
                                                "display": "flex",          # Enables Flexbox
                                                "justify-content": "center", # Centers horizontally
                                                "align-items": "center",     # Centers vertically
                                                "height": "100%",            # Takes full available height
                                                "margin": "0",               # Removes default margins
                                                "padding-bottom": "20px"    # Optional: Adjusts spacing
                                            }
                                        )
                                    ],
                                    style={
                                        "background-color": "#00245a",
                                        "display": "flex",              # Flexbox for the container
                                        "flex-direction": "column",     # Stacks children vertically
                                    }
                                ),
                                width=3
                            ),
                            
                            dcc.Store(id='alarms-data-compressed', data=site_alarms.to_dict('records')), 
                            dcc.Store(id='all-dates', data=str([date.strftime('%Y-%m-%d') for date in full_dates])), 
                            # bar chart: alarms name and categories distrubion
                            dbc.Col(html.Div(className="boxwithshadowhidden p-2 h-100", style={"background-color": "#ffffff"}, 
                                                children=[
                                                    html.Div([
                                                        dbc.ButtonGroup([
                                                            dbc.Button("Categories", id="btn-alarm-type", n_clicks=0, 
                                                                    color="secondary", className="me-1"),
                                                            dbc.Button("Alarms", id="btn-alarm-name", n_clicks=0,
                                                                    color="secondary")
                                                        ], style={"margin-bottom": "10px"}),
                                                        dcc.Store(id="active-button-store", data="alarm group"),
                                                    ]),
                                                    dcc.Loading(
                                                        html.Div(
                                                            id="type-of-alarms",
                                                            children=[
                                                                dcc.Graph(id="bar-graph", figure=create_bar_chart(site_alarms, full_dates, 'alarm group')),
                                                            ],
                                                            style={"height": "100%", "width": "100%"}
                                                        ),
                                                        color='#ffffff'
                                                    )
                                                ]
                                                ), width=5, style={"height": "100%"}),
                            # metadata card
                            dbc.Col(
                                html.Div(className="boxwithshadow page-cont p-2 h-100", style={"background-color": "#ffffff", "align-content":"center", 'justify-items':'stretch'}, children=[
                                    # html.H3("Summary", style={"padding-top": "5%", "padding-left": "5%"}),
                                    dbc.Row(
                                        [
                                        dbc.Col(
                                            html.Div(
                                                [   
                                                    html.H4(f"Country: \t{country}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    html.H4(f"CPUs: \t{cpus}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    html.H4(f"CPU Cores: \t{cpu_cores}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    dbc.Col([dbc.Button(
                                                                html.H4("View Measurements →"),
                                                                id="view-measurements-btn",
                                                                color="link"
                                                            )], style={"padding-left": "7%"})
                                                    ],
                                                    style={"height": "100%", "display": "flex", "flex-direction": "column"}
                                                ),
                                                    width=6,
                                                    style={
                                                        "border-right": "1px solid #ddd",  # Thin vertical line
                                                        "padding-right": "20px",
                                                        "padding-top": "20px",
                                                        "align-content":"center"
                                                        
                                                    }
                                        ),
                                        dbc.Col([
                                            html.H4(f"{site} hosts:", style={"padding-left": "10%", "padding-top": "20px"}),
                                            html.Div([
                                                
                                                html.Ul(
                                                    [
                                                        html.Li(
                                                            [
                                                                html.Span(f"{host} ", className="font-weight-bold"),
                                                                html.Span(
                                                                    f"({ip_ver})",
                                                                    className="badge badge-pill badge-success" if ip_ver == "ipv6" 
                                                                    else "badge badge-pill badge-primary"
                                                                )
                                                            ],
                                                            className=""
                                                        )
                                                        for host, ip_ver in hosts_ip  
                                                    ],
                                                    className="list-unstyled", style={"padding-top": "5%"},
                                            )
                                            ], style={
                                                            'height': '200px', 
                                                            'overflow-y': 'auto',  # Enable vertical scrolling
                                                            'width': '100%', # Thin vertical line
                                                            "padding-right": "20px",
                                                            "padding-left": "10%",
                                                            "justify-self": "end"
                                                        }
                                            )
                                        ], width=6)
                                        
                                    ], className="align-items-stretch",  # Makes columns equal height
                                ),
                                
                            
                            ]), width=4, style={"background-color": "#b9c4d4;", "height": "100%"})
                        ], className="my-3 pl-1", style={"height": "300px"}),
                    
                        
                        
                        # third row with alarms list and summary
                        dbc.Row([
                            
                            # Left Column (Summary)
                            dbc.Col([
                                
                                html.Div(
                                    className="boxwithshadow page-cont p-2 h-100",
                                    children=[
                                        html.H3("Summary", 
                                                style={
                                                    "padding-left": "5%", 
                                                    "padding-top": "5%"
                                                }),
                                        html.H4(generate_summary(fromDate, toDate, site_alarms), style={
                                                    "display": "flex",
                                                    "justify-content": "center",
                                                    "align-items": "stretch",
                                                    "margin-top": "20px",
                                                    "height": "90%",
                                                    "padding-left": "8%",
                                                    "padding-right": "8%",
                                                    "font-size": "120%"
                                                    
                                                })
                                    ], style={"height": "100%"}
                                )
                            ], width=3, style={
                                "display": "flex",
                                "flex-direction": "column",
                                "height": "600px"  
                            }),
                            
                            # Right Column (Filters + Table)
                            dbc.Col([
                                html.Div(
                                    className="boxwithshadowhidden page-cont h-100",
                                    style={
                                            "height": "100%",
                                            "display": "flex",
                                            "flex-direction": "column",
                                            "background-color": "#ffffff"
                                        },
                                    children=[
                                        html.H1(f"List of alarms", className="text-center pt-2", style={"background-color": "#ffffff"}),
                                        html.H5("Filters", style={"padding-top": "1%"}, className="pl-1"),
                                        dbc.Row([
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id='filter-date',
                                                    placeholder="Date",
                                                    options=[{'label': date, 'value': date} for date in sorted(site_alarms['to'].unique())],
                                                    multi=True,
                                                    clearable=True,
                                                    
                                                )),
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id='filter-ip',
                                                    placeholder="IP version",
                                                    options=[{'label': ip.lower(), 'value': ip.lower()} for ip in sorted(site_alarms['IP version'].dropna().unique())],
                                                    multi=True,
                                                    clearable=True
                                                )),
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id='filter-group',
                                                    placeholder="Category",
                                                    options=[{'label': group, 'value': group} for group in sorted(site_alarms['alarm group'].unique())],
                                                    multi=True,
                                                    clearable=True
                                                )),
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id='filter-type',
                                                    placeholder="Alarm",
                                                    options=[{'label': alarm_name, 'value': alarm_name} for alarm_name in sorted(site_alarms['alarm name'].unique())],
                                                    multi=True,
                                                    clearable=True
                                                ))
                                        ], className="p-1 mb-2"),
                                        dbc.Col([
                                                dcc.Loading(
                                                    html.Div(
                                                        id='alarms-table',
                                                        style={
                                                            'height': '400px',
                                                            'overflow-y': 'scroll',  # Enable vertical scrolling
                                                            'border': '1px solid #ddd',  # Add a border for better visibility
                                                            'padding': '10px',  # Add padding inside the container
                                                            'width': '100%'
                                                        }
                                                    ),
                                                    style={"height": "100%"},
                                                    color='#00245A'
                                                )
                                        ], style={"padding-left": "1%", "padding-right": "1%"})
                                    ]
                                )
                            ], width=9, style={"height": "600px"})
                        ], className="my-3 pl-1", style={"height": "600px"}),
                        
                        #extension with the chosen alarm visualisation
                        dbc.Row([
                            dbc.Col([
                                html.Div(
                                        id="alarm-content-section",
                                        children=[
                                                dcc.Loading(
                                                        id="loading-content",  # Changed ID to avoid duplication
                                                        type="default",
                                                        color="#00245A",
                                                        children=[
                                                                    html.Div(
                                                                            className="boxwithshadowhidden",
                                                                            style={"background-color": "#ffffff"},
                                                                            children=[
                                                                                html.H3(
                                                                                    id="alarm-name-display",  # Changed ID
                                                                                    className="card header-line p-1",
                                                                                    style={
                                                                                        "background-color": "#00245a",
                                                                                        "color": "white",
                                                                                        "font-size": "25px",
                                                                                        "border-top-left-radius": "0.25rem",
                                                                                        "border-top-right-radius": "0.25rem"
                                                                                    }
                                                                                ),
                                                                                html.Div(
                                                                                    id="dynamic-content-container",
                                                                                    className=""
                                                                                )
                                                                            ]
                                                                        )
                                                                    ])
                                                ], style={"scroll-margin-top": "0px", "visibility": "hidden"}
                                ), 
                            ])
                        ], className="my-3 pl-1"),
                        
                        # general websites' measurements
                        dbc.Row([
                            html.Div(id='site-measurements',
                                    children=siteMeasurements(q, pq),
                                    style={'margin-top': "10px"},
                                                ),
                        ], className="my-3 pl-1"),
                        
                        html.Div(id="dummy-output", style={"display": "none"}),
                        html.Div(
                                id="site-status-explanation",
                                className="boxwithshadow p-0",
                                children=[
                                    # 
                                ]
                            )
                    ]),
                    # ])                 
                ),
               
        ], className="ml-1 mt-0 mr-0 mb-1"),
    
       
    ], className="mt-1")
    
def full_range_dates_df(all_dates, column, df):
    """
    Adding all the days in the week to the dataset to visualise the days without alarms for easier underdsatnding.
    """
    df['to'] = pd.to_datetime(df['to']).dt.normalize()
    all_dates = pd.to_datetime(all_dates).normalize()
    df= df.groupby(['to', column])['cnt'].sum().reset_index(name='count')
    df.rename(columns={'to':'day'}, inplace=True)
    unique_categories = df[column].unique()

    # Cross-join dates with categories to ensure all combinations exist
    if len(unique_categories) > 0:
        all_combinations = pd.MultiIndex.from_product(
            [all_dates, unique_categories],
            names=['day', column]
        ).to_frame(index=False)
        
        # Merge with actual data (fills missing combinations with 0)
        print(all_combinations.head(5))
        df = all_combinations.merge(
            df,
            on=['day', column],
            how='left'
        ).fillna({'count': 0})
    else:
        # If no categories exist, just ensure all dates are present
        df = all_dates.merge(df, on='day', how='left').fillna({'count': 0})
    
    return df


def create_bar_chart(graphData, full_range_dates, column_name='alarm group'):
    """
    This function creates the bar chart to depict the alarms' or alarm categories' 
    distribution based on the given parameter 'column_name'.
    
    """
    graphData = full_range_dates_df(full_range_dates, column_name, graphData)
    fig = px.bar(
        graphData, 
        x='day', 
        y='count', 
        color=f'{column_name}',
        barmode='stack',
        color_discrete_sequence=px.colors.qualitative.Prism
    )

    fig.update_layout(
        margin=dict(t=20, b=20, l=0, r=0),
        showlegend=True,
        legend_orientation='h',
        legend=dict(
            x=0,
            y=1.35,
            orientation="h",
            xanchor='left',
            yanchor='top',
            font=dict(
                size=10,
            ),
            
        ),
        height=250,
        plot_bgcolor='#fff',
        autosize=True,
        width=None,
        title={
            'y': 0.01,
            'x': 0.95,
            'xanchor': 'right',
            'yanchor': 'bottom'
        },
        xaxis=dict(
            # tickangle=-45,
            categoryorder='category ascending',
            automargin=True,
        ),
        modebar={
            "orientation": 'v',
        }
    )

    return fig


def create_status_chart_explained(graphData, fromDay, toDay, full_range_dates):
    """
    This function creates the horizontal bar chart where 
    the status of the site throughout the week is shown and the
    alarms which explain it are highlighted.
    """
    global statuses
    fromDay = pd.to_datetime(fromDay)
    toDay = pd.to_datetime(toDay)
    if graphData.empty:
        # Use the last 7 days from full_range_dates or generate last 7 days from toDay
        if full_range_dates is not None and len(full_range_dates) >= 7:
            days = sorted(pd.to_datetime(full_range_dates[-7:]))
        else:
            days = [toDay - pd.Timedelta(days=i) for i in reversed(range(7))]
        
        # Format days as strings for hovertext
        day_labels = [d.strftime('%d %b') for d in days]
        
        # Create figure with one horizontal bar of length 7 (one segment per day)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=[1]*7,  # Equal width segments
            y=['Status']*7,
            marker_color='green',
            orientation='h',
            hoverinfo='text',
            hovertext=[f"Day: {label}<br>Status: ok" for label in day_labels],
            showlegend=False
        ))
        
        # Update layout for a clean single bar chart
        fig.update_layout(
            barmode='stack',
            height=150,
            margin=dict(l=40, r=20, t=20, b=20),
            yaxis=dict(showticklabels=True, tickfont=dict(size=12)),
            xaxis=dict(
                tickvals=list(range(7)),
                ticktext=day_labels,
                title='Last 7 Days',
                showgrid=False,
                zeroline=False
            ),
            plot_bgcolor='rgba(0,0,0,0)'
        )
        return fig
    
    fig = make_subplots(
        rows=2, 
        cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.15,
        row_heights=[0.2, 0.8]  # 20% for status bar, 80% for alarms
    )

    # graphData = graphData[(pd.to_datetime(graphData['to']).dt.date >= fromDay.date()) & (pd.to_datetime(graphData['to']).dt.date <= toDay.date())]
    # 
    red_status, yellow_status, grey_status, graphData = defineStatus(graphData, "alarm name", ['to', 'alarm name'])
    red_status_days, yellow_status_days, grey_status_days = red_status['to'].unique().tolist(), yellow_status['to'].unique().tolist(), grey_status['to'].unique().tolist()
    # print("STATUSES")
    # print(red_status_days)
    # print(yellow_status_days)
    # print(grey_status_days)
    graphData = full_range_dates_df(full_range_dates, 'alarm name', graphData)
    days = sorted(pd.to_datetime(graphData['day'].unique()))
    # print('DAYS')
    # print(days)
    
    # --- TOP PLOT (STATUS BAR) ---
    status_colors = []
    for day in days:
        day_str = day.strftime('%Y-%m-%d')
        day_formated = day.strftime('%d %b')
        if day_str in red_status_days:
            status_colors.append(('darkred', day_formated, 'critical', red_status[red_status['to'] == day_str]['alarm name'].unique().tolist()))
        elif day_str in yellow_status_days:
            status_colors.append(('goldenrod', day_formated, 'warning', yellow_status[yellow_status['to'] == day_str]['alarm name'].unique().tolist()))
        elif day_str in grey_status_days:
            status_colors.append(('grey', day_formated, 'unknown', grey_status[grey_status['to'] == day_str]['alarm name'].unique().tolist()))
        else:
            status_colors.append(('green', day_formated, 'ok', []))
    statuses = status_colors
    
    # Add single stacked bar for status
    for color, d, status, alarms in status_colors:
        fig.add_trace(go.Bar(
            y=['Status'],
            x=[1],  # Equal segments
            marker_color=color,
            orientation='h',
            width=0.5,
            hoverinfo='text',
            hovertext=f"Day: {d}<br>Status: {status}"
        ), row=1, col=1)
    
    # --- BOTTOM PLOT (ALARMS) ---
    alarm_colors = {
        'bandwidth decreased from multiple': ('darkred','critical'),
        'ASN path anomalies per site': ('goldenrod', 'significant'),
        'firewall issue': ('grey', 'moderate'),
        'source cannot reach any': ('grey', 'moderate'),
        'complete packet loss': ('grey', 'moderate')
    }
    
    pivot_df = graphData.pivot_table(
        index='alarm name',
        columns='day',
        values='count',
        aggfunc='sum',
        fill_value=0
    ).reindex(columns=days, fill_value=0)
    alarm_names = graphData['alarm name'].unique()
    for alarm in alarm_names:
        for i, date in enumerate(days):
            count = pivot_df.loc[alarm, date]
            color, influence = 'green', 'None'
            if count >= 1:
                if alarm in alarm_colors:
                    color, influence = alarm_colors[alarm][0], alarm_colors[alarm][1]
                else:
                    color, influence = '#5c7a51', 'insignificant' 
            
            fig.add_trace(go.Bar(
                y=[alarm],
                x=[1],  # Each segment has width 1
                orientation='h',
                marker=dict(color=color),
                name=date.strftime('%Y-%m-%d'),
                hoverinfo='text',
                hovertext=f"Alarm: {alarm}<br>Date: {date.strftime('%Y-%m-%d')}<br>Count: {count}<br>Impact: {influence}",
                width=0.8,
                base=i  # Position along the x-axis
            ), row=2, col=1)
    
    # --- LAYOUT CONFIGURATION ---
    fig.update_layout(
        barmode='stack',
        showlegend=False,
        height=350, 
        margin=dict(l=20, r=20, t=0, b=20),  # Left margin for y-axis labels
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(visible=False),  # Hide x-axis for top plot
        xaxis2=dict(
            title='Number of Alarms',
            tickvals=list(range(len(full_range_dates))),
            ticktext=[d.strftime('%d %b') for d in full_range_dates]
        ),
        yaxis=dict(
            fixedrange=True,
            tickfont=dict(size=10)
        ),
        yaxis2=dict(
            fixedrange=True,
            tickfont=dict(size=10)
        )
    )
    
    return fig
    
@dash.callback(
    [
        Output('alarms-table', 'children'),
        Output('filter-date', 'options'),
        Output('filter-ip', 'options'),
        Output('filter-group', 'options'),
        Output('filter-type', 'options'),
        Output("bar-graph", "figure"),
        Output("active-button-store", "data"),
        Output("num-alarms", "children"),
    ],
    [
        Input('filter-date', 'value'),
        Input('filter-ip', 'value'),
        Input('filter-group', 'value'),
        Input('filter-type', 'value'),
        Input("btn-alarm-type", "n_clicks"),
        Input("btn-alarm-name", "n_clicks"),
        Input("bar-graph", "figure")
    ],
    [
        State("alarms-data-compressed", "data"),
        State("active-button-store", "data"),
        State("all-dates", "data")
    ]
)
def update_alarms_table(date_filter, ip_filter, group_filter, type_filter, btn_type_clicks, btn_name_clicks, figure, df, active_btn, all_dates):
    """
    This function creates the table with all the alarms
    listed and updates it based on chosen filters.
    """
    print("Debugging update_alarms_table")
    
    df = pd.DataFrame(df)
    all_dates = eval(all_dates) # all dates to visualise graphs for all days of the week, not only those who had alarms
    if len(df) > 0:
        ctx = dash.callback_context
        print(f"ctx.triggered: {ctx.triggered}")
        date_options = [{'label': date, 'value': date} for date in df['to'].unique()]
        ip_options = [{'label': ip.lower(), 'value': ip} for ip in sorted(df['IP version'].dropna().unique())]
        group_options = [{'label': group, 'value': group} for group in sorted(df['alarm group'].unique())]
        type_options = [{'label': alarm_type, 'value': alarm_type} for alarm_type in sorted(df['alarm name'].unique())]
        pressed_btn = ctx.triggered[0]['prop_id'].split('.')[0]
        graph_type = {"btn-alarm-type": "alarm group", "btn-alarm-name": "alarm name"}
        if pressed_btn:
            # Apply filters
            if date_filter:
                df = df[df['to'].isin(date_filter)]
            if ip_filter:
                df = df[df['IP version'].isin(ip_filter)]
            if group_filter:
                df = df[df['alarm group'].isin(group_filter)]
            if type_filter:
                df = df[df['alarm name'].isin(type_filter)]
            if pressed_btn in graph_type.keys():
                active_btn = graph_type[pressed_btn]
            figure = create_bar_chart(df, all_dates, active_btn)
                
        
        df = df.sort_values(by='to', ascending=False)
        df.rename(columns={'to': "Date", 'alarm group':'Category', 'alarm name': 'Alarm', 'hosts': 'Hosts'}, inplace=True)
        element = dbc.Table.from_dataframe(
                                            df[['Date', 'Category', 'Alarm', 'Involved Site(s)', 'Details', 'Hosts', 'IP version']],
                                            id='alarms-site-table',
                                            striped=True,
                                            bordered=True,
                                            hover=True,
                                            style={  # General table styling
                                                'fontSize': '13px',  # Larger font size for all text
                                                "height": "100%",
                                                "overflowY": "auto",
                                                "width": "100%"
                                            },            
        )
        
    else:
        element = html.Div(html.H3('No data was found'), style={'textAlign': 'center'})
        date_options = []
        ip_options = []
        group_options = []
        type_options = []
    return (element,
            date_options,
            ip_options,
            group_options,
            type_options,
            figure,
            active_btn,
            str(len(df)))

@dash.callback(
    [
    Output("how-status-modal-report", "is_open"),
    Output("how-status-modal-body-report", "children"),
    ],
    [
        Input("how-status-modal-trigger", "n_clicks"),
        Input("close-how-status-modal-report", "n_clicks"),
    ],
    [State("how-status-modal-report", "is_open")],
    
)
def toggle_modal(n1, n2, is_open):
    """
    This function explains how the status of the site was defined.
    """
    if n1 or n2:
        if not is_open:
            catTable, statusExplainedTable = explainStatuses()
            data = dbc.Row([
                dbc.Col(children=[
                    html.H3('Category & Alarm types', className='status-title'),
                    html.Div(statusExplainedTable, className='how-status-table')
                ], lg=12, md=12, sm=12, className='page-cont pr-1 how-status-cont'),
                dbc.Col(children=[
                    html.H3('Status color rules', className='status-title'),
                    html.Div(catTable, className='how-status-table')
                ], lg=12, md=12, sm=12, className='page-cont how-status-cont')
            ], className='pt-1')
            return not is_open, data
        return not is_open, dash.no_update
    return is_open, dash.no_update

@dash.callback(
    [
        Output("dynamic-content-container", "children"),
        Output('alarm-name-display', 'children'),
        Output("alarm-storage", "data"),
        Output("alarm-content-section", "style")
    ],
    [
        Input('site', 'data'),
        Input({'type': 'alarm-link-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn-site', 'index': ALL}, 'n_clicks'),
        Input({'type': 'hosts-not-found-btn', 'index': ALL}, 'n_clicks'),
        Input("alarm-content-section", "style")
    ],
    [
        State('dynamic-content-container', 'children'),
        State('fromDay', 'data'),
        State('toDay', 'data'),
        State('now', 'data')
    ],
    prevent_initial_call=True
)
def update_dynamic_content(site, alarm_clicks, path_clicks, path_clicks_2, hosts_clicks, visibility, current_children, fromDay, toDay, now):
    """
    This function extracts the visualisation of the chosen alarm and shows it under the table with alarms.
    """
    print("Dynamic content callback")
    print(f"alarm_clicks: {alarm_clicks}")
    button_clicks = {'alarm-link-btn': alarm_clicks, 'path-anomaly-btn': path_clicks, 'path-anomaly-btn-site': path_clicks_2, 'hosts-not-found-btn': hosts_clicks}
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, {}, visibility

    # global site
    alarmsInst = Alarms()

    
    button_content_mapping = {'hosts-not-found-btn': "hosts not found",
                            #   'path-anomaly-btn': "ASN path anomalies",
                              'path-anomaly-btn-site': "ASN path anomalies per site"
                              }
    if len(ctx.triggered) == 1:
        print(f"ctx.triggered: {ctx.triggered}")
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if button_id:
            button_id = eval(button_id)
            if sum(button_clicks[button_id['type']]) == 0:
                return dash.no_update, dash.no_update, {}, visibility
            if button_id['type'] in button_content_mapping:
                event = button_content_mapping[button_id['type']]
            else:
                id, event = button_id['index'].split(', ')
            if event == 'hosts not found':
                toDay = now
            # else:
            #     date = toDay 
                
            frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
            pd_df = pivotFrames[event]
            if pd_df.empty:
                return dash.no_update, dash.no_update, {}, visibility
            
            else:
                visibility = {'visibility': 'visible'}
                
                #host not found visualisation
                # if event == 'hosts not found':
                #     histData = createDictionaryWithHistoricalData(pd_df)
                #     site_name, id = button_id['index'].split(', ')
                #     fig, test_types, hosts, site = create_heatmap(pd_df, site, fromDay.replace("T", " ").replace(".000Z", ""), toDay.replace("T", " ").replace(".000Z", ""))
                #     alarm = qrs.getAlarm(id)['source']
                #     return dcc.Graph(figure=fig, className="p-3"), event, alarm, visibility

                #ASN anomalies visualisation
                if 'ASN path anomalies' in event:
                    print(event)
                    print(button_id['index'])
                    params = {'id': None, 'src': None, 'dest': None, 'dt': None, 'site': None, 'date': None}
                    if event == "ASN path anomalies":
                        src, dest, dt = button_id['index'].split('*')
                        params['src'] = src
                        params['dest'] = dest
                        params['dt'] = dt
                    else:
                        site, dt, aId = button_id['index'].split('*')
                        params['site'] = site
                        params['date'] = dt
                        params['id'] = aId
                    print("PARAMS")
                    print(params)
                    figures, data = asnAnomalesPerSiteVisualisation(params)
                    return figures, event, data, visibility
                
                #other alarms visualisations
                else:
                    print(f"id: {id}, event: {event}")
                    alarm_cont = qrs.getAlarm(id)
                    if event in ["complete packet loss", "high packet loss", "high packet loss on multiple links", "firewall issue", "high delay from/to multiple sites", "high one/way delay", "high one-way delay"]:
                        alarmsInst = Alarms()
                        alrmContent = alarm_cont['source']
                        event = alarm_cont['event']
                        summary = dbc.Row([dbc.Row([
                                    html.H1(f"Summary", className="text-left"),
                                    html.Hr(className="my-2")]),
                                    dbc.Row([
                                        html.P(alarmsInst.buildSummary(alarm_cont), className='subtitle'),
                                    ], justify="start"),
                                    ], className='m-2 p-2')
                        kibana_row = html.Div(children=[summary, loss_delay_kibana(alrmContent, event)])
                        return kibana_row, event, alrmContent, visibility
                        
                    if event in ["bandwidth decreased", "bandwidth increased"]:
                        print('URL query:', id)
                        sitePairs = getSitePairs(alarm_cont)
                        alarmData = alarm_cont['source']
                        dateFrom, dateTo = hp.getPriorNhPeriod(alarmData['to'])
                        # print('Alarm\'s content:', alarmData)
                        pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]

                        data = alarmsInst.getOtherAlarms(
                                                        currEvent=alarm_cont['event'],
                                                        alarmEnd=alarmData['to'],
                                                        pivotFrames=pivotFrames,
                                                        site=alarmData['site'] if 'site' in alarmData.keys() else None,
                                                        src_site=alarmData['src_site'] if 'src_site' in alarmData.keys() else None,
                                                        dest_site=alarmData['dest_site'] if 'dest_site' in alarmData.keys() else None
                                                        )

                        otherAlarms = alarmsInst.formatOtherAlarms(data)

                        expand = True
                        alarmsIn48h = ''
                        if alarm_cont['event'] not in ['bandwidth decreased', 'bandwidth increased']:
                            expand = False
                            alarmsIn48h = dbc.Row([
                                            dbc.Row([
                                                    html.P(f'Site {alarmData["site"]} takes part in the following alarms in the period 24h prior \
                                                    and up to 24h after the current alarm end ({alarmData["to"]})', className='subtitle'),
                                                    html.B(otherAlarms, className='subtitle')
                                                ], className="boxwithshadow alarm-header pair-details g-0 p-3", justify="between", align="center"),
                                            ], style={"padding": "0.5% 1.5%"}, className='g-0')
                        return html.Div(bandwidth_increased_decreased(alarm_cont, alarmData, alarmsInst,alarmsIn48h, sitePairs, expand, '-2')), event, alarmData, visibility
        
    return html.Div(), "", {}, visibility

# this callback moves the user to the part of the page with the network measurements
dash.clientside_callback(
    """
    function(alarmClicks, pathClicks, hostsClicks, measurementsClick) {
        const ctx = dash_clientside.callback_context;

        if (ctx.triggered.length > 1) return dash_clientside.no_update;

        const triggered = ctx.triggered[0];
        console.log('Triggered by:', triggered.prop_id);

        const sumClicks = (arr) => Array.isArray(arr) ? arr.reduce((a, b) => a + (b || 0), 0) : 0;
        const totalAlarmClicks = sumClicks(alarmClicks);
        const totalPathClicks = sumClicks(pathClicks);
        const totalHostsClicks = sumClicks(hostsClicks);

        const isInitialLoad = triggered.prop_id === '.' || (
            totalAlarmClicks + totalPathClicks + totalHostsClicks + (measurementsClick || 0) === 0
        );

        // Skip scroll on initial load or when no buttons were actually clicked
        if (isInitialLoad) return dash_clientside.no_update;

        if (triggered.prop_id === 'view-measurements-btn.n_clicks') {
            setTimeout(() => {
                const element = document.getElementById('site-measurements');
                if (element) {
                    element.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            }, 300);
        } else {
            setTimeout(() => {
                const element = document.getElementById('alarm-content-section');
                if (element) {
                    const yOffset = -100;
                    const y = element.getBoundingClientRect().top + window.pageYOffset + yOffset;
                    window.scrollTo({top: y, behavior: 'smooth'});
                }
            }, 350);
        }

        return dash_clientside.no_update;
    }
    """,
    Output('scroll-trigger', 'children'),
    [
        Input({'type': 'alarm-link-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn-site', 'index': ALL}, 'n_clicks'),
        Input({'type': 'hosts-not-found-btn', 'index': ALL}, 'n_clicks'),
        Input('view-measurements-btn', 'n_clicks'),
    ],
    prevent_initial_call=True
)

@timer
@dash.callback(
    [
      Output({'type': 'collapse-p2', 'index': MATCH},  "is_open"),
      Output({'type': 'collapse-p2', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'collapse-button-p2', 'index': MATCH}, "n_clicks"),
      Input({'type': 'collapse-button-p2', 'index': MATCH}, "value"),
      Input('alarm-storage', 'data')
    ],
    [State({'type': 'collapse-p2', 'index': MATCH},  "is_open")],
)
def toggle_collapse_2(n, pair, alarm, is_open):
    """
    This callback opens details in dynamic content that is generated for paths site.
    """
    data = ''
    global chdf
    global posDf
    global baseline
    global altPaths
    if n:
      if is_open==False:
        data = pairDetails(pair, alarm,
                        chdf[chdf['pair']==pair],
                        baseline[baseline['pair']==pair],
                        altPaths[altPaths['pair']==pair],
                        posDf[posDf['pair']==pair],
                        pivotFrames,
                        alarmsInst)
      return [not is_open, data]
    return [is_open, data]

@dash.callback(
    [
      Output({'type': 'tp-collapse-2', 'index': MATCH},  "is_open"),
      Output({'type': 'tp-collapse-2', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'tp-collapse-button-2', 'index': MATCH}, "n_clicks"),
      Input({'type': 'tp-collapse-button-2', 'index': MATCH}, "value"),
      Input('alarm-store', 'data')
    ],
    [State({'type': 'tp-collapse-2', 'index': MATCH},  "is_open")],
)
def toggle_collapse_3(n, alarmData, alarm, is_open):
    """
    This callback opens details in dynamic content that is generated for throughput alarms
    (bandwidth increased/decreased).
    """
    data = ''
    dateFrom, dateTo = hp.getPriorNhPeriod(alarm['source']['to'])
    pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]
    if n:
        if is_open==False:
            data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'], pivotFrames)
        return [not is_open, data]
    if is_open==True:
        data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'], pivotFrames)
        return [is_open, data]
    return [is_open, data]



@timer
def buildGraphComponents(alarmData, dateFrom, dateTo, event, pivotFrames):

  df = getRawDataFromES(alarmData['src_site'], alarmData['dest_site'], alarmData['ipv6'], dateFrom, dateTo)
  df.loc[:, 'MBps'] = df['throughput'].apply(lambda x: round(x/1e+6, 2))

  data = alarmsInst.getOtherAlarms(currEvent=event, alarmEnd=dateTo, pivotFrames=pivotFrames,
                                   src_site=alarmData['src_site'], dest_site=alarmData['dest_site'])
  otherAlarms = alarmsInst.formatOtherAlarms(data)
  return throughput_graph_components(alarmData, df, otherAlarms)


@dash.callback(
    [Output("status-container", "style"),
     Output("toggle-alarms-button", "children")],
    [Input("toggle-alarms-button", "n_clicks")],
    [State("status-container", "style")]
)
def toggle_container(n_clicks, current_style):
    """
    This function expands status container and shows alarms which influenced the status.
    """
    if n_clicks and n_clicks % 2 == 1:  # Odd click - expand
        return {"height": "400px", "transition": "height 0.3s ease"}, "Hide Details"
    return {"height": "110px", "transition": "height 0.3s ease"}, "Show Details ⬇"


def generate_summary(dataFrom, dataTo, alarms):
    """
    This function creates the text with the summary
    """
    def find_frequent_alarm_pairs(alarms_df, top_n=2):
    # Get all alarm combinations per day
        daily_combinations = (
            alarms_df.sort_values('to')
            .groupby('to')['alarm name']
            .apply(lambda x: list(combinations(sorted(set(x)), 2)))  # Get all possible 2-alarm combinations
            .explode()  # Flatten the list of combinations
            .dropna()  # Remove days with only 1 alarm
        )
        
        # Count occurrences of each pair
        pair_counts = Counter(daily_combinations).most_common(top_n)
        
        # Format the results
        frequent_pairs = [
            {'pair': ' & '.join(pair), 'count': count} 
            for pair, count in pair_counts
        ]
        
        return frequent_pairs
    
    # print('GENERATE SUMMARY')
    df = pd.DataFrame(statuses, columns=['color', 'date', 'status', 'alarms'])
    df_exploded = df.explode('alarms')
    date_range = f"{dataFrom.strftime('%b %d')} to {dataTo.strftime('%b %d')}"
    
    category_dist = (alarms.groupby('alarm group')['alarm name']
                     .count()
                     .sort_values(ascending=False))
    alarms_pairs = find_frequent_alarm_pairs(alarms)
    if len(alarms_pairs) < 2:
        while len(alarms_pairs) < 2:
            alarms_pairs.append({'pair': None, 'count': 0})
    
    summary = html.Div([
        html.H4(f"Site Status Overview ({date_range}):"),
        html.Ul([
            html.Li([
                html.Strong("Critical (Red) days: "), 
                f"{len(df[df['status']=='critical'])} out of 7",
                html.Br(),
                html.Span(f"Triggered by: {set(df_exploded[df_exploded['status']=='critical']['alarms'].tolist())}")
            ]),
            html.Li([
                html.Strong("Warning (Yellow) days: "),
                f"{len(df[df['status']=='warning'])} out of 7",
                html.Br(),
                html.Span(f"Triggered by: {set(df_exploded[df_exploded['status']=='warning']['alarms'].tolist())}")
            ]),
            html.Li([
                html.Strong("Unknown (Grey) days: "),
                f"{len(df[df['status']=='unknown'])} out of 7",
                html.Br(),
                html.Span(f"Triggered by: {set(df_exploded[df_exploded['status']=='unknown']['alarms'].tolist())}")
            ])
        ]),
        html.Br(),
        html.H4("Weekly Alarm Distribution:"),
        html.Pre(category_dist.to_string()),  
        html.Br(),
        html.H4("Most Frequent Alarm Combinations:"),
        html.Ul([
            html.Li([
                html.Span(f"Pair 1: {alarms_pairs[0]['pair']} ({alarms_pairs[0]['count']} occurrences)"),
                html.Br()
            ]),
            html.Li([
                html.Span(f"Pair 2: {alarms_pairs[1]['pair']} ({alarms_pairs[1]['count']} occurrences)"),
                html.Br()])
        ])
    ])
    return summary

@dash.callback(
    [
      Output({'type': 'asn-collapse-site', 'index': MATCH},  "is_open"),
      Output({'type': 'asn-collapse-site', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'asn-collapse-button-site', 'index': MATCH}, "n_clicks"),
      Input({'type': 'asn-collapse-button-site', 'index': MATCH}, "value")
    ],
    [State({'type': 'asn-collapse-site', 'index': MATCH},  "is_open")],
)
def toggle_collapse(n, alarmData, is_open):
    ctx = dash.callback_context
    if ctx.triggered:
        if n:
            if is_open==False:
                source = alarmData['src']
                destination = alarmData['dest']
                date = alarmData['date']
                data = qrs.query_ASN_anomalies(source, destination, date)
                visuals = generate_graphs(data, source, destination, date)
                return [not is_open, visuals]
            else:
                return [not is_open, None]
        return [is_open, None]
    return dash.no_update