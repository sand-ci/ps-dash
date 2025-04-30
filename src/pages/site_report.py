#TODO: set up "firewall issue button"
#TODO: add country and overview to my summary, add button that switch you to measurement
#TODO: if len(hosts) > 6 then break into 2 column
#TODO: finish isolating functions and buttons 

#TODO: debug groupAlarms in Updater.py, number of alarms is counted not correct
#TODO: for 0 alarms

#TODO: substitute hosts with summary
#      Percentage of alarms in Infrastracture/Network/Others
#      

from datetime import datetime, timedelta
from functools import lru_cache

import numpy as np
import dash
import dash_bootstrap_components as dbc

from dash import Dash, dash_table, dcc, html
from dash.dependencies import Input, Output, State, ALL, MATCH
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
from utils.utils import defineStatus, explainStatuses, create_heatmap, createDictionaryWithHistoricalData, generate_plotly_heatmap_with_anomalies, extractAlarm, getSitePairs, getRawDataFromES
from utils.components import siteBoxPathChanged, siteMeasurements, toggleCollapse, pairDetails, loss_delay_kibana, bandwidth_increased_decreased, throughput_graph_components
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

site = None
alarmsInst = Alarms()
# def reformat_time(timestamp):
#         try:
#             timestamp_new = pd.to_datetime(timestamp).strftime("%d %b %Y")
#             return timestamp_new
#         except Exception as err:
#             print(err)
#             return timestamp

def layout(q=None, **other_unknown_query_strings):
    global site
    site = q
    
    if q is not None:
        print(f"Site name: {q}")
    else:
        return html.Div(className="boxwithshadowhidden", children=[
                        # header line with site name
                        html.H3("\tSITE-NAME", className="card header-line p-1",  
                                style={"background-color": "#00245a", "color": "white", "font-size": "25px"}),
                        html.H4("The site name is missing. It is impossible to generate a report on the site.", style={"padding-top": "3%", "padding-left": "3%"}),
                        ]
                        )
    pq = Parquet()
    
    now = datetime.now()
    fromDay = (now - timedelta(days=8)).replace(hour=0, minute=0, second=0, microsecond=0)
    fromDay = fromDay.strftime("%Y-%m-%d %H:%M:%S")  # "2024-02-20 00:00:00"

    # Calculate toDay (2 days ago at 23:59:59)
    toDay = (now - timedelta(days=1)).replace(hour=00, minute=00, second=00, microsecond=00000)
    toDay = toDay.strftime("%Y-%m-%d %H:%M:%S")  # "2024-02-26 23:59:59"
    
    # toDay = toDay - timedelta(days=2)
    global pivotFrames
    frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
    
    # alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    # alarmCnt = alarmCnt[alarmCnt['site'] == q]
    print(f"fromDay: {fromDay}, toDay: {toDay}")
    # print(type(frames))
    # print(type(pivotFrames))
    
    site_alarms = pd.DataFrame(columns=["to", "alarm group", "alarm name", "hosts", "IP version", "Details"])
    site_alarms_num = 0
    subcategories = qrs.getSubcategories()
    if 'path changed between sites' in frames:
        frames.pop("path changed between sites", None)
    if 'path changed' in frames:
        frames.pop("path changed", None)
    
        
    
    ########TODO: count the most frequent host
    alarms_with_hosts = {'destination cannot be reached from any':'hosts', 'destination cannot be reached from multiple':'hosts', 
                         'firewall issue': 'host',
                         'source cannot reach any':'hosts'}
    alarms_with_src_dest = ['complete packet loss', 'high packet loss']
    #check source dest for complete packet loss, high packet loss,
    all_hosts = []
    for frame in frames:
        df = frames[frame]
        site_df = df[df['tag'].apply(lambda x: q in x)]
        
        if not site_df.empty:
            if frame in alarms_with_hosts:
                print(frame)
                print("----------------")
                print(frames[frame].columns)
                for item in site_df[alarms_with_hosts[frame]]:
                    all_hosts.extend(
                        item.tolist() if isinstance(item, np.ndarray) 
                        else item if isinstance(item, list) 
                        else [str(item)]
                    )
            if frame in alarms_with_src_dest:
                print(frame)
                print("----------------")
                print(site_df[site_df['src_site'] == q])
                print(site_df[site_df['dest_site'] == q])
                for item in site_df[site_df['src_site'] == q]['src_host']:
                    all_hosts.extend([item])
                for item in site_df[site_df['dest_site'] == q]['dest_host']:
                    all_hosts.extend([item])
            # print(site_df.head(5))
            site_df['to'] = pd.to_datetime(site_df['to'], errors='coerce')
            
            # drop time
            site_df['to'] = site_df['to'].dt.normalize()  # or .dt.floor('D')
            site_df['to'] = site_df['to'].dt.strftime('%Y-%m-%d')
            if frame == "hosts not found":
                print(site_df['hosts_not_found'].head(5))
                site_df['hosts'] = None
                site_df['hosts list'] = None
                for i, row in site_df.iterrows():
                    s = set()
                    hosts_list = row['hosts_not_found'].values()
                    for item in hosts_list:
                        if item is not None:
                            if isinstance(item, np.ndarray):  # Handle numpy arrays
                                s.update(tuple(item))  # Convert to tuple first
                            elif isinstance(item, (list, set, tuple)):
                                s.update(item)
                            else:
                                s.add(item)
                    hosts_list = s
                    print("HOSTS LIST")
                    print(hosts_list)
                    all_hosts.extend(hosts_list)
                    site_df.at[i, 'hosts list'] = hosts_list
                    site_df.at[i, 'hosts'] = hosts_list
            site_df = alarmsInst.formatDfValues(site_df, frame, False, True)
            # TODO: put it in the formating to the Alarms.py to def formatDfValues()
            if 'hosts' in site_df.columns:
                print(site_df['hosts'].head(5))
                site_df['hosts'] = site_df['hosts'].apply(lambda x: html.Div([html.Div(item) for item in x.split('\n')]) if isinstance(x, str) else x)
            site_alarms_num += len(site_df)
            site_df.reset_index(drop=True, inplace=True)
            for i, alarm in site_df.iterrows():
                add_hosts = 'hosts'
                if 'host' in alarm:
                    add_hosts = 'host'
                # Create a new row dictionary
                new_row = {
                    # 'from': alarm.get('from', '"from" field not found'),
                    'to': alarm.get('to', '"to" field not found'),
                    'alarm group': subcategories[subcategories['event'] == frame]['category'].values[0],
                    'alarm name': frame,
                    'IP version': alarm.get('IP version', None),  # Added IP version if available
                    'hosts': alarm[add_hosts] if add_hosts in alarm else None,
                    'Details': alarm.get('alarm_link', None),  # Added Details if available
                }
                dest_site_fields = ['dest_site', 'dest_netsite', 'sites']  # ordered by priority if multiple exist
                destination_sites = None

                for field in dest_site_fields:
                    if field in alarm:
                        destination_sites = html.Div([html.Div(item) for item in alarm[field].split('\n')])
                        break

                new_row['Destination Site(s)'] = destination_sites
                site_alarms = pd.concat([site_alarms, pd.DataFrame([new_row])], ignore_index=True)
    # print("-----------HOST COUNT------------")
    # host_counts = Counter(all_hosts)
    # most_common_host = host_counts.most_common(1)[0] if host_counts else (None, 0)

    # print(f"Most frequent host: {most_common_host[0]} (appears {most_common_host[1]} times)")
    # flat_list = np.concatenate(hosts_count).tolist()
    # print(flat_list)
    print(f"Total alarms collected: {site_alarms_num}")
    # print(site_alarms)
    # get meta data for summary
    # create_horizontal_bar_chart(site_alarms, fromDay, toDay)
    # f = create_status_chart_explained(site_alarms, fromDay, toDay)
    # f.show()
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
    return html.Div(children = [
        html.Div(id='scroll-trigger', style={'display': 'none'}),
        dcc.Store(id='fromDay', data=fromDay),
        dcc.Store(id='toDay', data=toDay),
        dcc.Store(id='alarm-storage', data={}),
        dbc.Row([
            dbc.Row(
                dbc.Col([
                    # dashboard card begins
                    html.Div(className="boxwithshadowhidden", style={"background-color": "white"}, children=[
                        # header line with site name
                        html.H3(f"\t{q}", className="header-line p-2",  
                                style={"background-color": "#00245a", "color": "white", "font-size": "25px"}),
                        # first row
                        dbc.Row(children=[
                            dbc.Col([
                                html.Div(
                                    className="boxwithshadowhidden",
                                    id="status-container",
                                    
                                    children=[
                                        # Header with toggle button
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
                                            dbc.Col(html.H3("Daily Site Status", style={"padding-left": "3%", "padding-top": "3%", 'margin-bottom': '0px'})),
                                            dbc.Col(
                                                dbc.Button(
                                                        "Show Details ⬇",
                                                        id="toggle-alarms-button",
                                                        color="link",
                                                        style={"font-size": "0.8rem", "padding-top": "3%",}
                                                    ),
                                                )
                                                 ], style={'margin-bottom': '0px'}),
                                        # Status graph (always visible)
                                            
                                        html.Div(
                                            dcc.Graph(
                                                id="site-status-alarms",
                                                figure=create_status_chart_explained(site_alarms, fromDay, toDay),
                                                config={'displayModeBar': False},
                                                style={
                                                    'width': '100%',
                                                    'height': '350px'
                                                }
                                            ), style={'margin-top': '0px'}
                                        ),
                                        
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
                                    ]
                                )
                        ])], className="mb-1 pr-1 pl-1"),
                        
                        # second row of stats: number of alarms, graph with alarms types and general site information
                        dbc.Row([
                            dbc.Col(
                                html.Div(
                                    className="boxwithshadow page-cont p-2 h-100",
                                    children=[
                                        html.H3("Number of Alarms", style={"color": "white", "padding-left": "5%", "padding-top": "5%"}),
                                        html.H1(
                                            # f"{site_alarms_num if site_alarms_num > 0 else 0}", 
                                            id="num-alarms",
                                            style={
                                                "color": "white", 
                                                "font-size": "120px",
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
                                                                dcc.Graph(id="bar-graph", figure=create_bar_chart(site_alarms)),
                                                            ],
                                                            style={"height": "100%", "width": "100%"}
                                                        ),
                                                        color='#ffffff'
                                                    )
                                                ]
                                                ), width=5, style={"background-color": "#ffffff", "height": "100%"}),
                            
                            dbc.Col(
                                html.Div(className="boxwithshadow page-cont p-2 h-100", style={"background-color": "#ffffff", "align-content":"center"}, children=[
                                    # html.H3("Summary", style={"padding-top": "5%", "padding-left": "5%"}),
                                    dbc.Row(
                                        [
                                        dbc.Col(
                                            html.Div(
                                                [
                                                    html.H4(f"Country: \t{country}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    html.H4(f"CPUs: \t{cpus}", style={"padding-left": "10%", "pading-top": "5"}),
                                                    html.H4(f"CPU Cores: \t{cpu_cores}", style={"padding-left": "10%", "pading-top": "5"})
                                                    ],
                                                        style={"height": "100%", "display": "flex", "flex-direction": "column"}
                                                ),
                                                    width=6,
                                                    style={
                                                        "border-right": "1px solid #ddd",  # Thin vertical line
                                                        "padding-right": "20px"
                                                    }
                                        ),
                                        dbc.Col([
                                            html.H4(f"{site} hosts:"),
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
                                                            'width': '100%',
                                                            # 'mask-image': 'linear-gradient(to top, transparent, black)',
                                                            # 'mask-size': '100% 190px',
                                                            # 'mask-position': 'bottom',
                                                            # 'mask-repeat': 'no-repeat',
                                                            # 'padding-bottom': '70px',
                                                            # 'mask-composite': 'exclude'
                                                        }
                                            )
                                    ], width=6
                                    )
                                        
                                ], className="align-items-stretch",  # Makes columns equal height
                            ),
                            dbc.Button(
                                        "View Measurements →",
                                        id="view-measurements-btn",
                                        color="link",
                                        style={
                                            "margin-left": "80px",
                                            "margin-top": "15px",
                                            "width": "60%",
                                            # "border-radius": "5px"
                                        }
                                    )
                            
                            ]), width=4, style={"background-color": "#b9c4d4;", "height": "100%"})
                        ], className="my-3 pr-1 pl-1", style={"height": "300px"}),
                    
                        
                        
                        # third row with alarms list and summary
                        dbc.Row([
                            # Left Column (Site status + Problematic host)
                            dbc.Col([
                                
                                html.Div(
                                    className="boxwithshadow page-cont p-2 h-100",
                                    children=[
                                        html.H3("Summary", 
                                                style={
                                                    "padding-left": "5%", 
                                                    "padding-top": "5%"
                                                }),
                                    ], style={"height": "100%"}
                                )
                            ], width=3, style={
                                "display": "flex",
                                "flex-direction": "column",
                                "height": "600px"  # Fixed height for left column
                            }),
                            
                            # Right Column (Filters + Table)
                            dbc.Col([
                                html.Div(
                                    className="boxwithshadowhidden page-cont h-100",
                                    style={
                                            "height": "100%",  # Adjust based on filters height
                                            "display": "flex",
                                            "flex-direction": "column"
                                        },
                                    children=[
                                        html.H1(f"List of alarms", className="text-center pt-2"),
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
                                                    html.Div(  # Changed from html.Div to DataTable
                                                        id='alarms-table',
                                                    # columns=[{"name": col, "id": col} for col in site_alarms.columns],
                                                        style={
                                                            'height': '400px',  # Fixed height of full column lenght for the scrollable container
                                                            'overflow-y': 'scroll',  # Enable vertical scrolling
                                                            'border': '1px solid #ddd',  # Optional: Add a border for better visibility
                                                            'padding': '10px',  # Optional: Add padding inside the container
                                                            'width': '100%'
                                                        }
                                                    ),
                                                    style={"height": "100%"},
                                                    color='#00245A'
                                                # color='#00245A'
                                                )
                                            # ]
                                        ], style={"padding-left": "1%", "padding-right": "1%"})
                                        # ]),
                                    ]
                                )
                            ], width=9, style={"height": "600px"})
                        ], className="my-3 pr-1 pl-1", style={"height": "600px"})
                        ]),
                    ])                 
                )
        ], className="ml-1 mt-1 mr-0 mb-1"),
        # Hidden component to track URL changes
    
        
       dbc.Row([
            dbc.Row(
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
                                                                        className="card header-line p-2",
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
            )
       ], className="ml-1 mt-1 mr-0 mb-1"),
        html.Div(id='site-measurements',
                children=siteMeasurements(q, pq),
                style={'margin-top': "20px"},
                            ),
        html.Div(id="dummy-output", style={"display": "none"}),
        html.Div(
                id="site-status-explanation",
                className="boxwithshadow p-0 mt-3",
                children=[
                    # Your explanation content here
                ]
            )
    ], className="scroll-container ml-2")
    

def create_bar_chart(graphData, column_name='alarm group'):
    # Calculate the total counts for each event type
    # event_totals = graphData.groupby('event')['cnt'].transform('sum')
    # Calculate percentage for each site relative to the event total
    # graphData['percentage'] = (graphData['cnt'] / event_totals) * 100
    graphData= graphData.groupby(['to', f"{column_name}"]).size().reset_index(name='count')
    # graphData['percentage'] = graphData['count'].transform(lambda x: x / x.sum() * 100)
    graphData.rename(columns={'to':'day'}, inplace=True)
    # print(graphData.head(5))
    # print(graphData.columns)
    # Create the bar chart using percentage as the y-axis
    fig = px.bar(
        graphData, 
        x='day', 
        y='count', 
        color=f'{column_name}', 
        # labels={'count': 'Count', 't': '', f'{column_name}': 'Event Type'},
        barmode='stack',
        color_discrete_sequence=px.colors.qualitative.Prism
    )

    # Update layout parameters
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

def create_status_chart_explained(graphData, fromDay, toDay):
    # Convert input dates to datetime
    fromDay = pd.to_datetime(fromDay)
    toDay = pd.to_datetime(toDay)
    
    # Create figure with proper spacing
    fig = make_subplots(
        rows=2, 
        cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.15,
        row_heights=[0.2, 0.8]  # 20% for status bar, 80% for alarms
    )
    graphData = graphData.groupby(['to', "alarm name"]).size().reset_index(name='cnt')
    red_status_days, yellow_status_days, grey_status_days = defineStatus(graphData, "alarm name", 'to')
    days = sorted(pd.to_datetime(graphData['to'].unique()))
    graphData = graphData.rename(columns={'to': 'day'})
    graphData['day'] = pd.to_datetime(graphData['day'])
    
    all_days = []
    print(yellow_status_days)
    # --- TOP PLOT (STATUS BAR) ---
    status_colors = []
    for day in days:
        day_str = day.strftime('%Y-%m-%d')
        day_formated = day.strftime('%d %b')
        if day_str in red_status_days:
            status_colors.append(('darkred', day_formated, 'critical'))
        elif day_str in yellow_status_days:
            status_colors.append(('goldenrod', day_formated, 'warning'))
        elif day_str in grey_status_days:
            status_colors.append(('grey', day_formated, 'unknown'))
        else:
            status_colors.append(('green', day_formated, 'ok'))
    print(status_colors)
    print(days)
    # Add single stacked bar for status
    for color, d, status in status_colors:
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
        'ASN path anomalies': ('goldenrod', 'significant'),
        'firewall issue': ('grey', 'moderate'),
        'source cannot reach any': ('grey', 'moderate'),
        'complete packet loss': ('grey', 'moderate')
    }
    
    # Create pivot table
    pivot_df = graphData.pivot_table(
        index='alarm name',
        columns='day',
        values='cnt',
        aggfunc='sum',
        fill_value=0
    ).reindex(columns=days, fill_value=0)
    alarm_names = graphData['alarm name'].unique()
    # Add bars for each alarm type
    for alarm in alarm_names:
        for i, date in enumerate(days):
            count = pivot_df.loc[alarm, date]
            color, influence = 'green', 'None'
            if count >= 1:
                if alarm in alarm_colors:
                    color, influence = alarm_colors[alarm][0], alarm_colors[alarm][1]
                else:
                    color, influence = 'lightgrey', 'insignificant' 
            
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
    # for alarm in pivot_df.index:
    #     fig.add_trace(go.Bar(
    #         y=[alarm],
    #         x=pivot_df.loc[alarm].values,
    #         orientation='h',
    #         marker_color=[alarm_colors.get(alarm, 'lightgrey')]*len(days),
    #         name=alarm,
    #         hoverinfo='x+name',
    #         width=0.5
    #     ), row=2, col=1)
    
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
            tickvals=list(range(len(days))),
            ticktext=[d.strftime('%d %b') for d in days]
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

def create_status_chart(graphData):
    # Process data
    graphData = graphData.groupby(['to', "alarm name"]).size().reset_index(name='cnt')
    red_status_days, yellow_status_days, grey_status_days = defineStatus(graphData, "alarm name", 'to')
    days = sorted(pd.to_datetime(graphData['to'].unique()))

    # Create status mapping (1 = colored segment, 0 = no segment)
    status_data = []
    for day in days:
        day_str = day.strftime('%Y-%m-%d')
        day_formated = day.strftime('%d %b')
        if day_str in red_status_days:
            status_data.append(('darkred', 1, day_formated))
        elif day_str in yellow_status_days:
            status_data.append(('goldenrod', 1, day_formated))
        elif day_str in grey_status_days:
            status_data.append(('grey', 1, day_formated))
        else:
            status_data.append(('green', 1, day_formated))
    
    # Create horizontal stacked bar
    fig = go.Figure()
    
    # Add each day as a separate bar segment
    for i, (color, value, date) in enumerate(status_data):
        fig.add_trace(go.Bar(
            y=['Status'],  # Single row
            x=[value],    # Value (always 1)
            orientation='h',
            marker=dict(color=color),
            width=0.8,    # Bar thickness
            name=days[i].strftime('%Y-%m-%d'),  # Day as label
            hoverinfo='name',
            # xaxis=dict(visible=True),
            base=sum(x[1] for x in status_data[:i])  # Stacking position
        ))
    
    # Customize layout
    fig.update_xaxes(
    ticktext=[x[2] for x in status_data],
    tickvals=[x for x in range(1, len(status_data)+1)]
    )
    fig.update_yaxes(
        tickangle = 0,
        ticklabelposition="inside"
    )
    fig.update_layout(
        barmode='stack',
        xaxis=dict(visible=True),  # Hide x-axis
        yaxis=dict(visible=True),
        showlegend=False,
        height=25,  # Fixed height for status bar
        margin=dict(l=0, r=0, t=30, b=10),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    # fig.show()
    return fig
    
# def create_status_chart(graphData):
#     graphData= graphData.groupby(['to', f"alarm name"]).size().reset_index(name='cnt')
#     red_status_days, yellow_status_days, grey_status_days = defineStatus(graphData, "alarm name", 'to')
#     days = sorted(graphData['to'].unique())
#     colors = []
#     for day in days:
#         if day in red_status_days:
#             colors.append('darkred')
#         elif day in yellow_status_days:
#             colors.append('goldenrod')
#         elif day in grey_status_days:
#             colors.append('grey')
#         else:
#             colors.append('green')

#     # Create the figure
#     fig = go.Figure()
#     # Add dots (markers) for each date
#     fig.add_trace(go.Scatter(
#         x=days,
#         y=[1]*len(days),  # Constant y-value to make a straight line
#         mode='markers',
#         marker=dict(
#             color=colors,
#             size=16,
#             symbol='circle',
#             opacity=0.9
#         ),
#         name='Status'
#     ))
#     fig.update_xaxes(
#         tickangle = 45
#         )
#     # Customize layout
#     fig.update_layout(
#         title=dict(text='Daily Status Overview',
#                    font=dict(
#                        size=14
#                        )
#                    ),
#         margin=dict(l=0, r=0, t=25, b=0),
        
#         yaxis=dict(visible=False),  # Hide y-axis as it's just for visual spacing
#         showlegend=False
#     )
#     return fig

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
        # Input('alarms-data', 'data'),
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
    ]
)
def update_alarms_table(date_filter, ip_filter, group_filter, type_filter, btn_type_clicks, btn_name_clicks, figure, df, active_btn):
            
    print("Debugging update_alarms_table")
    
    df = pd.DataFrame(df)
    if len(df) > 0:
        ctx = dash.callback_context
        print(f"ctx.triggered: {ctx.triggered}")
        [{'prop_id': 'filter-date.value', 'value': ['04 Mar 2025 (~04:08)']}]
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
            figure = create_bar_chart(df, active_btn)
                
        
        df = df.sort_values(by='to', ascending=True)
        df.rename(columns={'to': "Date", 'alarm group':'Category', 'alarm name': 'Alarm', 'hosts': 'Hosts'}, inplace=True)
        element = dbc.Table.from_dataframe(
                                            df,
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

def generate_alarm_content(alarm=""):
    return html.Div([
        html.H4(f"{alarm}"),
        dbc.Col(),  # Your plotting logic
        dbc.Button("Close", id="close-btn", className="mt-3")
    ])

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
        Input({'type': 'alarm-link-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'hosts-not-found-btn', 'index': ALL}, 'n_clicks'),
        Input("alarm-content-section", "style")
    ],
    [
        # State('url', 'pathname'),
        State('dynamic-content-container', 'children'),
        State('fromDay', 'data'),
        State('toDay', 'data')
    ],
    prevent_initial_call=True
)
def update_dynamic_content(alarm_clicks, path_clicks, hosts_clicks, visibility, current_children, fromDay, toDay):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update
    
    global site
    print("Debugging update_dynamic_content")
    alarmsInst = Alarms()
    # fromDay, toDay = hp.defaultTimeRange(6)
    print(f"fromDay: {fromDay}, toDay: {toDay}")
    frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
    button_content_mapping = {'hosts-not-found-btn': "hosts not found",
                              'path-anomaly-btn': "ASN path anomalies"
                              }
    if len(ctx.triggered) == 1:
        print(f"ctx.triggered: {ctx.triggered}")
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        print(button_id)
        if button_id:
            button_id = eval(button_id)  # Convert string to dictlink-btn
            # print(button_id)
            if button_id['type'] in button_content_mapping:
                event = button_content_mapping[button_id['type']]
            else:
                id, event = button_id['index'].split(', ')
                
            pd_df = pivotFrames[event]
            # print("pivotFrame")
            print(pd_df)
            if pd_df.empty:
                return dash.no_update, dash.no_update, {}, visibility
            else:
                visibility = {'visibility': 'visible'}
                if event == 'hosts not found':
                    print("createDictionaryWithHistoricalData site report")
                    print('from')
                    print(pd_df.describe())
                    histData = createDictionaryWithHistoricalData(pd_df)
                    print('to')
                    print(histData)
                    
                    site_name, id = button_id['index'].split(', ')
                    fig, test_types, hosts, site = create_heatmap(pd_df, site, fromDay, toDay)
                    alarm = qrs.getAlarm(id)['source']
                    return dcc.Graph(figure=fig), event, alarm, visibility

                    
                if event == 'ASN path anomalies':
                    src, dest = button_id['index'].split('*')
                    # print(f"src: {src}, dest: {dest}")
                    data = qrs.query_ASN_anomalies(src, dest, dates=[f"{fromDay.replace(' ', 'T')}.000Z", f"{toDay.replace(' ', 'T')}.000Z"])
                    # print(data)
                    if len(data) > 0:
                        if len(data['ipv6'].unique()) == 2:
                            ipv6_figure = generate_plotly_heatmap_with_anomalies(data[data['ipv6'] == True])
                            ipv4_figure = generate_plotly_heatmap_with_anomalies(data[data['ipv6'] == False])
                            figures = [
                                dcc.Graph(figure=ipv4_figure, id="asn-sankey-ipv4"),
                                dcc.Graph(figure=ipv6_figure, id="asn-sankey-ipv6")
                            ]
                        else:
                            figure = generate_plotly_heatmap_with_anomalies(data)
                            figures = [dcc.Graph(figure=figure, id="asn-sankey-ipv4")]
                        # alarm = qrs.getAlarm(q)['source']
                        return html.Div(figures), event, data.to_dict(), visibility
                else:
                    print(f"id: {id}, event: {event}")
                    alarm_cont = qrs.getAlarm(id)
                    if event in ["complete packet loss", "high packet loss"]:
                        
                        print('URL query:', id)
                        print()
                        print('Alarm content:', alarm_cont)
                        alarmsInst = Alarms()
                        alrmContent = alarm_cont['source']
                        event = alarm_cont['event']
                        summary = dbc.Row([dbc.Row([
                                    html.H1(f"Summary", className="text-left"),
                                    html.Hr(className="my-2")]),
                                    dbc.Row([
                                        html.P(alarmsInst.buildSummary(alarm_cont), className='subtitle'),
                                    ], justify="start"),
                                    ])
                        kibana_row = html.Div(children=[summary, loss_delay_kibana(alrmContent, event)])
                        return kibana_row, event, alrmContent, visibility
                        
                    if event in ["bandwidth decreased", "bandwidth increased"]:
                        print("here I am")
                        print('URL query:', id)
                        sitePairs = getSitePairs(alarm_cont)
                        alarmData = alarm_cont['source']
                        dateFrom, dateTo = hp.getPriorNhPeriod(alarmData['to'])
                        print('Alarm\'s content:', alarmData)
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

                        print("Trying to build graph for bandwidth decreased/increased")
                        return html.Div(bandwidth_increased_decreased(alarm_cont, alarmData, alarmsInst,alarmsIn48h, sitePairs, expand, '-2')), event, alarmData, visibility


                
    return html.Div(), "", {}, visibility

dash.clientside_callback(
    """
    function(alarmClicks, pathClicks, hostsClicks, measurementsClick) {
        const ctx = dash_clientside.callback_context;
        // Only proceed if something was triggered
        if (ctx.triggered.length > 1) return dash_clientside.no_update;
        
        const triggered = ctx.triggered[0];
        console.log('Triggered by:', triggered.prop_id);

        // Check if measurements button was clicked
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
        } 
        // Handle pattern-matched buttons (original logic)
        else {
            const isInitialLoad = triggered.prop_id === '.' && triggered.value === null;
            if (!isInitialLoad) {
                setTimeout(() => {
                    const element = document.getElementById('alarm-content-section');
                    if (element) {
                        const yOffset = -100;
                        const y = element.getBoundingClientRect().top + window.pageYOffset + yOffset;
                        window.scrollTo({top: y, behavior: 'smooth'});
                    }
                }, 350);
            }
        }
        return dash_clientside.no_update;
    }
    """,
    Output('scroll-trigger', 'children'),
    [
        Input({'type': 'alarm-link-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn', 'index': ALL}, 'n_clicks'),
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
    data = ''
    global chdf
    global posDf
    global baseline
    global altPaths
    if n:
      if is_open==False:
        # chdf, posDf, baseline, altPaths = pq.readFile(f'{location}chdf.parquet'), pq.readFile(f'{location}posDf.parquet'), pq.readFile(f'{location}baseline.parquet'), pq.readFile(f'{location}altPaths.parquet')
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
  data = ''
  print("HERE")  
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


def create_horizontal_bar_chart(df, fromD, toD):
    print(f"fromD: {fromD}, toD: {toD}")
    df = df.groupby(['to', f"{'alarm name'}"]).size().reset_index(name='count')
    # graphData['percentage'] = graphData['count'].transform(lambda x: x / x.sum() * 100)
    df.rename(columns={'to':'day'}, inplace=True)
    df['day'] = pd.to_datetime(df['day'])
    
    # Create complete date range
    all_dates = pd.date_range(start=fromD, end= pd.to_datetime(toD)-timedelta(days=1))


    # Get all unique alarm names
    alarm_names = df['alarm name'].unique()
    status_colors = {
        'bandwidth decreased from multiple': ('darkred','critical'),
        'ASN path anomalies': ('goldenrod', 'significant'),
        'firewall issue': ('grey', 'moderate'),
        'source cannot reach any': ('grey', 'moderate'),
        'complete packet loss': ('grey', 'moderate')
    }
    # Create a pivot table with alarm names as rows and dates as columns
    pivot_df = df.pivot_table(
        index='alarm name',
        columns='day',
        values='count',
        aggfunc='sum',
        fill_value=0
    ).reindex(columns=all_dates, fill_value=0)
    
    # Create the figure
    fig = go.Figure()
    bar_height = max(20, 300 / max(1, len(alarm_names)))  # Ensures min height of 20px
    # Add a bar segment for each date for each alarm
    for alarm in alarm_names:
        for i, date in enumerate(all_dates):
            count = pivot_df.loc[alarm, date]
            color, influence = 'green', 'None'
            if count >= 1:
                if alarm in status_colors:
                    color, influence = status_colors[alarm][0], status_colors[alarm][1]
                else:
                    color, influence = 'lightgrey', 'insignificant' 
            
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
            ))
    
    # Customize layout
    fig.update_layout(
        # title=dict(text="Alarms affect on status"),
        barmode='stack',
        xaxis=dict(
            # visible=True,
            tickvals=list(range(len(all_dates))),
            ticktext=[date.strftime('%d %b') for date in all_dates],
            title='Days'
        ),
        height=40 + len(alarm_names) * bar_height,  # Dynamic height based on number of alarms
        margin=dict(l=150, r=20, t=30, b=20),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=False
    )
 
        
    fig.update_yaxes(ticklabelposition="inside", tickvals=list(range(len(alarm_names))), ticktext=alarm_names, automargin=True, fixedrange=True)
    # fig.show()
    return fig

@dash.callback(
    [Output("status-container", "style"),
     Output("toggle-alarms-button", "children")],
    [Input("toggle-alarms-button", "n_clicks")],
    [State("status-container", "style")]
)
def toggle_container(n_clicks, current_style):
    if n_clicks and n_clicks % 2 == 1:  # Odd click - expand
        return {"height": "400px", "transition": "height 0.3s ease"}, "Hide Details"
    return {"height": "110px", "transition": "height 0.3s ease"}, "Show Details ⬇"
# # @dash.callback(
# #     Output("site-status-explanation", "style"),
# #     Input("status-explanation-trigger", "n_clicks"),
# #     State("site-status-explanation", "style")
# # )
# # def toggle_explanation(n_clicks, current_style):
# #     if n_clicks and n_clicks > 0:
# #         if current_style.get("display") == "none":
# #             return {"display": "block"}
# #         else:
# #             return {"display": "none"}
# #     return current_style



# dash.clientside_callback(
#     """
#     function(alarmClicks, pathClicks, hostsClicks, measurementsClick) {
#         const ctx = dash_clientside.callback_context;
#         // Only proceed if something was triggered
#         if (ctx.triggered.length > 1) return dash_clientside.no_update;
        
#         const triggered = ctx.triggered[0];
#         console.log('Triggered by:', triggered.prop_id);

#         // Check if measurements button was clicked
#         if (triggered.prop_id === 'view-measurements-btn.n_clicks') {
#             setTimeout(() => {
#                 const element = document.getElementById('site-measurements');
#                 if (element) {
#                     element.scrollIntoView({
#                         behavior: 'smooth',
#                         block: 'start'
#                     });
#                 }
#             }, 300);
#         } 
#         // Handle pattern-matched buttons (original logic)
#         else {
#             const isInitialLoad = triggered.prop_id === '.' && triggered.value === null;
#             if (!isInitialLoad) {
#                 setTimeout(() => {
#                     const element = document.getElementById('alarm-content-section');
#                     if (element) {
#                         const yOffset = -100;
#                         const y = element.getBoundingClientRect().top + window.pageYOffset + yOffset;
#                         window.scrollTo({top: y, behavior: 'smooth'});
#                     }
#                 }, 350);
#             }
#         }
#         return dash_clientside.no_update;
#     }
#     """,
#     Output('scroll-trigger', 'children'),
#     [
#         Input({'type': 'alarm-link-btn', 'index': ALL}, 'n_clicks'),
#         Input({'type': 'path-anomaly-btn', 'index': ALL}, 'n_clicks'),
#         Input({'type': 'hosts-not-found-btn', 'index': ALL}, 'n_clicks'),
#         Input('view-measurements-btn', 'n_clicks'),
#     ],
#     prevent_initial_call=True
# )
# @dash.callback(
#     Output("site-status-explanation", "children"),  # This can be any output you're not using
#     Input("status-explanation-trigger", "n_clicks"),
#     prevent_initial_call=True
# )
# def scroll_to_explanation(n_clicks):
#     print(n_clicks)
#     ctx = dash.callback_context
#     print(f"ctx.triggered: {ctx.triggered}")
#     # ctx.triggered: [{'prop_id': 'status-explanation-trigger.n_clicks', 'value': 1}]
#     if n_clicks:
#         return dcc.Location(id="dummy-location", pathname="site_report/MWT2_IU#site-status-explanation")
#     return None