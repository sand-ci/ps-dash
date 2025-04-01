#TODO: count statuses throughout the week
#TODO: i guess i have fixed it, I changed something in hosts not found Alarms.py and now I broke home page list and the webpage

#TODO: debug groupAlarms in Updater.py, number of alarms is counted not correct
#TODO: graphs
#TODO: add button which generates detailed information about alarm
#TODO: add measurements
#TODO: add graph like in Grafana

from datetime import datetime
from functools import lru_cache

import numpy as np
import dash
import dash_bootstrap_components as dbc

from dash import Dash, dash_table, dcc, html
from dash.dependencies import Input, Output, State, ALL

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
from pages.home import generate_status_table


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

# def layout(q=None, **other_unknown_query_strings):
#     pq = Parquet()
#     alarmsInst = Alarms()
#     fromDay, toDay = hp.defaultTimeRange(7)
#     frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
#     alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
#     alarmCnt = alarmCnt[alarmCnt['site'] == q]
#     print(f"fromDay: {fromDay}, toDay: {toDay}")
#     print(alarmCnt)
#     return html.Div()
def reformat_time(timestamp):
        try:
            timestamp_new = pd.to_datetime(timestamp).strftime("%d %b %Y")
            return timestamp_new
        except Exception as err:
            print(err)
            return timestamp

def layout(q=None, **other_unknown_query_strings):
    if q is not None:
        print(f"Site name: {q}")
    else:
        return html.Div(className="boxwithshadowhidden", children=[
                        # header line with site name
                        html.H3("\tSITE-NAME", className="card header-line p-2",  
                                style={"background-color": "#00245a", "color": "white", "font-size": "25px"}),
                        html.H4("The site name is missing. It is impossible to generate a report on the site.", style={"padding-top": "3%", "padding-left": "3%"}),
                        ]
                        )
    pq = Parquet()
    alarmsInst = Alarms()
    fromDay, toDay = hp.defaultTimeRange(7)
    frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
    # alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    # alarmCnt = alarmCnt[alarmCnt['site'] == q]
    print(f"fromDay: {fromDay}, toDay: {toDay}")
    # print(type(frames))
    # print(type(pivotFrames))
    
    site_alarms = pd.DataFrame(columns=["to", "alarm group", "alarm name", "hosts", "IP version", "Details"])
    site_alarms_num = 0
    subcategories = qrs.getSubcategories()

    for frame in frames:
        print(frame)
        print("----------------")
        df = frames[frame]
        # print(df.columns)
        site_df = df[df['tag'].apply(lambda x: q in x)]
        if not site_df.empty:
            print(site_df.head(5))
            site_df["to"] = site_df['to'].apply(reformat_time)
            if frame == "hosts not found":
                print(site_df['hosts_not_found'].head(5))
                # site_df['hosts'] = site_df['hosts_not_found'].apply(
                #                                                 lambda x:[item for item in x.values() if item is not None]
                #                                             )
                # for i, row in site_df.iterrows():
                #     s = set()
                #     hosts_list = row['hosts_not_found']
                #     if hosts_list is not None:\
                #         s.update(hosts_list)
                #     row['hosts'] = list(s)
                #     print(row)
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
                    hosts_list = list(s)
                    print("HOSTS LIST")
                    print(hosts_list)
                    site_df.at[i, 'hosts list'] = hosts_list
                    # string_representation = '\n'.join(hosts_list).replace('-', '\u2011')
                    site_df.at[i, 'hosts'] = hosts_list

                    
                print(site_df.columns)
                        
                # site_df['hosts'] = site_df['hosts_not_found'].apply(
                #             lambda x: [item for item in x.values() if item is not None]
                #         )
                

                # site_df['hosts'] = site_df['hosts_not_found']
                
                # site_df = alarmsInst.formatDfValues(site_df, frame, True)
                # site_df.rename(columns={'alarm_button': "alarm_link"}, inplace=True)
            site_df = alarmsInst.formatDfValues(site_df, frame, False, True)
            site_alarms_num += len(site_df)
            site_df.reset_index(drop=True, inplace=True)
            for i, alarm in site_df.iterrows():
                
                # Create a new row dictionary
                new_row = {
                    # 'from': alarm.get('from', '"from" field not found'),
                    'to': alarm.get('to', '"to" field not found'),
                    'alarm group': subcategories[subcategories['event'] == frame]['category'].values[0],
                    'alarm name': frame,
                    'IP version': alarm.get('IP version', None),  # Added IP version if available
                    'hosts': str('\n'.join(alarm['hosts']).replace('-', '\u2011')) if 'hosts' in alarm else None,
                    'Details': alarm.get('alarm_link', None),  # Added Details if available
                }
                
                # Append the new row to site_alarms
                site_alarms = pd.concat([site_alarms, pd.DataFrame([new_row])], ignore_index=True)
        # print(site_df.head(5))

    print(f"Total alarms collected: {site_alarms_num}")
    # print(site_alarms)
    
    
    return html.Div([
        dbc.Row([
            dbc.Row(
                dbc.Col([
                    # dashboard card begins
                    html.Div(className="boxwithshadowhidden", children=[
                        # header line with site name
                        html.H3(f"\t{q}", className="card header-line p-2",  
                                style={"background-color": "#00245a", "color": "white", "font-size": "25px"}),
                        # first row of stats: number of alarms, graph with alarms types and summary text
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
                            dcc.Store(id='alarms-data', data=site_alarms.to_dict('records')), 
                            dbc.Col(html.Div(className="boxwithshadowhidden p-2 h-100", style={"background-color": "#ffffff"}, 
                                                children=[
                                                    html.Div([
                                                        dbc.ButtonGroup([
                                                            dbc.Button("Alarm Type", id="btn-alarm-type", n_clicks=0, 
                                                                    color="secondary", className="me-1"),
                                                            dbc.Button("Alarm Name", id="btn-alarm-name", n_clicks=0,
                                                                    color="secondary")
                                                        ], style={"margin-bottom": "10px"}),
                                                        dcc.Store(id="active-button-store", data="alarm_type"),
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
                            
                            dbc.Col(html.Div(className="boxwithshadow page-cont p-2 h-100", style={"background-color": "#b4dede"}, children=[
                                html.H5("Summary", style={"padding-top": "3%", "padding-left": "3%"}),
                                html.P(" ", id="summary-text")
                            ]), width=4, style={"background-color": "#b9c4d4;", "height": "100%"})
                        ], className="my-3 pr-1 pl-1", style={"height": "300px"}),
                        
                        dbc.Row([
                            # Left Column (Site status + Problematic host)
                            dbc.Col([
                                html.Div(
                                    className="boxwithshadowhidden page-cont h-100 p-2",
                                    children=[
                                        # html.H5("Site status throughout the week", style={"padding-top": "3%", "padding-left": "3%"}),
                                        # dcc.Graph(id="site-status", style={"height": "calc(100% - 50px)"}
                                        dcc.Graph(id="site-status", figure=create_status_chart(site_alarms), style={"height": "100%"}
                                    )
                                    ], style={
                                        "height": "65%",
                                        "margin-bottom": "1rem",
                                        "display": "flex",
                                        "flex-direction": "column"
                                    }
                                ),
                                html.Div(
                                    className="boxwithshadow page-cont p-2 h-100",
                                    children=[
                                        html.H5("Most problematic host"),
                                        html.H4("host1.domain.com", id="most-problematic-host"),
                                        dbc.Button("HOSTS OVERVIEW", color="primary", className="mt-2")
                                    ], style={"height": "35%"}
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
                                                    options=[{'label': date, 'value': date} for date in site_alarms['to'].unique()],
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
                                                    placeholder="Alarm group",
                                                    options=[{'label': group, 'value': group} for group in sorted(site_alarms['alarm group'].unique())],
                                                    multi=True,
                                                    clearable=True
                                                )),
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id='filter-type',
                                                    placeholder="Alarm name",
                                                    options=[{'label': alarm_name, 'value': alarm_name} for alarm_name in sorted(site_alarms['alarm name'].unique())],
                                                    multi=True,
                                                    clearable=True
                                                ))
                                        ], className="p-1 mb-2"),
                                        dbc.Col([
                                            # style={
                                            #     "flex": "1",
                                            #     "overflow": "hidden",
                                            #     "display": "flex",
                                            #     "flex-direction": "column"
                                            # },
                                            # children=[
                                                # dbc.Col([
                                                    # html.Hr(className="my-2"),
                                                    # html.Br(),
                                                 # Changed to 'records' format
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
        ], className="", style={"background-color": "#ffffff"}),
        # Hidden component to track URL changes
        dbc.Row([
            dcc.Location(id='url', refresh=False),
            # Container to display dynamic content
            html.Div(
                id="dynamic-content-container",
                className="boxwithshadow p-3 mt-3",
                style={"background-color": "#ffffff"}
            )
        ])
    ])

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
        legend_title_text='Alarm Type',
        legend=dict(
            x=0,
            y=1.35,
            orientation="h",
            xanchor='left',
            yanchor='top',
            font=dict(
                size=12,
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

def create_status_chart(graphData):
    graphData= graphData.groupby(['to', f"alarm name"]).size().reset_index(name='count')
    # print(graphData.head(5))
    # print(graphData.columns)
    graphData = graphData[graphData['alarm name'] != 'path changed between sites']

    red_status_days = graphData[(graphData['alarm name']=='bandwidth decreased from/to multiple sites')
            & (graphData['count']>0)]['to'].unique().tolist()

    yellow_status_days = graphData[(graphData['alarm name'].isin(['path changed', 'ASN path anomalies']))
                    & (graphData['count']>0)]['to'].unique().tolist()

    grey_status_days = graphData[(graphData['alarm name'].isin(['firewall issue', 'source cannot reach any', 'complete packet loss']))
                    & (graphData['count']>0)]['to'].unique().tolist()
    # status_dict= {'red': None, 'yellow': None, 'grey': None, 'green': None}
    days = sorted(graphData['to'].unique())
    # for day in days:
    #     if day in red_status_days:
    #         status_dict['red'] = day
    #     elif day in yellow_status_days:
    #         status_dict['yellow'] = day
    #     elif day in grey_status_days:
    #         status_dict['grey'] = day
    #     else:
    #         status_dict['green'] = day

    colors = []
    for day in days:
        if day in red_status_days:
            colors.append('darkred')
        elif day in yellow_status_days:
            colors.append('goldenrod')
        elif day in grey_status_days:
            colors.append('grey')
        else:
            colors.append('green')

    # Create the figure
    fig = go.Figure()
    # Add dots (markers) for each date
    fig.add_trace(go.Scatter(
        x=days,
        y=[1]*len(days),  # Constant y-value to make a straight line
        mode='markers',
        # marker=dict(
        #     color=colors,
        #     size=12,
            
        # ),
        marker=dict(
            color=colors,
            size=16,
            symbol='circle',
            opacity=0.9
        ),
        name='Status'
    ))

    # Customize layout
    fig.update_layout(
        title='Daily Status Overview',
        yaxis=dict(visible=False),  # Hide y-axis as it's just for visual spacing
        showlegend=False
    )

    # Show the figure
    
    # print(status_dict)
    # print(len(red_status_days))
    # print(len(yellow_status_days))
    # print(len(grey_status_days))
    
    # graphData= graphData.groupby(['to', f"{column_name}"]).size().reset_index(name='count')
    
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
        Output("num-alarms", "children")
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
        State("alarms-data", "data"),
        State("active-button-store", "data"),
    ]
)
def update_alarms_table(date_filter, ip_filter, group_filter, type_filter, btn_type_clicks, btn_name_clicks, figure, df, active_btn):
            
    print("Debugging update_alarms_table")
    
    df = pd.DataFrame(df)
    print(df)
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
                
        
        # df["from"] = df['from'].apply(reformat_time)
        df = df.sort_values(by='to', ascending=True)
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
        # element = dash_table.DataTable(
        #     df.to_dict('records'),
        #     [{"name": i.upper(), "id": i, "presentation": "markdown"} for i in df.columns],
        #     filter_action="native",
        #         filter_options={"case": "insensitive"},
        #         sort_action="native",
        #         is_focused=True,
        #         markdown_options={"html": True},
        #         page_size=8,
        #         style_cell={
        #         'padding': '10px',
        #         'font-size': '1.2em',
        #         'textAlign': 'center',
        #         'backgroundColor': '#ffffff',
        #         'border': '1px solid #ddd',
        #         },
        #         style_header={
        #         'backgroundColor': '#ffffff',
        #         'fontWeight': 'bold',
        #         'color': 'black',
        #         'border': '1px solid #ddd',
        #         },
        #         style_data={
        #         'height': 'auto',
        #         'overflowX': 'auto',
        #         },
                # style_table={
                # 'overflowY': 'auto',
                # 'overflowX': 'auto',
                # 'border': '1px solid #ddd',
                # 'borderRadius': '5px',
                # 'boxShadow': '0 2px 5px rgba(0,0,0,0.1)',
                # },
                # style_data_conditional=[
                # {
                #     'if': {'row_index': 'odd'},
                #     'backgroundColor': '#f7f7f7',
                # },
                # {
                #     'if': {'column_id': 'SITE NAME'},
                #     'textAlign': 'left !important',
                # }
                # ],
                # id='status-tbl')
        
        
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
    Output("dynamic-content-container", "children"),
    [
        Input({'type': 'alarm-link-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'path-anomaly-btn', 'index': ALL}, 'n_clicks'),
        Input({'type': 'hosts-not-found-btn', 'index': ALL}, 'n_clicks')
    ],
    [
        State('url', 'pathname'),
        State('dynamic-content-container', 'children')
    ]
)
def update_dynamic_content(alarm_clicks, path_clicks, hosts_clicks, current_path, current_children):
    print("Debugging update_dynamic_content")
    ctx = dash.callback_context

    if not ctx.triggered:
        return dash.no_update
    print(ctx.triggered)
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    button_id = eval(button_id)  # Convert string to dict
    
    print(button_id)
    # Determine which button was clicked
    if button_id['type'] == 'alarm-link-btn':
        alarm_id = button_id['index']
        content = generate_alarm_content(alarm_id)  # Replace with your logic
    elif button_id['type'] == 'path-anomaly-btn':
        src, dest = button_id['index'].split('_')
        # content = generate_alarm_content(src, dest)  # Replace with your logic
        content = generate_alarm_content(src)
    elif button_id['type'] == 'hosts-not-found-btn':
        site = button_id['index']
        content = generate_alarm_content(site)  # Replace with your logic
    else:
        return dash.no_update

    return content