# #TODO: formatValues change date format
# #TODO: add option to callback

# #TODO: get data through week
# #TODO: graphs
# #TODO: add button which generates detailed information about alarm
# #TODO: add measurements
# #TODO: add graph like in Grafana

# from datetime import datetime
# from functools import lru_cache

# import numpy as np
# import dash
# import dash_bootstrap_components as dbc

# from dash import Dash, dash_table, dcc, html
# from dash.dependencies import Input, Output

# import plotly.graph_objects as go
# import plotly.express as px
# from plotly.subplots import make_subplots

# import pandas as pd
# from flask import request

# from model.Alarms import Alarms
# import utils.helpers as hp
# from utils.helpers import timer
# import model.queries as qrs
# from utils.parquet import Parquet



# def title(q=None):
#     return f"Report for {q}"



# def description(q=None):
#     return f"Weekly Site Overview for {q}"



# dash.register_page(
#     __name__,
#     path_template="/site_report/<q>",
#     title=title,
#     description=description,
# )

# # def layout(q=None, **other_unknown_query_strings):
# #     pq = Parquet()
# #     alarmsInst = Alarms()
# #     fromDay, toDay = hp.defaultTimeRange(7)
# #     frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
# #     alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
# #     alarmCnt = alarmCnt[alarmCnt['site'] == q]
# #     print(f"fromDay: {fromDay}, toDay: {toDay}")
# #     print(alarmCnt)
# #     return html.Div()


# def layout(q=None, **other_unknown_query_strings):
#     if q is not None:
#         print(f"Site name: {q}")
#     else:
#         return html.Div(className="boxwithshadowhidden", children=[
#                         # header line with site name
#                         html.H3("\tSITE-NAME", className="card header-line p-2",  
#                                 style={"background-color": "#00245a", "color": "white", "font-size": "25px"}),
#                         html.H4("The site name is missing. It is impossible to generate a report on the site.", style={"padding-top": "3%", "padding-left": "3%"}),
#                         ]
#                         )
#     pq = Parquet()
#     alarmsInst = Alarms()
#     fromDay, toDay = hp.defaultTimeRange(7)
#     frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
#     # alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
#     # alarmCnt = alarmCnt[alarmCnt['site'] == q]
#     print(f"fromDay: {fromDay}, toDay: {toDay}")
#     # print(type(frames))
#     # print(type(pivotFrames))
    
#     site_alarms = pd.DataFrame(columns=["from", "to", "alarm group", "alarm name", "hosts", "IP version", "Details"])
#     site_alarms_num = 0
#     subcategories = qrs.getSubcategories()
#     print(f"Subcategories:\n{subcategories}")

#     for frame in frames:
#         print(frame)
#         print("----------------")
#         df = frames[frame]
#         site_df = df[df['tag'].apply(lambda x: q in x)]
#         if not site_df.empty:
#             site_df = alarmsInst.formatDfValues(site_df, frame)
#             site_alarms_num += len(site_df)
#             site_df.reset_index(drop=True, inplace=True)
#             for i, alarm in site_df.iterrows():
#                 print("Alarm:")
#                 print(alarm)
                
#                 # Create a new row dictionary
#                 new_row = {
#                     'from': alarm.get('from', '"from" field not found'),
#                     'to': alarm.get('to', '"to" field not found'),
#                     'alarm group': subcategories[subcategories['event'] == frame]['category'].values[0],
#                     'alarm name': frame,
#                     'IP version': alarm.get('IP version', None),  # Added IP version if available
#                     'hosts': list(alarm['hosts']) if 'hosts' in alarm else None,
#                     'Details': alarm.get('details', None)  # Added Details if available
#                 }
                
#                 # Append the new row to site_alarms
#                 site_alarms = pd.concat([site_alarms, pd.DataFrame([new_row])], ignore_index=True)

#     print(f"Total alarms collected: {site_alarms_num}")
#     print(site_alarms)
    
    
#     return html.Div([
#         dbc.Row([
#             dbc.Row(
#                 dbc.Col([
#                     # dashboard card begins
#                     html.Div(className="boxwithshadowhidden", children=[
#                         # header line with site name
#                         html.H3(f"\t{q}", className="card header-line p-2",  
#                                 style={"background-color": "#00245a", "color": "white", "font-size": "25px"}),
#                         # first row of stats
#                         dbc.Row([
#                             dbc.Col(
#                                 html.Div(
#                                     className="boxwithshadow page-cont p-2 h-100",
#                                     children=[
#                                         html.H3("Number of Alarms", style={"color": "white", "padding-left": "5%", "padding-top": "5%"}),
#                                         html.H1(
#                                             f"{site_alarms_num if site_alarms_num > 0 else 0}", 
#                                             id="num-alarms",
#                                             style={
#                                                 "color": "white", 
#                                                 "font-size": "120px",
#                                                 "display": "flex",
#                                                 "justify-content": "center",
#                                                 "align-items": "center",
#                                                 "height": "100%",
#                                                 "margin": "0",
#                                                 "padding-bottom": "20px"
#                                             }
#                                         )
#                                     ],
#                                     style={
#                                         "background-color": "#00245a",
#                                         "display": "flex",
#                                         "flex-direction": "column",
#                                     }
#                                 ),
#                                 width=3
#                             ),
                            
#                             dbc.Col(html.Div(className="boxwithshadowhidden p-2 h-100", children=[
#                                 dcc.Graph(id="type-of-alarms", style={"height": "100%", "width": "100%"})
#                             ]), width=5, style={"background-color": "#b9c4d4", "height": "100%"}),
                            
#                             dbc.Col(html.Div(className="boxwithshadow page-cont p-2 h-100", children=[
#                                 html.H5("Summary", style={"padding-top": "3%", "padding-left": "3%"}),
#                                 html.P(" ", id="summary-text")
#                             ]), width=4, style={"background-color": "#b9c4d4", "height": "100%"})
#                         ], className="my-3 pr-1 pl-1", style={"height": "300px"}),
                        
#                         # Second row with fixed height
#                         dbc.Row([
#                             # Left Column (fixed height sections)
#                             dbc.Col([
#                                 html.Div(
#                                     className="boxwithshadowhidden page-cont h-100",
#                                     children=[
#                                         html.H5("Site status throughout the week", style={"padding-top": "3%", "padding-left": "3%"}),
#                                         dcc.Graph(id="site-status", style={"height": "calc(100% - 50px)"})
#                                     ],
#                                     style={
#                                         "height": "65%",
#                                         "margin-bottom": "1rem",
#                                         "display": "flex",
#                                         "flex-direction": "column"
#                                     }
#                                 ),
#                                 html.Div(
#                                     className="boxwithshadow page-cont p-3 h-100",
#                                     children=[
#                                         html.H5("Most problematic host"),
#                                         html.H4("host1.domain.com", id="most-problematic-host"),
#                                         dbc.Button("HOSTS OVERVIEW", color="primary", className="mt-2")
#                                     ],
#                                     style={"height": "35%"}
#                                 )
#                             ], width=3, style={
#                                 "display": "flex",
#                                 "flex-direction": "column",
#                                 "height": "600px"  # Fixed height for left column
#                             }),
                            
#                             # Right Column (scrollable table)
#                             dbc.Col([
#                                 html.Div(
#                                     className="boxwithshadowhidden page-cont h-100",
#                                     style={
#                                         "height": "100%",
#                                         "display": "flex",
#                                         "flex-direction": "column"
#                                     },
#                                     children=[
#                                         html.H1(f"List of alarms", className="text-center pt-2"),
#                                         html.H5("Filters", style={"padding-top": "1%"}, className="pl-1"),
#                                         dbc.Row([
#                                             dbc.Col(dcc.Dropdown(id='filter-date', placeholder="Date"), width=3),
#                                             dbc.Col(dcc.Dropdown(id='filter-ip', placeholder="IP version"), width=3),
#                                             dbc.Col(dcc.Dropdown(id='filter-group', placeholder="Alarm group"), width=3),
#                                             dbc.Col(dcc.Dropdown(id='filter-type', placeholder="Alarm name"), width=3)
#                                         ], className="p-1 mb-2"),
#                                         html.Div(
#                                             style={
#                                                 "flex": "1",
#                                                 "overflow": "hidden",
#                                                 "display": "flex",
#                                                 "flex-direction": "column"
#                                             },
#                                             children=[
#                                                 dcc.Store(id='alarms-data', data=site_alarms.to_dict('records')),
#                                                 dcc.Loading(
#                                                     dash_table.DataTable(
#                                                         id='alarms-table',
#                                                         columns=[{"name": col, "id": col} for col in site_alarms.columns],
#                                                         style_table={
#                                                             "height": "100%",
#                                                             "overflowY": "auto",
#                                                             "width": "100%"
#                                                         },
#                                                         style_cell={
#                                                             'fontSize': '13px',
#                                                             'textAlign': 'left',
#                                                             'padding': '8px',
#                                                             'whiteSpace': 'normal'
#                                                         },
#                                                         style_header={
#                                                             'backgroundColor': '#00245A',
#                                                             'color': 'white',
#                                                             'fontWeight': 'bold',
#                                                             'position': 'sticky',
#                                                             'top': 0
#                                                         },
#                                                         page_size=20
#                                                     ),
#                                                     style={"height": "100%"},
#                                                     color='#00245A'
#                                                 )
#                                             ]
#                                         )
#                                     ]
#                                 )
#                             ], width=9, style={"height": "600px"})  # Same fixed height as left column
#                         ], className="my-3 pr-1 pl-1", style={"height": "600px"})  # Fixed height for entire row
#                     ]),
#                 ])
#             )
#         ], className="boxwithshadow page-cont", style={"background-color": "#b9c4d4", "padding": "1%"})
#     ])


# @dash.callback(
#     [
#         Output('alarms-table', 'children'),
#         Output('filter-date', 'options'),
#         Output('filter-ip', 'options'),
#         Output('filter-group', 'options'),
#         Output('filter-type', 'options')
#     ],
#     [
#         Input('alarms-data', 'data'),
#         Input('filter-date', 'value'),
#         Input('filter-ip', 'value'),
#         Input('filter-group', 'value'),
#         Input('filter-type', 'value')
#     ]
# )
# def update_alarms_table(df, date_filter, ip_filter, group_filter, type_filter):
#     def reformat_time(timestamp):
#         try:
#             timestamp_new = pd.to_datetime(timestamp).strftime("%d %b %Y (~%H:%M:%S)")
#             return timestamp_new
#         except Exception as err:
#             print(err)
#             return timestamp
            
#     print("Debugging update_alarms_table")
    
#     df = pd.DataFrame(df)
#     print(df)
#     if len(df) > 0:
#         ctx = dash.callback_context
#         print(f"ctx.triggered: {ctx.triggered}")
#         [{'prop_id': 'filter-date.value', 'value': ['04 Mar 2025 (~04:08)']}]
#         date_options = [{'label': date, 'value': date} for date in df['from'].unique()]
#         ip_options = [{'label': ip.lower(), 'value': ip} for ip in sorted(df['IP version'].dropna().unique())]
#         group_options = [{'label': group, 'value': group} for group in sorted(df['alarm group'].unique())]
#         type_options = [{'label': alarm_type, 'value': alarm_type} for alarm_type in sorted(df['alarm name'].unique())]
            
#         if ctx.triggered[0]['prop_id'].split('.')[0]:
#             # Apply filters
#             if date_filter:
#                 df = df[df['from'].isin(date_filter)]
#             if ip_filter:
#                 df = df[df['IP version'].isin(ip_filter)]
#             if group_filter:
#                 df = df[df['alarm group'].isin(group_filter)]
#             if type_filter:
#                 df = df[df['alarm name'].isin(type_filter)]
        
#         df["from"] = df['from'].apply(reformat_time)
#         df["to"] = df['to'].apply(reformat_time)
#         element = dbc.Table.from_dataframe(
#                                             df,
#                                             id='alarms-site-table',
#                                             striped=True,
#                                             bordered=True,
#                                             hover=True,
#                                             style={  # General table styling
#                                                 'fontSize': '13px',  # Larger font size for all text
                                                
#                                             },
                                            
                                        
#         )
        
#     else:
#         element = html.Div(html.H3('No data was found'), style={'textAlign': 'center'})
#     return (element,
#             date_options,
#             ip_options,
#             group_options,
#             type_options)
#     # return (
#         # element,
#         # date_options,
#         # ip_options,
#         # group_options,
#         # type_options
#     # )
#     # print(f"n_clicks: {n_clicks}")
#     # print(f"start_date: {start_date}")
#     # print(f"end_date: {end_date}")
#     # print(f"sites: {sites}")
#     # print(f"all: {all}")
#     # print(f"sitesState: {sitesState}")
#     # ctx = dash.callback_context
#     # # print(f"ctx.triggered: {ctx.triggered}")
#     # if not ctx.triggered or ctx.triggered[0]['prop_id'].split('.')[0] == 'search-button-2':
#     #     if start_date and end_date:
#     #         print("Start and end dates are specified")
#     #         start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']
#     #     else: 
#     #         print("Start and end dates are not specified")
#     #         start_date, end_date = hp.defaultTimeRange(2)
#     #     alarmsInst = Alarms()
#     #     print(f"start_date: {start_date}")
#     #     print(f"end_date: {end_date}")
#     #     framesAllAlarms, pivotFramesAllAlarms = alarmsInst.loadData(start_date, end_date)
        
#     #     # print(f"pivotFramesAllAlarms: {pivotFramesAllAlarms}")
#     #     df, pivotFrame = framesAllAlarms['hosts not found'], pivotFramesAllAlarms['hosts not found']
#     #     # print(f"pivotFrame: {pivotFrame}")
#     #     scntdf = pd.DataFrame()
#     #     df['event'] = 'hosts not found'
#     #     scntdf = pd.concat([scntdf, df])
#     #     graphData = scntdf
        
#     #     if (sitesState is not None and len(sitesState) > 0):
#     #         graphData = graphData[graphData['site'].isin(sitesState)]

#     #     sites_dropdown_items = []
#     #     for s in sorted(scntdf['site'].unique()):
#     #         if s:
#     #             sites_dropdown_items.append({"label": s.upper(), "value": s.upper()})

#     #     dataTables = []
#     #     df = pivotFrame
#     #     # filtering the sites selected in search field
#     #     if 'site' in df.columns:
#     #         df = df[df['site'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
#     #     elif 'tag' in df.columns:
#     #         df = df[df['tag'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
            
#     #     if len(df) > 0:
#     #         dataTables.append(generate_table(df, alarmsInst, start_date, end_date))
#     #     # print(pivotFrame)
#     #     # print("DaraTables:")
#     #     # print(dataTables)
#     #     def createDictionaryWithHistoricalData(dframe):
#     #         site_dict = dframe.groupby('site').apply(
#     #             lambda group: [
#     #                 (row['from'], row['to'], row['hosts_not_found'])
#     #                 for _, row in group.iterrows()
#     #             ]
#     #         ).to_dict()
#     #         return site_dict
#     #     historicalData = createDictionaryWithHistoricalData(pivotFrame)
#     #     dataTables = html.Div(
#     #         dataTables
#     #         # dcc.Store(id='alarm-store', data=historicalData),
#     #         )
#     #     # histData = html.Div(
#     #     #     dcc.Store(id='historical-data', data=historicalData)
#     #     #     )
#     #     return [sites_dropdown_items, dataTables, historicalData]

#     # else:
#     #     raise dash.exceptions.PreventUpdate
    
    