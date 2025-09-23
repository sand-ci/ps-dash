
# import dash
# from dash import dcc, html
# import dash_bootstrap_components as dbc
# from dash.dependencies import Input, Output, MATCH, State, ALL
# import plotly.graph_objects as go
# import plotly.express as px
# import json
# import psconfig.api

# from datetime import datetime, timedelta, time, date
# import time as tm
# import re
# import requests
# from urllib3.exceptions import InsecureRequestWarning
# import urllib3


# import utils.helpers as hp
# from utils.helpers import timer
# from model.Alarms import Alarms
# from utils.parquet import Parquet
# import pandas as pd
# import numpy as np
# import os

# urllib3.disable_warnings()
# requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# def title(q=None):
#     return f"Hosts not found"

# def description(q=None):
#     return f"Visual represention on a hosts not found alarm"
# if not os.getenv("TEST_MODE"):
#     dash.register_page(
#         __name__,
#         path_template="/hosts_not_found/<q>",
#         title=title,
#         description=description,
#     )

# def parse_date(date_str):
#     """
#     Parse a date string using regex to handle both formats:
#     - '%Y-%m-%d %H:%M:%S.%fZ'
#     - '%Y-%m-%dT%H:%M:%S.%fZ'
#     """
#     pattern = r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}\.\d{3})Z"
#     match = re.match(pattern, date_str)

#     if match:
#         # Extract date and time components
#         date_part, time_part = match.groups()
#         # Combine into a datetime object
#         return datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S.%f")
#     else:
#         raise ValueError(f"Date string '{date_str}' does not match expected formats")



# def count_unique_not_found_hosts(df, category):
#     """
#     The function helps to count unique hosts among 
#     different categories to count the general statistics
#     about perfSonar tests availability in Elasticsearch.
#     """
#     missing_hosts = df.groupby("site")["hosts_not_found"].apply(lambda x: set(
#             host for d in x 
#             if isinstance(d, dict) and isinstance(d.get(category), (list, set, np.ndarray))
#             for host in (d[category].tolist() if isinstance(d[category], np.ndarray) else d[category])
#         ))
#     all_missing_hosts = set().union(*missing_hosts.dropna())
#     # print(all_missing_hosts)
#     return (
#         len(all_missing_hosts)
#     )
    
# def checkTestsForHost(host, mesh_config):
#     """
#     Classifies the host as belonging to one of
#     the three test groups (latency, trace and throughput).
#     """
#     try:
#         types = mesh_config.get_test_types(host)
#     except Exception:
#         return False, False
#     latency = any(test in ['latency', 'latencybg'] for test in types)
#     trace = 'trace' in types
#     throughput = any(test in ['throughput', 'rtt'] for test in types) # as rtt is now in ps_throughput
#     return host, latency, trace, throughput

# def createHostsTestsTypesGrid(hosts, mesh_config):
#     """
#     Creates a dataframe with a list of all hosts and whether
#     or not they are supposed to be tested in each group(latency and trace).
#     """
#     host_test_type = pd.DataFrame({
#     'host': list(hosts),
#     'owd': False,
#     'trace': False,
#     'throughput': False
#     })
#     host_test_type = host_test_type['host'].apply(
#         lambda host: pd.Series(checkTestsForHost(host, mesh_config))
#     )
#     host_test_type.columns = ['host', 'owd', 'trace', 'throughput']
#     return host_test_type

# def getExpectedHosts(pq=Parquet()):
#     """
#     This function gets expected hosts from psConfig
#     uploads it to parquet and update the parquet once for 24 hours.
#     """
#     parquet_path = 'parquet/hosts_not_found_expected.parquet'
#     freshness_threshold = 24 * 60 * 60 # 24 hours in seconds

#     if os.path.exists(parquet_path):
#         last_modified = os.path.getmtime(parquet_path)
#         age = tm.time() - last_modified
#         if age < freshness_threshold:
#             print("Hosts not found data is fresh. Skipping update.")
#             parquetData = pq.readFile(parquet_path)
#             print(parquetData)
#             return parquetData
#     print("Updating hosts not found data...")

#     mesh_url = "https://psconfig.aglt2.org/pub/config"
#     mesh_config = psconfig.api.PSConfig(mesh_url)

#     all_hosts = mesh_config.get_all_hosts()
#     ips = list(all_hosts)
#     df = createHostsTestsTypesGrid(ips, mesh_config)
#     expected_tests_types = {
#                             "owd": len(df[df["owd"] == True]),
#                             "trace": len(df[df["trace"] == True]),
#                             "throughput": len(df[df["throughput"] == True])
#                             }
#     df = pd.DataFrame.from_dict(expected_tests_types, orient='index', columns=['Count'])
#     df['to'] = datetime.now()
#     try:
#         pq.writeToFile(df, parquet_path)
#         print("Hosts not found data updated successfully.")
#         return df
#     except Exception as err:
#         print(err)
#         print(f"Problems with writing file {parquet_path}")


# def getHostsNotFoundDataFromParquet(data, path='parquet/frames/hosts_not_found.parquet', pq=Parquet()):
#     """
#     The function extracts data about the alarms hosts_not_found from Parquet.
#     """
#     tests_types_results = {'owd': None, 'throughput': None, 'trace': None}
#     try:
#         expected_tests_and_hosts = getExpectedHosts().to_dict()['Count']
#         all_missing_num = 0
#         for key in tests_types_results.keys():
#             missing_hosts = count_unique_not_found_hosts(data, key)
#             expected_hosts = expected_tests_and_hosts[key]
#             tests_types_results[key] = (missing_hosts, expected_hosts)
#             all_missing_num += missing_hosts
#         if all_missing_num == 0:
#             print("!!!!!!!!!!!!!!!!!!!!suspicious!!!!!!!!!!!!!!!!!!!!!!!!!")
#             print("Check parquet file existence and data format.")
#         return data, tests_types_results
#     except Exception as err:
#         print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#         print(err)
#         print("Check parquet file existence and data format.")
#         print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#         return data, tests_types_results


# def build_pie_chart(total, part, title):
#     """
#     The function builds pie chart with general
#     statistics about data availability in Elasticsearch.
#     """
#     percentage = (part / total) * 100

#     labels = ['Not Found', 'Found']
#     values = [part, total - part]

#     fig = px.pie(
#         names=labels,
#         values=values,
#         hole=0.4,
#         color=labels,
#         color_discrete_map={'Not Found': '#00245a', 'Found': '#69c4c4'} 
#     )

#     fig.update_layout(
#         title={
#             'text': title.upper(),  
#             'y': 0.95,  
#             'x': 0.05,
#             'xanchor': 'left',
#             'yanchor': 'top',
#             'font': {'size': 16, 'color': '#00245a'}  
#         },
#         showlegend=False,
#         template='plotly_white',
#         annotations=[
#             {
#                 'text': f'{percentage:.1f}%',
#                 'x': 0.5,
#                 'y': 0.5,
#                 'font_size': 20,
#                 'showarrow': False
#             },
#             {
#                 'text': f"{percentage:.1f}% of the expected host testing data is missing from Elasticsearch.",
#                 'x': 0.5, 
#                 'y': -0.1,
#                 'font_size': 10,
#                 'showarrow': False,
#                 # 'xref': 'paper',
#                 # 'yref': 'paper',
#                 'align': 'left'
#             }
#         ]
#     )

#     fig.update_traces(
#         marker=dict(
#             line=dict(color='#ffffff', width=2)  # Add a white border to the slices
#         )
#     )

#     return fig

# def get_stats_per_site(df):
#     """
#     The function creates the dataframe with the list of
#     all the sites and number of not found hosts and types of tests.
#     The data is then used 
#     """
    
#     def count_failed_hosts_and_tests(row):
#         """
#         The function put together all the records
#         about certain site and return summarized data.
#         """
#         hosts = set()
#         num_of_failed_tests_types = set()
#         # print(row)
#         for record in row:
#             d = dict()
#             for key in ['owd', 'throughput', 'trace']:
#                 if key in record.keys():
#                     if record[key] is not None: 
#                         num_of_failed_tests_types.update([key])
#                         hosts_failed = record[key]
#                         hosts.update(hosts_failed)  # Add hosts to the set
#                         d[key] = list(hosts_failed)
#         return (hosts, num_of_failed_tests_types, d)  # Return the count of unique hosts
#     # Apply the function to each row
#     # print(df)
#     try:
#         df = df.groupby("site", as_index=False).agg({
#             "hosts_not_found": lambda x: count_failed_hosts_and_tests(x),  # Apply custom function
#             "from": "min",  
#             "to": "max",  
#             "alarm_id": lambda x: "".join(x) if x.dtype == "object" else x.sum(),  # Handle string concatenation
#             "tag": "first"
#         })
#         # print(df)
#         # missing_hosts_num_by_site = df.groupby("site")["hosts_not_found"].apply(lambda x: count_failed_hosts_and_tests(x))
#         # print(type(missing_hosts_num_by_site))
#         # df = missing_hosts_num_by_site.to_frame()
#         # print(df.info())
#         # df.reset_index(inplace=True)
#         # print(df.info())
        
#         df["num_of_hosts_failed"] = df["hosts_not_found"].apply(lambda x: len(x[0]))
#         df["num_of_tests_types_failed"] = df["hosts_not_found"].apply(lambda x: len(x[1]))
#         df["hosts_not_found_org"] = df["hosts_not_found"].apply(lambda x: x[2])
#         df["hosts_failed"] = df["hosts_not_found"].apply(lambda x: list(x[0]))
#         df["tests_types_failed"] = df["hosts_not_found"].apply(lambda x: list(x[1]))

#         df["total_score"] = df["num_of_hosts_failed"] + df["num_of_tests_types_failed"]
#         sorted_sites = df.sort_values(by="total_score", ascending=False)
#         return sorted_sites[["site", "hosts_failed", "tests_types_failed", "from", "to", "alarm_id", "tag", "hosts_not_found_org"]]
#     except Exception as err:
#         print(err)
#         return err
    
# def generate_table(data, alarmsInsts, dateFrom=None, dateTo=None):
#     """
#     This function generates the table with host not found
#     alarm for all sites and with links to historical data plots.
#     """
#     # print(f"-----------------------\nDATA:\n{data}")
#     df_complete = get_stats_per_site(data)
#     print(f"Type dfr: {type(df_complete)}")
#     dfAddButton = alarmsInsts.formatDfValues(df_complete, 'hosts not found', True)
#     df = dfAddButton[["from", "to", "site", "hosts_failed", "tests_types_failed", "alarm_button"]]
#     # print(f"-----------------------\nDATA:\n{df_complete}") 
#     display_columns = df.keys()
#     # try:
#     #     print(f"========================\ndfAddButton\n{dfAddButton[['site', 'alarm_button']]}")
#     # except Exception as err:
#     #     print(err)
#     table = dfAddButton.to_dict('records')
#     # print('****************Table******************')
#     # print(table)
#     # print('****************df******************')
#     # print(df)
#     if len(df) > 0:
#         element = dbc.Table.from_dataframe(
#             df[display_columns],
#             id='results-table',
#             striped=True,
#             bordered=True,
#             hover=True
#         )
#         # print("Table was generated.")
#     else:
#         element = html.Div(html.H3('No data was found'), style={'textAlign': 'center'})
#     return element

# def layout(q=None, **other_unknown_query_strings):
#     # dateFrom, dateTo = hp.defaultTimeRange(1)
#     if q is not None:
#         default_search_value = q.split(',')
#         print(f"default_search_value: {default_search_value}")
#     rn = datetime.now()
#     start_date = (rn - timedelta(days=4)).date()
#     end_date = (rn - timedelta(days=2)).date()
#     now = [start_date, end_date]
#     initial_graph_data = None

#     return html.Div([
#         # Title and description
        
#         # Add the list of sites and search field
#         dbc.Row([
#             dbc.Col([
#                 dbc.Row([
#                     dbc.Col([
#                         html.H3([
#                             html.I(className="fas fa-search"),
#                             "Search the Sites"
#                         ], className="l-h-3"),
#                     ], align="center", className="text-left rounded-border-1 pl-1", md=12, xl=6),
#                     dbc.Col([
#                         dcc.DatePickerRange(
#                             id='date-picker-range',
#                             month_format='M-D-Y',
#                             min_date_allowed=now[1] - pd.Timedelta(days=29),
#                             max_date_allowed=now[1],
#                             initial_visible_month=now[0],
#                             start_date=now[0],
#                             end_date=now[1]
#                         )
#                     ], md=12, xl=6, className="mb-1 text-right")
#                 ], justify="around", align="center", className="flex-wrap"),
#                 dbc.Row([
#                     dbc.Col([
#                         dcc.Dropdown(multi=True, 
#                                      id='sites-dropdown-2', 
#                                      placeholder="Search for a site",
#                                      value=default_search_value),
#                     ]),
#                 ]),
#                 html.Br(),
#                 dbc.Row([
#                     dbc.Col([
#                         dbc.Button("Search", id="search-button-2", color="secondary", 
#                                 className="mlr-2", style={"width": "100%", "font-size": "1.5em"})
#                     ])
#                 ]),
#             ], lg=12, md=12, className="pl-1 pr-1 p-1"),
#         ], className="p-1 site g-0", justify="center", align="center"),
        
#         # Add the list of sites
#         dbc.Row([
#             dbc.Col([
#                 html.H1(f"List of alarms", className="text-center"),
#                 html.Hr(className="my-2"),
#                 html.Br(),
#                 dcc.Store(id='historical-data', data=initial_graph_data),
#                 dcc.Loading(
#                     html.Div(
#                         id='results-table-2',
#                         style={
#                             'height': '300px',  # Fixed height for the scrollable container
#                             'overflow-y': 'scroll',  # Enable vertical scrolling
#                             'border': '1px solid #ddd',  # Optional: Add a border for better visibility
#                             'padding': '10px'  # Optional: Add padding inside the container
#                         }
#                     ),
#                     style={'height': '0.5rem'}, color='#00245A'
#                 )
#             ], className="boxwithshadow page-cont p-2"),
#         ], className="g-0"),
#         html.Div([
#                 dbc.Row([
#                 dbc.Col([
#                     dcc.Dropdown(
#                         id='test-type-filter',
#                         placeholder="Select Test Type",
#                         multi=True  # Allow multiple selections
#                     )
#                 ], width=6),
#                 dbc.Col([
#                     dcc.Dropdown(
#                         id='hostname-filter',
#                         placeholder="Select Hostname",
#                         multi=True  # Allow multiple selections
#                     )
#                 ], width=6)
#             ]),
#             dcc.Store(id='selected-site-store'),
#             dcc.Graph(id='graph-placeholder')
#         ], className="boxwithshadow page-cont p-4 mt-4"),
#         ], className='', style={"padding": "0.5% 1% 0 1%"})
    
# @dash.callback(
#   [
#         Output("sites-dropdown-2", "options"),
#         Output('results-table-2', 'children'),
#         Output('historical-data', 'data'),
#     ],
#     [
#         Input('search-button-2', 'n_clicks'),
#         Input('date-picker-range', 'start_date'),
#         Input('date-picker-range', 'end_date'),
#         Input("sites-dropdown-2", "search_value"),
#         Input("sites-dropdown-2", "value")
#     ],
#     State("sites-dropdown-2", "value")
# )
# def update_output(n_clicks, start_date, end_date, sites, all, sitesState):
#     print(f"n_clicks: {n_clicks}")
#     print(f"start_date: {start_date}")
#     print(f"end_date: {end_date}")
#     print(f"sites: {sites}")
#     print(f"all: {all}")
#     print(f"sitesState: {sitesState}")
#     ctx = dash.callback_context
#     # print(f"ctx.triggered: {ctx.triggered}")
#     if not ctx.triggered or ctx.triggered[0]['prop_id'].split('.')[0] == 'search-button-2':
#         if start_date and end_date:
#             print("Start and end dates are specified")
#             rn = pd.to_datetime(end_date) + timedelta(days=2)
#             start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']
#         else: 
#             print("Start and end dates are not specified")
#             start_date, end_date = hp.defaultTimeRange(2)
#         alarmsInst = Alarms()
#         print(f"start_date: {start_date}")
#         print(f"end_date: {end_date}")
#         framesAllAlarms, pivotFramesAllAlarms = alarmsInst.loadData(start_date, f'{rn.date().strftime("%Y-%m-%d")}T23:59:59.000Z')
        
#         # print(f"pivotFramesAllAlarms: {pivotFramesAllAlarms}")
#         df, pivotFrame = framesAllAlarms['hosts not found'], pivotFramesAllAlarms['hosts not found']
#         # print(f"pivotFrame: {pivotFrame}")
#         scntdf = pd.DataFrame()
#         df['event'] = 'hosts not found'
#         scntdf = pd.concat([scntdf, df])
#         graphData = scntdf
        
#         if (sitesState is not None and len(sitesState) > 0):
#             graphData = graphData[graphData['site'].isin(sitesState)]

#         sites_dropdown_items = []
#         for s in sorted(scntdf['site'].unique()):
#             if s:
#                 sites_dropdown_items.append({"label": s.upper(), "value": s.upper()})

#         dataTables = []
#         df = pivotFrame
#         # filtering the sites selected in search field
#         if 'site' in df.columns:
#             df = df[df['site'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
#         elif 'tag' in df.columns:
#             df = df[df['tag'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
            
#         if len(df) > 0:
#             dataTables.append(generate_table(df, alarmsInst, start_date, end_date))
#         # print(pivotFrame)
#         # print("DaraTables:")
#         # print(dataTables)
#         def createDictionaryWithHistoricalData(dframe):
#             print("createDictionaryWithHistoricalData hosts not found...")
#             print('from')
#             print(dframe.describe())
#             site_dict = dframe.groupby('site').apply(
                
#                 lambda group: [
#                     (row['from'], row['to'], row['hosts_not_found'])
#                     for _, row in group.iterrows()
#                 ]
#             ).to_dict()
#             print('to')
#             print(site_dict)
#             return site_dict
#         # print(f"pivotFrame: {pivotFrame}")
#         historicalData = createDictionaryWithHistoricalData(pivotFrame)
#         dataTables = html.Div(
#             dataTables
#             # dcc.Store(id='alarm-store', data=historicalData),
#             )
#         # histData = html.Div(
#         #     dcc.Store(id='historical-data', data=historicalData)
#         #     )
#         return [sites_dropdown_items, dataTables, historicalData]

#     else:
#         raise dash.exceptions.PreventUpdate
    
# @dash.callback(
#     [   
#         Output('graph-placeholder', 'figure'),  # Output the updated graph
#         Output('test-type-filter', 'options'),
#         Output('hostname-filter', 'options'),
#         Output('selected-site-store', 'data'),
        
#     ],
#     [
#         Input({'type': 'generate-graph-button', 'index': dash.dependencies.ALL}, 'n_clicks'),
#         Input('historical-data', 'data'),  # Catch clicks on all buttons
#         Input("test-type-filter", "value"),
#         # Input("test-type-filter", "value"),
#         Input("hostname-filter", "value"),
#         # Input("hostname-filter", "value"),
#         Input('date-picker-range', 'start_date'),
#         Input('date-picker-range', 'end_date'),
#     ],
#     [
#         State('selected-site-store', 'data'),  # Retrieve the stored site
#     ],
#     prevent_initial_call=True
# )
# def update_graph(n_clicks, data=None, testTypeFilter=None, hostFilter = None, dateFrom=None, dateTo=None, siteData=None):
#     if data == None:
#         print("None data for graph given.")
#         return None
#     print(f"testTypeFilter: {testTypeFilter}")
#     print(f"hostFilter: {hostFilter}")
#     # print(f"all_tests_filters: {all_tests_filters}")
#     # print(f"all_hosts_filter: {all_hosts_filter}")
#     print(f"siteData: {siteData}")
#     triggered_id = dash.callback_context.triggered
#     # print(triggered_id)
#     if triggered_id:
#         if triggered_id[0]['prop_id'] in ['test-type-filter.value', 'hostname-filter.value']:
#             filter = triggered_id[0]['value']
#             print(f"Filter: {filter}")
#             site_index = siteData
#         elif triggered_id[0]['prop_id'] == 'historical-data.data':
#             print("Initial variant")
#             placeholder_figure = go.Figure()

#             # Add a centered annotation with the message
#             placeholder_figure.add_annotation(
#                 text="Your graph will appear here after clicking 'Generate Graph'.",
#                 xref="paper", yref="paper",
#                 x=0.5, y=0.5, showarrow=False,
#                 font=dict(size=16, color="gray"),
#                 align="center"
#             )

#             # Remove axes and grid
#             placeholder_figure.update_xaxes(visible=False)
#             placeholder_figure.update_yaxes(visible=False)
#             placeholder_figure.update_layout(
#                 plot_bgcolor="white",
#                 height=400  # Adjust height as needed
#             )

#             # graph_component = dcc.Graph(id="graph-placeholder", figure=)
#             return placeholder_figure, [], [], {}
#         else:
#             print("Else variant")
#             prop_id = triggered_id[0]['prop_id']
#             prop_id_json = prop_id.split('.n_clicks')[0]
#             parsed_id = json.loads(prop_id_json)
#             site_index = parsed_id['index']
#             # alarmsInst = Alarms()
#             # framesAllAlarms, pivotFramesAllAlarms = alarmsInst.loadData(start_date, end_date)
#             # df, pivotFrame = framesAllAlarms['hosts not found'], pivotFramesAllAlarms['hosts not found']
#             print(f"Button clicked: {site_index}")
            
#         def create_heatmap(site_dict, site, date_from, date_to, test_types=None, hostnames=None):
#             """
#             Create a Plotly heatmap for a specific site over a date range with optional filters.

#             Args:
#                 site_dict (dict): Dictionary with site data.
#                 site (str): The site to visualize.
#                 date_from (str): Start date in 'YYYY-MM-DD' format.
#                 date_to (str): End date in 'YYYY-MM-DD' format.
#                 test_types (list): List of selected test types to filter by.
#                 hostnames (list): List of selected hostnames to filter by.

#             Returns:
#                 plotly.graph_objects.Figure: The heatmap figure.
#             """
#             print("In create_heatmap function hosts_not_found...")
#             print(site_dict)
#             # Get the records for the specified site
#             records = site_dict.get(site, [])
#             print(records)
#             # print(f"Data records: {records}")
#             # Parse date_from and date_to
#             date_from = datetime.strptime(date_from, '%Y-%m-%d')
#             date_to = datetime.strptime(date_to, '%Y-%m-%d')
#             print(f"Building graph for date range {date_from} to {date_to}...")
#             # Generate all dates in the range
#             date_range = [date_from + timedelta(days=i) for i in range((date_to - date_from).days + 1)]
#             date_range_str = [date.strftime('%Y-%m-%d') for date in date_range]
#             print(f"Date range: {date_range_str}")
#             print("Extracting all hosts...")
#             # Extract all unique hostnames and test types
#             all_hostnames = set()
#             all_test_types = set()
#             for record in records:
#                 hosts_not_found = record[2]  # hosts_not_found dictionary
#                 for test_type, hosts in hosts_not_found.items():
#                     if hosts:  # Check if hosts list is not empty
#                         all_hostnames.update(hosts)
#                         all_test_types.add(test_type)
#             print("Applying filter....")
#             # Apply filters if provided
#             if test_types:
#                 all_test_types = set(test_types)
#             if hostnames:
#                 all_hostnames = set(hostnames)

#             # Create a list of all combinations (hostname + test type)
#             combinations = [f"({test}) {host}" for host in all_hostnames for test in all_test_types]

#             # Initialize a DataFrame to store the heatmap data
#             heatmap_data = pd.DataFrame(index=date_range_str, columns=combinations, dtype=int)

#             # Fill the DataFrame with 0 (absence) by default
#             heatmap_data.fillna(0, inplace=True)

#             # Mark presence of alarms with 1
#             for record in records:
#                 try:
#                     # Parse the dates using the regex-based function
#                     record_from = parse_date(record[0])
#                     record_to = parse_date(record[1])
#                 except ValueError as err:
#                     print(f"Error parsing date: {err}")

#                 # Iterate over each day in the record's date range
#                 current_date = record_from
#                 while current_date <= record_to:
#                     current_date_str = current_date.strftime('%Y-%m-%d')
#                     if current_date_str in heatmap_data.index:
#                         for test_type, hosts in hosts_not_found.items():
#                             if hosts and test_type in all_test_types:  # Apply test type filter
#                                 for host in hosts:
#                                     if host in all_hostnames:  # Apply hostname filter
#                                         combination = f"({test_type}) {host}"
#                                         if combination in heatmap_data.columns:
#                                             heatmap_data.at[current_date_str, combination] = 1
#                     current_date += timedelta(days=1)
#             # print(f"Heatmap data: {heatmap_data}")
#             case = 0
#             # print(f"Heatmap data unique values: {len(np.unique(heatmap_data.values))}")
#             if heatmap_data.shape[1] == 1 or len(np.unique(heatmap_data.values)) == 1:
#                 heatmap_data[' '] = 0 # turning 1D data in 2D
#                 case = 1 # corner cases for cases when there is only one value meaning all the test for the period failed to be found in Elasticsearch
#             #     print(f"Heatmap data: {heatmap_data}")
#             # print(f"Heatmap data values: {heatmap_data.T.values}")
#             # print(f"Heatmap data index: {heatmap_data.index}")
#             # print(f"Heatmap data columns: {heatmap_data.columns}")
#             # print("Heatmap data shape:", heatmap_data.shape)
#             colorscales = {0:[[0, '#69c4c4'], [1, 'grey']], 1:[[0, '#ffffff'], [1, 'grey']]}
#             colorbars = {1:dict(title="Data Availability", tickvals=[0, 1], ticktext=[' ', "Missed"]),
#                          0:dict(title="Data Availability", tickvals=[0, 1], ticktext=["Available", "Missed"])}
#             heatmap = go.Heatmap(
#                 z=heatmap_data.T.values,  # Transpose to have dates on the y-axis
#                 x=heatmap_data.index,  # Dates
#                 y=heatmap_data.columns,  # Host + Test Type combinations
#                 colorscale=colorscales[case],  # Custom colorscale
#                 colorbar=colorbars[case],
#                 hoverongaps=False,
#                 xgap=1,  # Add white borders between cells
#                 ygap=1,  # Add white borders between cells
#                 showscale=True
#             )

#             # Create the figure
#             fig = go.Figure(data=[heatmap])

#             # Update layout
#             fig.update_layout(
#                 title=f"Missing tests for {site} ({date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})",
#                 xaxis_title="Date",
#                 yaxis_title="Host + Test Type",
#                 font=dict(size=12)  # Set default font size
#             )

#             return fig, list(all_test_types), list(all_hostnames), site

#     print("Creating the heatmap....")
#     figure, allTestsFilter, allHostsFilter, selectedSite = create_heatmap(data, site_index, dateFrom, dateTo, testTypeFilter, hostFilter)
#     # print(f"testTypeFilter: {testTypeFilter}")
#     # print(f"hostFilter: {hostFilter}")
#     return [figure, allTestsFilter, allHostsFilter, selectedSite]

# def historicalDataGraph(dictionary):
#     df = pd.DataFrame(dictionary).T.reset_index()
#     df.rename(columns={"index": "date"}, inplace=True)

#     # Normalize the data (percentage change from the first day)
#     df_normalized = df.copy()
#     for col in ["owd", "throughput", "trace"]:
#         df_normalized[col] = df[col] - df[col].iloc[0]

#     df_melted = df_normalized.melt(id_vars=['date'], var_name='group', value_name='host_count')


#     # Create a Plotly line plot
#     fig = px.line(
#         df_melted,
#         x='date',
#         y='host_count',
#         color='group',
#         title='Number of Hosts Over Time by Group',
#         labels={'date': 'Date', 'host_count': 'Number of Hosts', 'group': 'Group'},
#         line_shape='linear'
#     )

#     # Show the plot
#     return fig

# @dash.callback(
#     [
#     Output("historical-data-modal", "is_open"),
#     Output("historical-data-modal-body", "children"),
#     ],
#     [
#         Input("gen-stats-historical-data-button", "n_clicks"),
#         Input("historical-data-for-graph", "data"),
#         Input("close-button", "n_clicks"),
#     ],
#     [State("historical-data-modal", "is_open")],
# )
# def historicalDataAvailabilty(button1, histData, button2, is_open):
#     print(f"In creating historical data graph")
#     print(button1)
#     print(histData)
#     print(button2)
#     print(is_open)
#     # return
#     if button1 or button2:
#         if not is_open:
#             graph = historicalDataGraph(histData)
#             data = dbc.Row([
#                         dcc.Graph(
#                         figure=graph,  
#                         id='graph-hsstorical-stats',
#                         className='cls-hist-data-stats'
#                     ),
#             ], className='pt-1')
#             return not is_open, data
#         return not is_open, dash.no_update
#     return is_open, dash.no_update
    
# @dash.callback(
#     [
#     Output("historical-data-owd-modal", "is_open"),
#     Output("historical-data-modal-owd-body", "children"),
#     ],
#     [
#         Input("owd-stats", "clickData"),
#         Input("throughput-stats", "clickData"),
#         # Input("trace-stats", "clickData"),
#         Input("historical-data-for-graph", "data"),
#         Input("close-button", "n_clicks"),
#     ],
#     [State("historical-data-owd-modal", "is_open")],
#     # [State("historical-data-thrpt-modal", "is_open")],
#     # [State("historical-data-trace-modal", "is_open")],
# )

# def toggle_owd(button1, button2, df, close_button, is_open):
#     # print(f"button1: {button1} button2: {button2} button3: {button3}")
#     # d = {str(button1): "owd", str(button2): "throughput", str(button3): "trace"}
#     # all_buttons = [button1, button2, button3]
#     # if any(all_buttons) or close_button:
#     #     pressed_button = all_buttons[all_buttons.index(True)]
#     #     print(d[pressed_button])
#     print("Button1:")
#     print(button1)
#     print("Button2:")
#     print(button2)
#     print("close_button:")
#     print(close_button)
#     if button1:
#         if not is_open:
#             all_dates = {'03/03/2025': {'owd': 48, 'throughput': 88, 'trace': 23}, '04/03/2025': {'owd': 48, 'throughput': 88, 'trace': 23}, '05/03/2025': {'owd': 48, 'throughput': 88, 'trace': 23}, '06/03/2025': {'owd': 49, 'throughput': 92, 'trace': 23}, '07/03/2025': {'owd': 49, 'throughput': 91, 'trace': 23}, '08/03/2025': {'owd': 49, 'throughput': 90, 'trace': 23}, '09/03/2025': {'owd': 48, 'throughput': 89, 'trace': 23}, '10/03/2025': {'owd': 49, 'throughput': 91, 'trace': 23}}

#             df = pd.DataFrame(all_dates).T.reset_index()
#             df.rename(columns={"index": "date"}, inplace=True)

#             # Normalize the data (percentage change from the first day)
#             df_normalized = df.copy()
#             for col in ["owd", "throughput", "trace"]:
#                 df_normalized[col] = df[col] - df[col].iloc[0]

#             df_melted = df_normalized.melt(id_vars=['date'], var_name='group', value_name='host_count')

#             fig = px.line(
#                 df_melted,
#                 x='date',
#                 y='host_count',
#                 color='group',
#                 title='Number of Hosts Over Time by Group',
#                 labels={'date': 'Date', 'host_count': 'Number of Hosts', 'group': 'Group'},
#                 line_shape='linear'
#             )

#             data = dbc.Row([
#                         dcc.Graph(
#                         figure=fig,  
#                         id='graph-hsstorical-stats',
#                         className='cls-hist-data-stats'
#                     ),
#             ], className='pt-1')
            
#             return not is_open, data
#         return not is_open, dash.no_update
#     return is_open, dash.no_update