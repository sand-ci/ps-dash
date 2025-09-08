"""
Module with the extracted functions from different pages that can be reused and are reused in different modules/pages.
"""
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import hashlib
import pandas as pd

import model.queries as qrs
from dash import dash_table, html, dcc
from flask import request
from datetime import datetime, timedelta
from collections import Counter
import re
import utils.helpers as hp
from model.Alarms import Alarms
from functools import lru_cache
from plotly.subplots import make_subplots
from utils.parquet import Parquet
from elasticsearch.helpers import scan
import dash_bootstrap_components as dbc


####################################################
                #pages/home.py
####################################################

def buildMap(mapDf, connectivity=False, grouped=False):
    # usually test and production sites are at the same location,
    # so we add some noise to the coordinates to make them visible
    mapDf['lat'] = mapDf['lat'].astype(float) + np.random.normal(scale=0.01, size=len(mapDf))
    mapDf['lon'] = mapDf['lon'].astype(float) + np.random.normal(scale=0.01, size=len(mapDf))

    # Sort so that '🔴' and '🟡' are last (drawn on top)
    status_order = {'⚪': 0, '🟢': 1, '🟡': 2, '🔴': 3}
    mapDf = mapDf.sort_values(by='Status', key=lambda x: x.map(status_order))

    color_mapping = {
    '⚪': '#6a6969',
    '🔴': '#c21515',
    '🟡': '#ffd500',
    '🟢': '#01a301'
    }

    size_mapping = {
    '⚪': 1,
    '🔴': 1,
    '🟡': 1,
    '🟢': 1
    }

    mapDf['size'] = mapDf['Status'].map(size_mapping)

    fig = px.scatter_mapbox(data_frame=mapDf, lat="lat", lon="lon",
                        color="Status",
                        color_discrete_map=color_mapping,
                        size_max=6,
                        size='size',
                        hover_name="site",
                        custom_data=['Infrastructure','Network','Other'],
                        zoom=1,
                    )

    fig.update_traces(
        hovertemplate="<br>".join([
            "<b>%{hovertext}</b>",
            "Infrastructure: %{customdata[0]}",
            "Network: %{customdata[1]}",
            "Other: %{customdata[2]}",
        ]),
        marker=dict(opacity=0.7)
    )

    fig.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        mapbox=dict(
            accesstoken='pk.eyJ1IjoicGV0eWF2IiwiYSI6ImNraDNwb3k2MDAxNnIyeW85MTMwYTU1eWoifQ.1QQ1E5mPh3hoZjK5X5LH7Q',
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=43,
                lon=-6
            ),
            pitch=0,
            style='mapbox://styles/petyav/ckh3spvk002i419mzf8m9ixzi'
        ),
        showlegend=False,
        title = 'Status of all sites in the past 48 hours',
        template='plotly_white'
    )
    if connectivity:
        print("connectivity")
        mapDf = mapDf.set_index('site')[['lat','lon']]
        line_traces = add_connectivity_status(grouped, mapDf)
        # print(line_traces)
        for trace in line_traces:
            fig.add_trace(trace)
        fig.update_layout(
            mapbox=dict(
                center=dict(lat=46.0, lon=8.0),  # set your default center
                zoom=2.2  # lower = more zoomed out, higher = more zoomed in
            ),
            title=f"T1 Sites - Traceroute perfSONAR tests",
            margin=dict(l=0, r=0, t=0, b=0),
            showlegend=False
            )

    return fig



def defineStatus(data, key_value, count_value):
    # remove the path changed between sites event because sites tend to show big numbers for this event
    # and it dominates the table. Use the summary event "path changed" instead
    data = data.groupby(['to', key_value]).size().reset_index(name='cnt')
    data = data[data[key_value] != 'path changed between sites']

    red_status = data[(data[key_value].isin(['bandwidth decreased from/to multiple sites']))
            & (data['cnt']>0)][count_value]

    yellow_status = data[(data[key_value].isin(['ASN path anomalies per site', 'ASN path anomalies']))
                    & (data['cnt']>0)][count_value]

    grey_status = data[(data[key_value].isin(['firewall issue', 'source cannot reach any', 'complete packet loss']))
                    & (data['cnt']>0)][count_value]
    return red_status, yellow_status, grey_status, data


def createDictionaryWithHistoricalData(dframe):
    site_dict = dframe.groupby('site').apply(
        lambda group: [
            (row['from'], row['to'], row['hosts_not_found'])
            for _, row in group.iterrows()
        ]
    ).to_dict()
    return site_dict

def generateStatusTable(alarmCnt):

    red_sites = alarmCnt[(alarmCnt['event']=='bandwidth decreased from/to multiple sites')
            & (alarmCnt['cnt']>0)]['site'].unique().tolist()

    yellow_sites = alarmCnt[(alarmCnt['event'].isin(['ASN path anomalies']))
                    & (alarmCnt['cnt']>0)]['site'].unique().tolist()

    grey_sites = alarmCnt[(alarmCnt['event'].isin(['firewall issue', 'source cannot reach any', 'complete packet loss']))
                    & (alarmCnt['cnt']>0)]['site'].unique().tolist()

    catdf = qrs.getSubcategories()
    catdf = pd.merge(alarmCnt, catdf, on='event', how='left')

    df = catdf.groupby(['site', 'category'])['cnt'].sum().reset_index()

    df_pivot = df.pivot(index='site', columns='category', values='cnt')
    df_pivot.reset_index(inplace=True)

    df_pivot.sort_values(by=['Network', 'Infrastructure', 'Other'], ascending=False, inplace=True)


    def give_status(site):
        if site in red_sites:
            return '🔴'

        elif site in yellow_sites:
            return '🟡'
        
        elif site in grey_sites:
            return '⚪'
        return '🟢'

    df_pivot['Status'] = df_pivot['site'].apply(give_status)
    df_pivot['site name'] = df_pivot.apply(lambda row: f"{row['Status']} {row['site']}", axis=1)

    df_pivot = df_pivot[['site', 'site name', 'Status', 'Network', 'Infrastructure', 'Other']]

    url = f'{request.host_url}site_report'
    df_pivot['url'] = df_pivot['site'].apply(lambda name: 
                                             f"<a class='btn btn-secondary' role='button' href='{url}/{name}' target='_blank'>See latest alarms</a>" if name else '-')

    status_order = ['🔴', '🟡', '🟢', '⚪']
    df_pivot = df_pivot.sort_values(by='Status', key=lambda x: x.map({status: i for i, status in enumerate(status_order)}))
    display_columns = [col for col in df_pivot.columns.tolist() if col not in ['Status', 'site']]
    print(display_columns)

    if len(df_pivot) > 0:
        element = html.Div([
                dash_table.DataTable(
                df_pivot.to_dict('records'),[{"name": i.upper(), "id": i, "presentation": "markdown"} for i in display_columns],
                filter_action="native",
                filter_options={"case": "insensitive"},
                sort_action="native",
                is_focused=True,
                markdown_options={"html": True},
                page_size=10,
                style_cell={
                'padding': '10px',
                'font-size': '1.2em',
                'textAlign': 'center',
                'backgroundColor': '#ffffff',
                'border': '1px solid #ddd',
                },
                style_header={
                'backgroundColor': '#ffffff',
                'fontWeight': 'bold',
                'color': 'black',
                'border': '1px solid #ddd',
                },
                style_data={
                'height': 'auto',
                'overflowX': 'auto',
                },
                style_table={
                'overflowY': 'auto',
                'overflowX': 'auto',
                'border': '1px solid #ddd',
                'borderRadius': '5px',
                'boxShadow': '0 2px 5px rgba(0,0,0,0.1)',
                },
                style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f7f7f7',
                },
                {
                    'if': {'column_id': 'SITE NAME'},
                    'textAlign': 'left !important',
                }
                ],
                id='status-tbl')
            ], className='table-container')
    else:
        element = html.Div(html.H3('No alarms for this site in the past day'), style={'textAlign': 'center'})

    return element, pd.merge(df_pivot, alarmCnt[['site', 'lat', 'lon']].drop_duplicates(subset='site', keep='first'), on='site', how='left')

def createTable(df, id):
    return dash_table.DataTable(
            df.to_dict('records'),
            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in df.columns],
            markdown_options={"html": True},
            style_cell={
                'padding': '10px',
                'font-size': '1.5em',
                'textAlign': 'center',
                'whiteSpace': 'normal',
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd',
                'fontFamily': 'sans-serif "Courier New", Courier, monospace !important'
            },
            style_header={
                'fontWeight': 'bold',
                'color': 'black',
                'border': '1px solid #ddd',
            },
            style_data={
                'height': 'auto',
                'lineHeight': '20px',
                'border': '1px solid #ddd',
            },
            style_table={
                'overflowY': 'auto',
                'overflowX': 'auto',
                'border': '1px solid #ddd',
                'borderRadius': '5px',
                'boxShadow': '0 2px 5px rgba(0,0,0,0.1)',
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f2f2f2',
                }
            ],
            id=id)
    
def explainStatuses():
  categoryDf = qrs.getSubcategories()

  red_infrastructure = ['firewall issue', 'source cannot reach any', 'complete packet loss']

  status = [
  {
    'status category': 'Global',
      'resulted status': '🔴',
      'considered alarm types': '\n'.join(['bandwidth decreased from multiple']),
      'trigger': 'any type has > 0 alarms'
  },
  {
    'status category': 'Global',
      'resulted status': '🟡',
      'considered alarm types': '\n'.join(['path changed']),
      'trigger': 'any type has > 0 alarms'
  },
  {
    'status category': 'Global',
      'resulted status': '⚪',
      'considered alarm types': '\n'.join(['Infrastructure']),
      'trigger': 'Infrastructure status is 🔴'
  },
  {
    'status category': 'Global',
      'resulted status': '🟢',
      'considered alarm types': '',
      'trigger': 'otherwise'
  },
  {
    'status category': 'Infrastructure',
      'considered alarm types': ',\n'.join(red_infrastructure),
      'trigger': 'any type has > 0 alarms',
      'resulted status': '🔴',
  },
  {
    'status category': 'Infrastructure',
      'considered alarm types': ',\n'.join(list(set(categoryDf[categoryDf['category']=='Infrastructure']['event'].unique()) - set(red_infrastructure))),
      'trigger': 'any type has > 0 alarms',
      'resulted status': '🟡',
  }]

  status_explaned = pd.DataFrame(status)
  categoryDf = categoryDf.pivot_table(values='event', columns='category', aggfunc=lambda x: '\n \n'.join(x))

  return createTable(status_explaned, 'status_explaned'), createTable(categoryDf, 'categoryDf')


####################################################
                #pages/hosts_not_found.py
####################################################

def parse_date(date_str):
    """
    Parse a date string using regex to handle both formats:
    - '%Y-%m-%d %H:%M:%S.%fZ'
    - '%Y-%m-%dT%H:%M:%S.%fZ'
    """
    pattern = r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}\.\d{3})Z"
    match = re.match(pattern, date_str)

    if match:
        # Extract date and time components
        date_part, time_part = match.groups()
        # Combine into a datetime object
        return datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S.%f")
    else:
        raise ValueError(f"Date string '{date_str}' does not match expected formats")


def create_heatmap(site_dict, site, date_from, date_to, test_types=None, hostnames=None):
    """
    Create a Plotly heatmap for a specific site over a date range with optional filters.

    Args:
        site_dict (dict): Dictionary with site data.
        site (str): The site to visualize.
        date_from (str): Start date in 'YYYY-MM-DD' format.
        date_to (str): End date in 'YYYY-MM-DD' format.
        test_types (list): List of selected test types to filter by.
        hostnames (list): List of selected hostnames to filter by.

    Returns:
    '2025-04-10T00:00:00.000Z', '2025-04-10T23:59:59.000Z', {'owd': array(['psmsu01.aglt2.org'], dtype=object),
    '2025-04-06T00:00:00.000Z', '2025-04-06T23:59:59.000Z', {'owd': array(['psmsu01.aglt2.org'], dtype=object)
        plotly.graph_objects.Figure: The heatmap figure.
    """
    print("In create_heatmap function site_report...")
    # print(site)
    # print(site_dict)
    site_dict = site_dict[site_dict['site']==site]
    # print(site_dict)
    
    records = site_dict.to_dict('records')
    # print(f"Data records: {records}")

    date_from = datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S')
    date_to = datetime.strptime(date_to, '%Y-%m-%d %H:%M:%S')
    # Generate all dates in the range
    date_range = [date_from + timedelta(days=i) for i in range((date_to - date_from).days - 1)]
    date_range_str = [date.strftime('%Y-%m-%d') for date in date_range]
    # print(f"Date range: {date_range_str}")
    
    # print("Extracting all hosts...")
    # Extract all unique hostnames and test types
    all_hostnames = set()
    all_test_types = set()
    for record in records:
        hosts_not_found = record['hosts_not_found']  # hosts_not_found dictionary
        for test_type, hosts in hosts_not_found.items():
            if hosts is not None:  # Check if hosts list is not empty
                all_hostnames.update(hosts)
                all_test_types.add(test_type)
                
    # print("Applying filter....")
    if test_types:
        all_test_types = set(test_types)
    if hostnames:
        all_hostnames = set(hostnames)

    # all combinations (hostname + test type)
    combinations = [f"({test}) {host}" for host in all_hostnames for test in all_test_types]

    # store the heatmap data
    heatmap_data = pd.DataFrame(index=date_range_str, columns=combinations, dtype=int)

    # 0 (absence) by default
    heatmap_data.fillna(0, inplace=True)

    # Mark presence of alarms with 1
    print(f"Records: {records}")
    for record in records:
        try:
            record_from = parse_date(record['from'])
            record_to = parse_date(record["to"])
        except ValueError as err:
            print(f"Error parsing date: {err}")

        current_date = record_from
        while current_date <= record_to:
            current_date_str = current_date.strftime('%Y-%m-%d')
            if current_date_str in heatmap_data.index:
                for test_type, hosts in hosts_not_found.items():
                    if (hosts is not None) & (test_type in all_test_types):  # Apply test type filter
                        for host in hosts:
                            if host in all_hostnames:  # Apply hostname filter
                                combination = f"({test_type}) {host}"
                                if combination in heatmap_data.columns:
                                    heatmap_data.at[current_date_str, combination] = 1
            current_date += timedelta(days=1)
    case = 0
    if heatmap_data.shape[1] == 1 or len(np.unique(heatmap_data.values)) == 1:
        heatmap_data[' '] = 0 # turning 1D data in 2D
        case = 1 # corner cases for cases when there is only one value meaning all the test for the period failed to be found in Elasticsearch

    colorscales = {0:[[0, '#69c4c4'], [1, 'grey']], 1:[[0, '#ffffff'], [1, 'grey']]}
    colorbars = {1:dict(title="Data Availability", tickvals=[0, 1], ticktext=[' ', "Missed"]),
                    0:dict(title="Data Availability", tickvals=[0, 1], ticktext=["Available", "Missed"])}
    heatmap = go.Heatmap(
        z=heatmap_data.T.values,  # Transpose to have dates on the y-axis
        x=heatmap_data.index,  # Dates
        y=heatmap_data.columns,  # Host + Test Type combinations
        colorscale=colorscales[case],
        colorbar=colorbars[case],
        hoverongaps=False,
        xgap=1,  # Add white borders between cells
        ygap=1,  # Add white borders between cells
        showscale=True
    )

    fig = go.Figure(data=[heatmap])

    fig.update_layout(
        title=f"Missing tests for {site} ({date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})",
        xaxis_title="Date",
        yaxis_title="Host + Test Type",
        font=dict(size=12)
    )

    return fig, list(all_test_types), list(all_hostnames), site



####################################################
                #pages/asn_anomalies.py
####################################################
def asnAnomaliesGroupedAlarmVisualisation(alarm, site, src_alarms, dest_alarms, date, callback_extension=""):
    alarms_dest = []
    alarms_src = []
    if len(src_alarms) > 0:
        alarms_src = [[src[0]+' to '+site+f' ||| Anomalies: {src[2]}', src[0], site, 'dest'] for src in src_alarms]
    if len(dest_alarms) > 0:
        alarms_dest = [[site+' to '+dest[0]+f' ||| Anomalies: {dest[2]}', site, dest[0], 'src'] for dest in dest_alarms]
    all_alarms = alarms_dest + alarms_src
    sites_list_src = alarm['source']['as_destination_from']
    sites_list_dest = alarm['source']['as_source_to']
    
    
    ############# added asn distribution graph#####################
    def flatten_alarm_data(data, direction):
        rows = []
        for site, alarm_id, asn_list in data:
            for asn in asn_list:
                rows.append({'site': site, 'alarm_id': alarm_id, 'asn': asn, 'direction': direction})
        return rows

    df_src = pd.DataFrame(flatten_alarm_data(src_alarms, f'from {site} to'))
    df_dest = pd.DataFrame(flatten_alarm_data(dest_alarms, f'to {site} from'))

    # Combine
    df_combined = pd.concat([df_src, df_dest], ignore_index=True)
    asn_counts = (
                    df_combined
                    .groupby(['site', 'direction', 'asn'])
                    .size()
                    .reset_index(name='count')
                )

    # Plot (grouped by site, colored by ASN or direction)
    asn_counts['asn'] = asn_counts['asn'].astype(str)
    asn_list = asn_counts['asn'].unique().tolist()

    # Get colors for each ASN
    colors = px.colors.qualitative.Prism
    asn_color_map = {asn: colors[i % len(colors)] for i, asn in enumerate(asn_list)}

    # Get ASN owners
    asn_owner_info = addNetworkOwners(asn_list)  # returns list of dicts: {'asn': '...', 'owner': '...'}
    asn_owner_dict = {str(entry['asn']): entry['owner'] for entry in asn_owner_info}

    fig = px.bar(
        asn_counts,
        x='site',
        y='count',
        color='asn',
        facet_col='direction',
        title="ASN Anomalies per Site (Source vs Destination)",
        labels={'count': 'Anomaly Count', 'site': 'Site(s)'},
        color_discrete_sequence=px.colors.qualitative.Prism
    )

    # 🔹 Remove the legend for ASN
    fig.update_traces(showlegend=False)
    fig.update_layout(
                      barmode='stack',
                      plot_bgcolor='rgba(0,0,0,0)',   # transparent plot area
                      paper_bgcolor='rgba(0,0,0,0)' )
    
    ##########################################################
    
    asn_legend = html.Div([
                        html.H6("ASN Legend"),
                        html.Div([
                            html.Div([
                                html.Span(style={
                                    'display': 'inline-block',
                                    'width': '12px',
                                    'height': '12px',
                                    'backgroundColor': asn_color_map[asn],
                                    'marginRight': '6px',
                                    'borderRadius': '3px'
                                }),
                                html.Span(f"{asn}: {asn_owner_dict.get(asn, 'Unknown')}")
                            ], style={
                                'display': 'inline-block',
                                'margin': '5px 10px',
                                'whiteSpace': 'nowrap'
                            }) for asn in asn_list
                        ])
                    ], style={'marginTop': '20px'})

    return_component = html.Div([
            dbc.Row([
              dbc.Row([
                dbc.Col([
                  html.H3("ASN path anomalies", className="text-center bold p-3"),
                  html.H5(date, className="text-center bold"),
                ], lg=2, md=12, className="p-3"),
                dbc.Col(
                    html.Div([
                        dbc.Row([
                            dbc.Row([
                                html.H1(f"Summary for {site}", className="text-left"),
                                html.Hr(className="my-2")
                            ]),
                            dbc.Row([
                                html.P([
                                    f"{alarm['source']['total_paths_anomalies']} path change(s) were detected involving site {site}. "
                                    f"New ASN(s) appeared on routes where {site} acted as a source {len(alarm['source']['as_source_to'])} time(s), "
                                    f"and {len(alarm['source']['as_destination_from'])} time(s) as destination. "
                                    "You can see visualisation for all the path anomalies below or explore paths for a given site and date here: ",
                                    html.A("Explore paths", href=f"{request.host_url}/explore-paths/site={site}&dt={date}", target="_blank")
                                ], className='subtitle'),
                                dcc.Graph(figure=fig, style={'height': '400px'}),
                                asn_legend
                            ], justify="start"),
                        ])
                    ]),
                    lg=10, md=12, className="p-3"
                )
              ], className="alarm-header pair-details g-0", justify="between", align="center")                
            ], style={"padding": "0.5% 1.5%"}, className='g-0'),
          dbc.Row([
            html.Div(id=f'site-section-asn-anomalies-{a[0]}',
              children=[
                dbc.Button(
                    a[0],
                    value = {'src': a[1], 'dest': a[2], 'date': date},
                    id={
                        'type': 'asn-collapse-button'+callback_extension,
                        'index': f'asn-anomalies'+a[0]
                    },
                    className="collapse-button",
                    color="white",
                    n_clicks=0,
                ),
                dcc.Loading(
                  dbc.Collapse(
                      id={
                          'type': 'asn-collapse'+callback_extension,
                          'index': f'asn-anomalies'+a[0]
                      },
                      is_open=False, className="collaps-container rounded-border-1"
                ), color="#f9f9f900", style={"top":0}),
              ]) for a in all_alarms
            ], className="rounded-border-1 g-0", align="start", style={"padding": "0.5% 1.5%"})
          ], style={'background-color': "#f9f9f9ff"})
    return return_component

def generate_asn_cards(asn_data, anomalies):
    # Create a card for each ASN
    print('anomalies')
    print(anomalies)
    print(asn_data)
    cards = [
        dbc.Card(
            dbc.CardBody([
                html.H2(f"AS {asn['asn']}", className="card-title plot-sub text-left"),
                html.P(f"{asn['owner']}", className="card-text plot-sub text-left"),
            ]),
            className=f"mb-3 shadow-sm h-100 {'border-danger' if int(asn['asn']) in anomalies else 'border-secondary'}",
            style={"width": "100%", "overflow": "hidden", "display": "flex", "flexDirection": "column", "justifyContent": "space-between"}
        )
        for asn in asn_data
    ]

    # Wrap cards in a responsive grid with align-items-stretch
    return dbc.Row([dbc.Col(card, lg=2, md=2, sm=3, className="d-flex") for card in cards], className="g-3 align-items-stretch")

def build_anomaly_heatmap(subset_sample):
    columns = ['src_netsite', 'dest_netsite', 'anomalies', 'ipv6']
    src_site, dest_site, anomaly, ipv = subset_sample[columns].values[0]
    ipv = 'IPv6' if ipv else 'IPv4'

    subset_sample['last_appearance_path'] = pd.to_datetime(subset_sample['last_appearance_path'], errors='coerce')
    subset_sample['last_appearance_short'] = subset_sample['last_appearance_path'].dt.strftime('%H:%M:%S %d-%b')
    time_start = subset_sample['last_appearance_path'].min()
    time_end = subset_sample['last_appearance_path'].max()
    print('Size of dataset:', len(subset_sample))
    max_length = subset_sample["path_len"].max()

    pivot_df = pd.DataFrame(
        subset_sample['repaired_asn_path'].tolist(),
        index=subset_sample.index,
        columns=[f"pos_{i+1}" for i in range(max_length)]
    ).map(lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else x)

    font_size = 15
    if max_length > 24: font_size = 10
    elif max_length > 18: font_size = 11
    elif max_length > 16: font_size = 13

    # map unique ASNs to colors
    unique_rids = pd.Series(pivot_df.stack().unique()).dropna().tolist()

    # normal path (first row)
    doc = qrs.query_ASN_paths_pos_probs(src_site, dest_site, subset_sample['last_appearance_path'].min(), ipv == 'IPv6')
    hm = doc["heatmap"]
    
    
    asns = [str(a) for a in hm["asns"]]
    probs = np.array(hm["probs"])
    # normal path is considered to be the one that appears in 90% of cases
    max_indices = probs[:, probs.sum(axis=0) > 0.899999].argmax(axis=0)
    best_asns = [int(asns[i]) for i in max_indices] 
    # setting up colors
    unique_rids += [int(x) for x in best_asns if int(x) not in unique_rids]
    if 0 not in unique_rids:
        unique_rids.append(0)
    rid_to_index = {rid: i + 1 for i, rid in enumerate(unique_rids)}
    rid_to_index[np.nan] = 0
    base_colors = px.colors.qualitative.Prism + px.colors.qualitative.Bold
    expanded_colors = (base_colors * (len(unique_rids) // len(base_colors) + 1))[:len(unique_rids)]
    color_list = ["#FFFFFF"] + expanded_colors
    color_list[rid_to_index[0]] = '#000000'
    for asn in anomaly:
        color_list[rid_to_index[asn]] = "#C50000"
    color_list[rid_to_index[0]] = '#000000'
    index_to_color = {i: color_list[i] for i in range(len(color_list))}
    index_df = pivot_df.map(lambda x: rid_to_index.get(x, 0))
    best_asns_colors = [rid_to_index.get(asn, 0) for asn in best_asns]

    
    
    best_asns_str = [str(asn) for asn in best_asns]
    positions = hm["positions"]
    # calculate length of graph
    maxLength = max(len(max_indices), index_df.columns.size)
    
    hover_bgcolor = np.array([index_to_color.get(z, '#FFFFFF') for row in index_df.values for z in row]).reshape(index_df.shape)
    customdata = pivot_df.map(lambda x: str(int(x)) if pd.notna(x) else "").values


    usual_heatmap_trace = go.Heatmap(
        z=[best_asns_colors],
        x=[f"pos_{p+1}" for p in range(maxLength)],
        y=["Most Probable Path"],
        colorscale=color_list,
        zmin=0,
        zmax=len(unique_rids),
        xgap=0.5,
        ygap=0.5,
        customdata=[best_asns_str],
        hoverlabel=dict(bgcolor=hover_bgcolor),
        hovertemplate="<b>Position: %{x}</b><br><b>ASN: %{customdata}</b><extra></extra>",
        showscale=False
    )

    # paths with anomaly (second row)
    heatmap_trace = go.Heatmap(
        z=index_df.values,
        x=index_df.columns,
        y=subset_sample['last_appearance_short'],
        colorscale=color_list,
        zmin=0,
        zmax=len(unique_rids),
        xgap=0.5,
        ygap=0.5,
        customdata=customdata,
        hoverlabel=dict(bgcolor=hover_bgcolor),
        hovertemplate="<b>Position: %{x}</b><br><b>ASN: %{customdata}</b><extra></extra>",
        showscale=False
    )

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.1, 0.9],
        subplot_titles=[
            "Regular Path (Based on 7-day frequency observation)",
            f"Sample of Regular vs Anomalious Paths from {time_start.strftime('%H:%M:%S %d-%b')} to {time_end.strftime('%H:%M:%S %d-%b')}"
        ]
    )
    fig.add_trace(usual_heatmap_trace, row=1, col=1)
    fig.add_trace(heatmap_trace, row=2, col=1)

    
    for col_idx, asn in enumerate(best_asns_str):
        fig.add_annotation(
            x=f"pos_{positions[col_idx]+1}",
            y="Most Probable Path",
            text=asn,
            showarrow=False,
            font=dict(color='white', size=font_size, family="Arial"),
            row=1, col=1
        )
                    
    for idx, row in enumerate(pivot_df.values):
        for col_idx, asn in enumerate(row):
            if ~np.isnan(asn):
                if asn in anomaly:
                    fig.add_annotation(
                        x=index_df.columns[col_idx],
                        y=subset_sample['last_appearance_short'].iloc[idx],
                        text=int(asn),
                        showarrow=False,
                        bordercolor='white',
                        font=dict(color='white', size=font_size, family="Arial"),
                        row=2, col=1
                    )
                else:
                    fig.add_annotation(
                        x=index_df.columns[col_idx],
                        y=subset_sample['last_appearance_short'].iloc[idx],
                        text=int(asn),
                        showarrow=False,
                        font=dict(color='white', size=font_size, family="Arial"),
                        row=2, col=1
                    )

    fig.update_layout(
        title_text=f"{ipv} ASN Paths — Usual vs Anomaly Period",
        height=700,
        margin=dict(r=150),
        paper_bgcolor='#FFFFFF',
        plot_bgcolor='#FFFFFF',
        yaxis2=dict(autorange='reversed', type='category', fixedrange=False),
        yaxis1=dict(showticklabels=False)
    )
    return fig

def build_position_based_heatmap(src, dest, dt, ipv, data) -> go.Figure:
    doc = qrs.query_ASN_paths_pos_probs(src, dest, dt, ipv)

    hm = doc["heatmap"]
    ipv = 'IPv6' if doc['ipv6'] else 'IPv4'

    # Create cards with anomalies and probabilities
    anomalious_asns = {}
    for asn in data['anomalies'].explode().unique():
        if asn in hm['asns']:
            probs = hm['probs'][hm['asns'].index(asn)]
            string_info = []
            for i in range(len(probs)):
                if probs[i] > 0:
                    string_info.append(f"pos_{i+1}: {probs[i]:.2%}")
            anomalious_asns[asn] = string_info

    fig = go.Figure()

    # Add the heatmap
    fig.add_trace(go.Heatmap(
        z=hm["probs"],
        x=[f"pos_{p+1}" for p in hm["positions"]],
        y=[str(a) for a in hm["asns"]],
        colorscale=[[0.0, "white"], [0.001, "#caf0f8"], [0.4, "#00b4d8"], [0.9, "#03045e"], [1, "black"]],
        zmin=0, zmax=1,
        xgap=0.8, ygap=0.8,
        hovertemplate="ASN %{y}<br>Position %{x}<br>Frequency: %{z:.2%}<extra></extra>"
    ))

    # positions of anomalies to mark
    x_labels = [f"pos_{p+1}" for p in hm["positions"]]

    # get positions of anomalous cells and mark them with values
    for asn in anomalious_asns:
        if asn in hm["asns"]:
            row = hm["asns"].index(asn)
            for col, val in enumerate(hm["probs"][row]):
                if val > 0:
                    fig.add_trace(go.Scatter(
                        x=[x_labels[col]],
                        y=[str(asn)],
                        mode="text",
                        text=[str("{:.2%}".format(val))],
                        textfont=dict(color="#C50000", size=12, family="Arial Black"),
                        hoverinfo='skip',
                        showlegend=False
                    ))


    fig.update_layout(
        title=(f"{ipv} paths - position-based ASN frequency (7 days)"),
        xaxis_title="Position on Path",
        yaxis_title="ASN",
        height=600
    )

    return fig

def addNetworkOwners(asn_list):
    asn_list = sorted(asn_list, key=lambda x: int(x) if str(x).isdigit() else float('inf'))
    owners = qrs.getASNInfo(asn_list)
    asn_data = [{"asn": str(asn), "owner": owners.get(str(asn), "Unknown")} for asn in asn_list]
    return asn_data

def generate_graphs(data, src, dest, dt):
    def create_graphs(ipv6_filter):
        heatmap_figure = build_anomaly_heatmap(data[data['ipv6'] == ipv6_filter])
        path_prob_figure = build_position_based_heatmap(src, dest, dt, int(ipv6_filter), data)
        # path_prob_figure, cards = build_position_based_heatmap(src, dest, dt, int(ipv6_filter), data)
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=heatmap_figure, id="asn-sankey-ipv4", className="full-height-graph"),
                    html.P(
                        'This is a sample of the paths between the pair of sites. '
                        'The plot shows new (anomalous) ASNs framed in white. '
                        'The data is based on the alarms of type "ASN path anomalies".',
                        className="plot-sub"
                    ),
                ], xxl=6, className="responsive-col"),
                dbc.Col([
                    dcc.Graph(figure=path_prob_figure, id="asn-path-prob-ipv4", className="full-height-graph"),
                    html.P(
                        'The plot shows how often each ASN appears on a position, '
                        'where 1 is 100% of time.',
                        className="plot-sub"
                    )
                ], xxl=6, className="responsive-col"),
            ]),
        ], className="responsive-graphs")


    
    # Extract all ASNs and their owners
    asn_list = list(set([asn for sublist in data['repaired_asn_path'] for asn in sublist]))
    asn_owners = addNetworkOwners(asn_list)

    # Create ASN cards
    anomalies = data['anomalies'].explode().unique()
    asn_cards = generate_asn_cards(asn_owners, anomalies)
    # anomalies_cards = generate_asn_anomalies_cards(data)
    if len(data['ipv6'].unique()) == 2:
        figures = html.Div([
            create_graphs(False),  # IPv4
            create_graphs(True),   # IPv6
        ], className="responsive-graphs")
    else:

        heatmap_figure = build_anomaly_heatmap(data)
        path_prob_figure = build_position_based_heatmap(src, dest, dt, -1, data)
        figures = html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=heatmap_figure, id="asn-sankey-ipv4", className="full-height-graph"),
                    html.P(
                        'This is a sample of the paths between the pair of sites. '
                        'The plot shows new (anomalous) ASNs framed in white. '
                        'The data is based on the alarms of type "ASN path anomalies".',
                        className="plot-sub"
                    ),
                ], xxl=6, className="responsive-col"),
                dbc.Col([
                    dcc.Graph(figure=path_prob_figure, id="asn-path-prob-ipv4", className="full-height-graph"),
                    html.P(
                        'The plot shows how often each ASN appears on a position, '
                        'where 1 is 100% of time.',
                        className="plot-sub"
                    )
                ], xxl=6, className="responsive-col"),
            ]),
        ], className="responsive-graphs")


    # Return the graphs and the ASN cards
    return html.Div([
        figures,
        html.Div([
            html.H3("Network Owners"),
            asn_cards
        ], className="asn-owner-section")
    ])

####################################################
                #pages/path_changed.py
####################################################
def descChange(pair, chdf, posDf):

  owners = qrs.getASNInfo(posDf[(posDf['pair']==pair)]['asn'].values.tolist())
  owners['-1'] = 'OFF/Unavailable'
  howPathChanged = []
  for diff in chdf[(chdf['pair']==pair)]['diff'].values.tolist()[0]:

    for pos, P in posDf[(posDf['pair']==pair)&(posDf['asn']==diff)][['pos','P']].values:
        atPos = posDf[(posDf['pair']==pair) & (posDf['pos']==pos)]['asn'].values.tolist()

        if len(atPos)>0:
          atPos.remove(diff)
          for newASN in atPos:
            if newASN not in [0, -1]:
              if P < 1:
                howPathChanged.append({'diff': diff,
                                      'diffOwner': owners[str(diff)], 'atPos': pos,
                                      'jumpedFrom': newASN, 'jumpedFromOwner': owners[str(newASN)]})
          # the following check is covering the cases when the change happened at the very end of the path
          # i.e. the only ASN that appears at that position is the diff detected
          if len(atPos) == 0:
            howPathChanged.append({'diff': diff,
                                  'diffOwner': owners[str(diff)], 'atPos': pos,
                                  'jumpedFrom': "No data", 'jumpedFromOwner': ''})

  if len(howPathChanged)>0:
    return pd.DataFrame(howPathChanged).sort_values('atPos')
  
  return pd.DataFrame()

def extractRelatedOnly(chdf, asn):
  chdf.loc[:, 'spair'] = chdf[['src_site', 'dest_site']].apply(lambda x: ' -> '.join(x), axis=1)

  diffData = []
  for el in chdf[['pair','spair','diff']].to_dict('records'):
      for d in el['diff']:
          diffData.append([el['spair'], el['pair'],d])

  diffDf = pd.DataFrame(diffData, columns=['spair', 'pair', 'diff'])
  return diffDf[diffDf['diff']==asn]

def extractAlarm(q, aInst=Alarms(), **other_unknown_query_strings):
    alarm = qrs.getAlarm(q)['source']
    print('URL query:', q, other_unknown_query_strings)
    print()
    print('Alarm content:', alarm)
    chdataFrame, posdataFrame, bline, alterPaths = qrs.queryTraceChanges(alarm['from'], alarm['to'], alarm['asn'])
    dfrom, dto = hp.getPriorNhPeriod(alarm["to"])
    fr, pivotFr = aInst.loadData(dfrom, dto)
    posdataFrame['asn'] = posdataFrame['asn'].astype(int)

    dsts = chdataFrame.groupby('dest_site')[['pair']].count().reset_index().rename(columns={'dest_site':'site'})
    srcs = chdataFrame.groupby('src_site')[['pair']].count().reset_index().rename(columns={'src_site':'site'})
    countPair = pd.concat([dsts, srcs]).groupby(['site']).sum().reset_index().sort_values('pair', ascending=False)
    topDownList =  countPair[ countPair['site'].isin(alarm['sites'])]['site'].values.tolist()
    selectTab = topDownList[0]
    if 'site' in other_unknown_query_strings.keys():
      if other_unknown_query_strings['site'] in  countPair['site'].values:
        selectTab = other_unknown_query_strings['site']
    return alarm, chdataFrame, posdataFrame, bline, alterPaths, dfrom, dto, topDownList, fr, pivotFr, selectTab,  countPair

####################################################
                #pages/site.py
####################################################
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

#######################################
         #pages/throughput.py
#######################################
def getRawDataFromES(src, dest, dateFrom, dateTo, ipv6):
    pq = Parquet()
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')
    sips = metaDf[(metaDf['site'] == src) | (metaDf['netsite'] == src)]['ip'].values.tolist()
    sips = [ip.upper() for ip in sips] + [ip.lower() for ip in sips]
    dips = metaDf[(metaDf['site'] == dest) | (metaDf['netsite'] == dest)]['ip'].values.tolist()
    dips = [ip.upper() for ip in dips] + [ip.lower() for ip in dips]

    if len(sips) > 0 or len(dips) > 0:
        return qrs.queryBandwidthIncreasedDecreased(dateFrom, dateTo, sips, dips, ipv6)

    else: print(f'No IPs found for the selected sites {src} and {dest} {ipv6}')

def buildPlot(df):
    fig = go.Figure(data=px.scatter(
        df,
        y = 'MBps',
        x = 'dt',
        color='pair'
        
    ))

    fig.update_layout(showlegend=False)
    fig.update_traces(marker=dict(
                        color='#00245a',
                        size=12
                    ))
    fig.layout.template = 'plotly_white'
    # fig.show()
    return fig


def buildDataTable(df):
    columns = ['dt', 'pair', 'throughput', 'src_host', 'dest_host',
              'retransmits', 'src_site', 'src_netsite', 'src_rcsite', 'dest_site',
              'dest_netsite', 'dest_rcsite', 'src_production', 'dest_production']

    df = df[columns]
    return html.Div(dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{"name": i, "id": i} for i in df.columns],
            id='tbl-raw',
            style_table={
                'overflowX': 'auto'
            },
            page_action="native",
            page_current= 0,
            page_size= 10,
            style_cell={
              'padding': '3px'
            },
            style_header={
                'fontWeight': 'bold'
            },
            style_data={
                'height': 'auto',
                'lineHeight': '15px',
            },
            filter_action="native",
            filter_options={"case": "insensitive"},
            sort_action="native",
        ))

def getSitePairs(alarm):
  sitePairs = []
  event = alarm['event']

  if event in ['bandwidth decreased', 'bandwidth increased']:
    sitePairs = [{'src_site': alarm['source']['src_site'],
                'dest_site': alarm['source']['dest_site'],
                'ipv6': alarm['source']['ipv6'],
                'change':  alarm['source']['change']}]

  elif event in ['bandwidth decreased from/to multiple sites', 'bandwidth increased from/to multiple sites']:
    for i,s in enumerate(alarm['source']['dest_sites']):
      temp = {'src_site': alarm['source']['site'],
              'dest_site': s,
              'ipv6': alarm['source']['ipv6'],
              'change':  alarm['source']['dest_change'][i]}
      sitePairs.append(temp)

    for i,s in enumerate(alarm['source']['src_sites']):
      temp = {'src_site': s,
              'dest_site': alarm['source']['site'],
              'ipv6': alarm['source']['ipv6'],
              'change':  alarm['source']['src_change'][i]}
      sitePairs.append(temp)
  
  return sitePairs

def getRawDataFromES(src, dest, ipv6, dateFrom, dateTo):
    pq = Parquet()
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')
    sips = metaDf[(metaDf['site'] == src) | (metaDf['netsite'] == src)]['ip'].values.tolist()
    sips = [ip.upper() for ip in sips] + [ip.lower() for ip in sips]
    dips = metaDf[(metaDf['site'] == dest) | (metaDf['netsite'] == dest)]['ip'].values.tolist()
    dips = [ip.upper() for ip in dips] + [ip.lower() for ip in dips]

    if len(sips) > 0 or len(dips) > 0:
      q = {
          "query" : {  
              "bool" : {
              "must" : [
                    {
                      "range": {
                          "timestamp": {
                          "gte": dateFrom,
                          "lte": dateTo,
                          "format": "strict_date_optional_time"
                          }
                        }
                    },
                    {
                      "terms" : {
                        "src": sips
                      }
                    },
                    {
                      "terms" : {
                        "dest": dips
                      }
                    },
                    {
                      "term": {
                          "ipv6": {
                            "value": ipv6
                          }
                      }
                    }
                ]
              }
            }
          }
      # print(str(q).replace("\'", "\""))

      result = scan(client=hp.es,index='ps_throughput',query=q)
      data = []

      for item in result:
          data.append(item['_source'])

      df = pd.DataFrame(data)

      df['pair'] = df['src']+'->'+df['dest']
      df['dt'] = df['timestamp']
      return df

    else: print(f'No IPs found for the selected sites {src} and {dest} {ipv6}')

######################################################
# map which visualises OPN site connectivity from ps_trace data
##################################################### 

def get_color(row):
    """Return a colour based on traceroute status."""
    dest_reached = row.get('destination_reached', False)
    path_complete = row.get('path_complete', False)
    if not dest_reached and not path_complete:
        return 'red'
    elif path_complete and not dest_reached:
        return 'yellow'
    else:
        return 'green'
    
def quad_bezier_points(p0, p1, p2, n=60):
    """Return n points along a quadratic Bezier curve defined by p0, p1, p2 (each [lon, lat])."""
    t = np.linspace(0, 1, n)
    one_minus_t = 1 - t
    pts = (one_minus_t[:, None]**2) * p0 + 2 * (one_minus_t[:, None] * t[:, None]) * p1 + (t[:, None]**2) * p2
    return pts[:, 0], pts[:, 1]  # lons, lats

def control_point_for_arc(lon0, lat0, lon1, lat1, curvature=0.20):
    """
    Control point offset from midpoint, perpendicular to the segment.
    curvature>0 bows one way, <0 the other. Magnitude ~ percent of segment length.
    """
    vx, vy = (lon1 - lon0), (lat1 - lat0)
    seg_len = np.hypot(vx, vy)
    if seg_len == 0:
        return lon0, lat0
    # unit perpendicular (rotate by +90°)
    px, py = (-vy / seg_len, vx / seg_len)
    mx, my = (lon0 + lon1) / 2.0, (lat0 + lat1) / 2.0
    ox, oy = curvature * seg_len * px, curvature * seg_len * py
    return mx + ox, my + oy

def curvature_for_pair(src, dst, lon0, lat0, lon1, lat1, base=0.22, flip_ratio=0.5, bias=0.0):
    """
    Decide curvature sign deterministically for a pair. 
    - base: absolute curvature magnitude
    - flip_ratio in [0,1]: fraction of links that should flip sign
    - bias in [-1,1]: nudges more arcs downward (<0) or upward (>0)
    """
    # Start from geometric preference so long east-west-ish links lean consistently
    base_sign = 1.0 if lon0 < lon1 else -1.0

    # Stable hash in [0,1)
    hkey = f"{src}|{dst}"
    hv = int(hashlib.sha1(hkey.encode()).hexdigest(), 16)
    u = (hv % 10_000_000) / 10_000_000.0

    # Apply bias: shift the threshold for flipping
    threshold = np.clip(flip_ratio + bias, 0.0, 1.0)

    # Flip if u < threshold (so threshold=0.5 ≈ half flip)
    sign = base_sign if u >= threshold else -base_sign
    return sign * base

def make_arc_trace(lat0, lon0, lat1, lon1, color='gray', hovertext='',
                   src=None, dst=None, base_curv=0.22, flip_ratio=0.5, bias=0.0, n_points=70):
    """
    Curved Scattermapbox line between two points using a quadratic Bezier.
    src/dst strings are used only to make the curvature flip deterministic.
    """
    curv = curvature_for_pair(src or "", dst or "", lon0, lat0, lon1, lat1,
                              base=base_curv, flip_ratio=flip_ratio, bias=bias)
    cx, cy = control_point_for_arc(lon0, lat0, lon1, lat1, curvature=curv)
    lons, lats = quad_bezier_points(
        np.array([lon0, lat0]),
        np.array([cx,   cy]),
        np.array([lon1, lat1]),
        n=n_points
    )
    return go.Scattermapbox(
        lon=lons.tolist(),
        lat=lats.tolist(),
        mode='lines',
        line=dict(color=color, width=2),
        hoverinfo='text',
        hovertext=hovertext,
        showlegend=False
    )

    
def add_connectivity_status(df, coords_df):
    line_traces = []
    print("in add_connectivity_status...")
   # Create arced line traces for each traceroute
    line_traces = []
    for _, row in df.iterrows():
        src = row.get('src_netsite')
        dst = row.get('dest_netsite')
        if not src or not dst or src == dst:
            continue
        try:
            lat0, lon0 = coords_df.loc[src, ['lat', 'lon']]
            lat1, lon1 = coords_df.loc[dst, ['lat', 'lon']]
            if pd.isna(lat0) or pd.isna(lon0) or pd.isna(lat1) or pd.isna(lon1):
                continue
        except KeyError:
            continue

        colour = row.get('color', 'grey')
        hovertext = (
            f"Source: {src}<br>"
            f"Src_host: {row.get('src_host')}<br>"
            f"Destination: {dst}<br>"
            f"Dest_host: {row.get('dest_host')}<br>"
            f"Destination reached: {row.get('destination_reached')}<br>"
            f"Destination reached stats: {row.get('destination_reached_stats')}<br>"
            f"Path complete: {row.get('path_complete')}<br>"
            f"Path complete stats: {row.get('path_complete_stats')}"
        )

        # Tune these two knobs if you want more/less “smiles”
        FLIP_RATIO = 0.50   # ≈ half of the links flip sign
        BIAS       = 0.00   # try -0.15 to bias more arcs downward globally

        trace = make_arc_trace(
            lat0=lat0, lon0=lon0, lat1=lat1, lon1=lon1,
            color=colour, hovertext=hovertext,
            src=src, dst=dst,           # ensures deterministic curvature per pair
            base_curv=0.22,             # curvature magnitude
            flip_ratio=FLIP_RATIO,      # fraction to flip
            bias=BIAS,                  # global nudge (negative -> more “down”)
            n_points=70
        )
        line_traces.append(trace)
        line_traces.append(trace)
    return line_traces

###################################################
#        analysis of perfSONAR data and CRIC data
###################################################


# import asyncio, socket, ssl, json, time
# from contextlib import suppress
# import aiohttp
# import pandas as pd
# from elasticsearch import Elasticsearch
# from elasticsearch.helpers import scan

# ES_INDICES = ["ps_trace", "ps_throughput", "ps_owd"]
# LOOKBACK_DAYS = 30
# VERIFY_TLS = False

# PORT = 443
# CONNECT_TIMEOUT = 3
# HTTP_TIMEOUT = aiohttp.ClientTimeout(total=5)
# RETRIES = 3
# BACKOFF = [0, 5, 15]
# STATUSES = ["ACTIVE_HTTP", "ACTIVE_TCP_ONLY", "UNREACHABLE_CANDIDATE", "RETIRED_DNS"]




# async def resolve(host):
#     with suppress(Exception):
#         return await asyncio.get_running_loop().getaddrinfo(
#             host, PORT, type=socket.SOCK_STREAM
#         )
#     return []

# async def tcp_connect(addr):
#     try:
#         fut = asyncio.open_connection(addr[4][0], addr[4][1], ssl=False)
#         r, w = await asyncio.wait_for(fut, timeout=CONNECT_TIMEOUT)
#         w.close(); await w.wait_closed()
#         return True
#     except Exception:
#         return False

# async def http_probe(host):
#     try:
#         ctx = ssl.create_default_context()
#         async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as s:
#             async with s.head(f"https://{host}/pscheduler", ssl=ctx) as resp:
#                 return 200 <= resp.status < 500
#     except Exception:
#         return False



# async def audit_host(host, cric_perf_hosts):
#     print(f"Auditing {host}...")
#     result = {"host": host}
#     result["in_cric"] = host in cric_perf_hosts
#     result["found_in_ES"] = hostFoundInES(host, LOOKBACK_DAYS, ES_INDICES)
#     addrs = await resolve(host)
#     if not addrs:
#         result["status"] = "RETIRED_DNS"
#         return result

#     # Retry with backoff
#     try:
#         tcp_ok = http_ok = False
#         for i in range(RETRIES):
#             if i: await asyncio.sleep(BACKOFF[i])
#             # try each resolved address until one works
#             tcp_ok = any([await tcp_connect(a) for a in addrs])
#             http_ok = await http_probe(host)
#             if tcp_ok or http_ok:
#                 break
#         if http_ok:
#             result["status"] = "ACTIVE_HTTP"
#         elif tcp_ok:
#             result["status"] = "ACTIVE_TCP_ONLY"
#         else:
#             result["status"] = "UNREACHABLE_CANDIDATE"
            
#     except Exception as e:
#         print(e)
#         result["status"] = "ERROR"
#         return result
        
#     return result

# async def hosts_audit(hosts, cric_hosts, concurrency=32):
#     sem = asyncio.Semaphore(concurrency)
#     async def wrapped(h):
#         async with sem:
#             return await audit_host(h, cric_hosts)
#     return await asyncio.gather(*[wrapped(h) for h in hosts])


# def group_by_status(results):
#     """
#     results: list of {"host": str, "status": str, ...} from main(hosts)
#     returns: (buckets: dict[str, list[str]], unknown: list[str])
#     """
#     buckets = {s: [] for s in STATUSES}
#     unknown = []
#     for r in results:
#         s = r.get("status", "UNKNOWN")
#         host_info = f"host: {r.get('host', '')} | in CRIC: {r.get('in_cric', '')} | in ES: {r.get('found_in_ES', '')}"
#         if s in buckets:
#             buckets[s].append(host_info)
#         else:
#             unknown.append(host_info)
#     # de-dup + sort for readability
#     for s in buckets:
#         buckets[s] = sorted(set(buckets[s]))
#     unknown = sorted(set(unknown))
#     return buckets, unknown

# def print_buckets(buckets, unknown=None):
#     total = sum(len(v) for v in buckets.values()) + (len(unknown) if unknown else 0)
#     print(f"\n=== Hosts by status (total: {total}) ===")
#     for s in STATUSES:
#         hs = buckets.get(s, [])
#         print(f"\n{s}: {len(hs)}")
#         for h in hs:
#             print(f"  - {h}")
#     if unknown:
#         print(f"\nUNKNOWN: {len(unknown)}")
#         for h in unknown:
#             print(f"  - {h}")

# # if __name__ == '__main__':
# #     print(" --- getting hosts from psConfigAdmin ---")
# #     mesh_url = "https://psconfig.aglt2.org/pub/config"
# #     mesh_config = psconfig.api.PSConfig(mesh_url)
# #     all_hosts_in_configs = mesh_config.get_all_hosts()
# #     print(f"All hosts in psConfig: {len(all_hosts_in_configs)}\n")
    
# #     print(" --- getting PerfSonars from WLCG CRIC ---")
# #     all_cric_perfsonar_hosts = []
# #     r = requests.get(
# #         'https://wlcg-cric.cern.ch/api/core/service/query/?json&state=ACTIVE&type=PerfSonar',
# #         verify=False
# #     )
# #     res = r.json()
# #     for _key, val in res.items():
# #         if not val['endpoint']:
# #             print('no hostname? should not happen:', val)
# #             continue
# #         p = val['endpoint']
# #         all_cric_perfsonar_hosts.append(p)
# #     print(f"All perfSONAR hosts in CRIC: {len(all_cric_perfsonar_hosts)}\n")

# #     print(" --- audit of all hosts from psConfigAdmin ---")
# #     result = await hosts_audit(all_hosts_in_configs, all_cric_perfsonar_hosts)
# #     grouped_result = group_by_status(result)