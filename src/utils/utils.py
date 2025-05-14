"""
Module with the extracted functions from different pages that can be reused and are reused in different modules/pages.
"""
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

import pandas as pd

import model.queries as qrs
from dash import dash_table, html, dcc
from flask import request
from datetime import datetime, timedelta
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

def buildMap(mapDf):
    # usually test and production sites are at the same location,
    # so we add some noise to the coordinates to make them visible
    mapDf['lat'] = mapDf['lat'].astype(float) + np.random.normal(scale=0.01, size=len(mapDf))
    mapDf['lon'] = mapDf['lon'].astype(float) + np.random.normal(scale=0.01, size=len(mapDf))

    color_mapping = {
    '⚪': '#6a6969',
    '🔴': '#c21515',
    '🟡': '#ffd500',
    '🟢': '#01a301'
    }

    size_mapping = {
    '⚪': 4,
    '🔴': 3,
    '🟡': 2,
    '🟢': 1
    }

    mapDf['size'] = mapDf['Status'].map(size_mapping)

    fig = px.scatter_mapbox(data_frame=mapDf, lat="lat", lon="lon",
                        color="Status",
                        color_discrete_map=color_mapping,
                        size_max=11,
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

    return fig

def defineStatus(data, key_value, count_value):
    # remove the path changed between sites event because sites tend to show big numbers for this event
    # and it dominates the table. Use the summary event "path changed" instead
    data = data[data[key_value] != 'path changed between sites']

    red_status = data[(data[key_value].isin(['bandwidth decreased from/to multiple sites']))
            & (data['cnt']>0)][count_value]

    yellow_status = data[(data[key_value].isin(['ASN path anomalies']))
                    & (data['cnt']>0)][count_value]

    grey_status = data[(data[key_value].isin(['firewall issue', 'source cannot reach any', 'complete packet loss']))
                    & (data['cnt']>0)][count_value]
    return red_status, yellow_status, grey_status


def createDictionaryWithHistoricalData(dframe):
    site_dict = dframe.groupby('site').apply(
        lambda group: [
            (row['from'], row['to'], row['hosts_not_found'])
            for _, row in group.iterrows()
        ]
    ).to_dict()
    return site_dict


def generateStatusTable(alarmCnt):
    red_sites, yellow_sites, grey_sites = defineStatus(alarmCnt, 'event', 'site')
    
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

    url = f'{request.host_url}site'
    df_pivot['url'] = df_pivot['site'].apply(lambda name: 
                                             f"<a class='btn btn-secondary' role='button' href='{url}/{name}' target='_blank'>See latest alarms</a>" if name else '-')

    status_order = ['🔴', '🟡', '🟢', '⚪']
    df_pivot = df_pivot.sort_values(by='Status', key=lambda x: x.map({status: i for i, status in enumerate(status_order)}))
    display_columns = [col for col in df_pivot.columns.tolist() if col not in ['Status', 'site']]
    # print(display_columns)
    # print(df_pivot)
    # print(df_pivot.to_dict('records'))
    if len(df_pivot) > 0:
        element = html.Div([
                dash_table.DataTable(
                df_pivot.to_dict('records'),[{"name": i.upper(), "id": i, "presentation": "markdown"} for i in display_columns],
                filter_action="native",
                filter_options={"case": "insensitive"},
                sort_action="native",
                is_focused=True,
                markdown_options={"html": True},
                page_size=8,
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
                    'padding': '2px',
                    'font-size': '1.5em',
                    'textAlign': 'center',
                    'whiteSpace': 'pre-line',
                    },
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold'
                },
                style_data={
                    'height': 'auto',
                    'overflowX': 'auto',
                },
                style_table={
                    'overflowY': 'auto',
                    'overflowX': 'auto'
                },
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
def build_anomaly_heatmap(subset_sample):
    """
    This function creates the heatmap for the alarm 'ASN anomalies'
    """
    columns = ['src_netsite', 'dest_netsite', 'anomalies', 'ipv6']
    src_site, dest_site, anomaly, ipv = subset_sample[columns].values[0]
    ipv = 'IPv6' if ipv else 'IPv4'

    subset_sample['last_appearance_path'] = pd.to_datetime(subset_sample['last_appearance_path'], errors='coerce')

    subset_sample['last_appearance_short'] = subset_sample['last_appearance_path'].dt.strftime('%H:%M:%S %d-%b')

    print('Size of dataset:', len(subset_sample))
    max_length = subset_sample["path_len"].max()

    pivot_df = pd.DataFrame(
        subset_sample['repaired_asn_path'].tolist(),
        index=subset_sample.index,
        columns=[f"pos_{i+1}" for i in range(max_length)]
    ).applymap(lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else x)

    unique_rids = pd.Series(pivot_df.stack().unique()).dropna().tolist()
    if 0 not in unique_rids:
        unique_rids.append(0)
    rid_to_index = {rid: i + 1 for i, rid in enumerate(unique_rids)}
    rid_to_index[np.nan] = 0

    base_colors = px.colors.qualitative.Prism + px.colors.qualitative.Bold
    expanded_colors = (base_colors * (len(unique_rids) // len(base_colors) + 1))[:len(unique_rids)]
    color_list = ['#FFFFFF'] + expanded_colors
    color_list[rid_to_index[0]] = '#000000'

    index_df = pivot_df.applymap(lambda x: rid_to_index.get(x, 0))

    index_to_color = {i: color_list[i] for i in range(len(color_list))}
    hover_bgcolor = np.array([index_to_color.get(z, '#FFFFFF') for row in index_df.values for z in row]).reshape(index_df.shape)

    # Ensure customdata values are strings
    customdata = pivot_df.applymap(lambda x: str(int(x)) if pd.notna(x) else "").values

    fig = go.Figure()
    heatmap = go.Heatmap(
        z=index_df.values,
        x=index_df.columns,
        y=subset_sample['last_appearance_short'],
        colorscale=color_list,
        zmin=0,
        zmax=len(unique_rids),
        xgap=0.5, ygap=0.5,
        customdata=customdata,  # Use formatted customdata
        hoverlabel=dict(bgcolor=hover_bgcolor),
        hovertemplate="<b>Position: %{x}</b><br><b>ASN: %{customdata}</b><extra></extra>",
        showscale=False,
    )
    fig.add_trace(heatmap)

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
                        font=dict(color='white', size=15, family="Arial"),
                    )
                else:
                    fig.add_annotation(
                        x=index_df.columns[col_idx],
                        y=subset_sample['last_appearance_short'].iloc[idx],
                        text=int(asn),
                        showarrow=False,
                        font=dict(color='white', size=15, family="Arial"),
                    )

    fig.update_layout(
        title=f"{ipv} ASN paths",
        xaxis_title='Position',
        yaxis_title='Path Observation Date',
        margin=dict(r=150),
        height=600,
        yaxis=dict(autorange='reversed', type='category', fixedrange=False)
    )

    return fig

def build_position_based_heatmap(src, dest, dt, ipv) -> go.Figure:
    doc = qrs.query_ASN_paths_pos_probs(src, dest, dt, ipv)

    hm = doc["heatmap"]
    ipv = 'IPv6' if doc['ipv6'] else 'IPv4'

    fig = go.Figure(go.Heatmap(
        z=hm["probs"],
        x=[f"pos {p+1}" for p in hm["positions"]],
        y=[str(a) for a in hm["asns"]],
        colorscale=[[0.0, "white"], [0.001, "#caf0f8"], [0.4, "#00b4d8"], [0.9, "#03045e"], [1, "black"]],
        zmin=0, zmax=1,
        xgap=0.8, ygap=0.8,
        hovertemplate="ASN %{y}<br>Position %{x}<br>Frequency: %{z:.2%}<extra></extra>"
    ))
    fig.update_layout(
        title=(f"{ipv} paths - position-based ASN frequency"),
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

def generate_asn_cards(asn_data):
    # Create a card for each ASN
    cards = [
        dbc.Card(
            dbc.CardBody([
                html.H2(f"AS {asn['asn']}", className="card-title plot-subtitle text-left"),
                html.P(f"{asn['owner']}", className="card-text plot-subtitle text-left"),
            ]),
            className="mb-3 shadow-sm h-100",
            style={"width": "100%", "overflow": "hidden", "display": "flex", "flexDirection": "column", "justifyContent": "space-between"}
        )
        for asn in asn_data
    ]

    # Wrap cards in a responsive grid with align-items-stretch
    return dbc.Row([dbc.Col(card, lg=2, md=2, sm=3, className="d-flex") for card in cards], className="g-3 align-items-stretch")

def generate_graphs(data, src, dest, dt):
    def create_graphs(ipv6_filter):
        heatmap_figure = build_anomaly_heatmap(data[data['ipv6'] == ipv6_filter])
        path_prob_figure = build_position_based_heatmap(src, dest, dt, int(ipv6_filter))
        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=heatmap_figure, id=f"asn-sankey-ipv{'6' if ipv6_filter else '4'}"),
                html.P(
                    'This is a sample of the paths between the pair of sites. '
                    'The plot shows new (anomalous) ASNs framed in white. '
                    'The data is based on the alarms of type "ASN path anomalies".',
                    className="plot-subtitle"
                ),
            ], lg=12, xl=12, xxl=6, align="top", className="responsive-col"),
            dbc.Col([
                dcc.Graph(figure=path_prob_figure, id=f"asn-path-prob-ipv{'6' if ipv6_filter else '4'}"),
                html.P(
                    'The plot shows how often each ASN appears on a position, '
                    '(1 is 100% of time)',
                    className="plot-subtitle"
                ),
            ], lg=12, xl=12, xxl=6, align="top", className="responsive-col"),
        ], className="graph-pair")

    
    # Extract all ASNs and their owners
    asn_list = list(set([asn for sublist in data['repaired_asn_path'] for asn in sublist]))
    asn_owners = addNetworkOwners(asn_list)

    # Create ASN cards
    asn_cards = generate_asn_cards(asn_owners)

    if len(data['ipv6'].unique()) == 2:
        figures = html.Div([
            create_graphs(False),  # IPv4
            create_graphs(True),   # IPv6
        ], className="responsive-graphs")
    else:
        heatmap_figure = build_anomaly_heatmap(data)
        path_prob_figure = build_position_based_heatmap(src, dest, dt, -1)
        figures = html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=heatmap_figure, id="asn-sankey-ipv4"),
                    html.P(
                        'This is a sample of the paths between the pair of sites. '
                        'The plot shows new (anomalous) ASNs framed in white. '
                        'The data is based on the alarms of type "ASN path anomalies".',
                        className="plot-subtitle"
                    ),
                ], lg=12, xl=12, xxl=6, align="top", className="responsive-col"),
                dbc.Col([
                    dcc.Graph(figure=path_prob_figure, id="asn-path-prob-ipv4"),
                    html.P(
                        'The plot shows how often each ASN appears on a position, '
                        'where 1 is 100% of time.',
                        className="plot-subtitle"
                    ),
                ], lg=12, xl=12, xxl=6, align="top", className="responsive-col"),
            ], className="graph-pair", justify="between"),
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
# TODO: delete path_changed function
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
    