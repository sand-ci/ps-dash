"""
Module with the extracted functions from different pages that can be reused.
"""

####################################################
                #pages/home.py
####################################################

import numpy as np
import plotly.express as px
import plotly.graph_objects as go

import pandas as pd

import model.queries as qrs
from dash import dash_table, html
from flask import request
from datetime import datetime, timedelta
import re
import utils.helpers as hp
from model.Alarms import Alarms
from functools import lru_cache
from plotly.subplots import make_subplots
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

    red_status = data[(data[key_value]=='bandwidth decreased from/to multiple sites')
            & (data['cnt']>0)][count_value].unique().tolist()

    yellow_status = data[(data[key_value].isin(['path changed', 'ASN path anomalies']))
                    & (data['cnt']>0)][count_value].unique().tolist()

    grey_status = data[(data[key_value].isin(['firewall issue', 'source cannot reach any', 'complete packet loss']))
                    & (data['cnt']>0)][count_value].unique().tolist()
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
    
def create_heatmap(site_dict, site, date_from=None, date_to=None, test_types=None, hostnames=None):
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
        plotly.graph_objects.Figure: The heatmap figure.
    """
    print("Debug create_heatmap for site reports...")
    records = site_dict.get(site, [])
    print(records)
    all_hostnames = set()
    all_test_types = set()
    for record in records:
        hosts_not_found = record[2]  # hosts_not_found dictionary
        for test_type, hosts in hosts_not_found.items():
            if hosts is not None:  # Check if hosts list is not empty
                all_hostnames.update(hosts)
                all_test_types.add(test_type)
    print("Applying filter....")
    # Apply filters if provided
    if test_types:
        all_test_types = set(test_types)
    if hostnames:
        all_hostnames = set(hostnames)

    # Create a list of all combinations (hostname + test type)
    combinations = [f"({test}) {host}" for host in all_hostnames for test in all_test_types]
    print(date_from)
    
    dateFrom = date_from.split(' ')[0]
    dateTo = date_to.split(' ')[0]
    date_from = datetime.strptime(dateFrom, '%Y-%m-%d')
    date_to = datetime.strptime(dateTo, '%Y-%m-%d') + timedelta(days=0, seconds=-1)
    print(f"Building graph for date range {date_from} to {date_to}...")
    # Generate all dates in the range
    date_range = [date_from + timedelta(days=i) for i in range((date_to - date_from).days + 1)]
    date_range_str = [date.strftime('%Y-%m-%d') for date in date_range]
    # Initialize a DataFrame to store the heatmap data
    heatmap_data = pd.DataFrame(index=date_range_str, columns=combinations, dtype=int)

    # Fill the DataFrame with 0 (absence) by default
    heatmap_data.fillna(0, inplace=True)

    # Mark presence of alarms with 1
    for record in records:
        try:
            # Parse the dates using the regex-based function
            record_from = parse_date(record[0])
            record_to = parse_date(record[1])
        except ValueError as err:
            print(f"Error parsing date: {err}")

        # Iterate over each day in the record's date range
        current_date = record_from
        while current_date <= record_to:
            current_date_str = current_date.strftime('%Y-%m-%d')
            if current_date_str in heatmap_data.index:
                for test_type, hosts in hosts_not_found.items():
                    if hosts and test_type in all_test_types:  # Apply test type filter
                        for host in hosts:
                            if host in all_hostnames:  # Apply hostname filter
                                combination = f"({test_type}) {host}"
                                if combination in heatmap_data.columns:
                                    heatmap_data.at[current_date_str, combination] = 1
            current_date += timedelta(days=1)
    # print(f"Heatmap data: {heatmap_data}")
    case = 0
    # print(f"Heatmap data unique values: {len(np.unique(heatmap_data.values))}")
    if heatmap_data.shape[1] == 1 or len(np.unique(heatmap_data.values)) == 1:
        heatmap_data[' '] = 0 # turning 1D data in 2D
        case = 1 # corner cases for cases when there is only one value meaning all the test for the period failed to be found in Elasticsearch
    #     print(f"Heatmap data: {heatmap_data}")
    # print(f"Heatmap data values: {heatmap_data.T.values}")
    # print(f"Heatmap data index: {heatmap_data.index}")
    # print(f"Heatmap data columns: {heatmap_data.columns}")
    # print("Heatmap data shape:", heatmap_data.shape)
    colorscales = {0:[[0, '#69c4c4'], [1, 'grey']], 1:[[0, '#ffffff'], [1, 'grey']]}
    colorbars = {1:dict(title="Data Availability", tickvals=[0, 1], ticktext=[' ', "Missed"]),
                    0:dict(title="Data Availability", tickvals=[0, 1], ticktext=["Available", "Missed"])}
    heatmap = go.Heatmap(
        z=heatmap_data.T.values,  # Transpose to have dates on the y-axis
        x=heatmap_data.index,  # Dates
        y=heatmap_data.columns,  # Host + Test Type combinations
        colorscale=colorscales[case],  # Custom colorscale
        colorbar=colorbars[case],
        hoverongaps=False,
        xgap=1,  # Add white borders between cells
        ygap=1,  # Add white borders between cells
        showscale=True
    )

    # Create the figure
    fig = go.Figure(data=[heatmap])

    # Update layout
    fig.update_layout(
        title=f"Missing tests for {site} ({date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')})",
        xaxis_title="Date",
        yaxis_title="Host + Test Type",
        font=dict(size=12)  # Set default font size
    )

    return fig, list(all_test_types), list(all_hostnames), site


####################################################
                #pages/asn_anomalies.py
####################################################


def generate_plotly_heatmap_with_anomalies(subset_sample):
    columns = ['src_netsite', 'dest_netsite', 'asn_list', 'ipv6']
    src_site, dest_site, anomaly, ipv = subset_sample[columns].values[0]
    ipv = 'IPv6' if ipv else 'IPv4'

    subset_sample['last_appearance_path'] = pd.to_datetime(subset_sample['last_appearance_path'], errors='coerce')

    # Create a short format date column for plotting
    subset_sample['last_appearance_short'] = subset_sample['last_appearance_path'].dt.strftime('%H:%M %d-%b')

    print('Size of dataset:', len(subset_sample))
    max_length = subset_sample["path_len"].max()

    # Convert the path list into a pivot DataFrame
    pivot_df = pd.DataFrame(
        subset_sample['repaired_asn_path'].tolist(),
        index=subset_sample.index,
        columns=[f"pos_{i+1}" for i in range(max_length)]
    ).applymap(lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else x)

    # Map ASNs to colors
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

    # Prepare hoverlabel background colors
    index_to_color = {i: color_list[i] for i in range(len(color_list))}
    hover_bgcolor = np.array([index_to_color.get(z, '#FFFFFF') for row in index_df.values for z in row]).reshape(index_df.shape)

    # Prepare the heatmap using WebGL
    fig = go.Figure()
    heatmap = go.Heatmap(
        z=index_df.values,
        x=index_df.columns,
        y=subset_sample['last_appearance_short'],  # Use formatted short date for y-axis
        colorscale=color_list,
        zmin=0,
        zmax=len(unique_rids),
        customdata=pivot_df.values,
        hoverlabel=dict(bgcolor=hover_bgcolor),
        hovertemplate="<b>Position: %{x}</b><br><b>ASN: %{customdata}</b><extra></extra>",
        showscale=False,
    )
    fig.add_trace(heatmap)

    # Add annotations for anomalies
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
                        font=dict(color='white', size=12, family="Arial", weight='bold'),
                    )
                else:
                    fig.add_annotation(
                        x=index_df.columns[col_idx],
                        y=subset_sample['last_appearance_short'].iloc[idx],
                        text=int(asn),
                        showarrow=False,
                        font=dict(color='white', size=10, family="Arial", weight='bold'),
                    )

    fig.update_layout(
        title=f"ASN path signature between {src_site} and {dest_site} for {ipv} paths",
        xaxis_title='Position',
        yaxis_title='Path Observation Date',
        margin=dict(r=150),
        height=600,
        yaxis=dict(autorange='reversed', type='category', fixedrange=False)
    )

    return fig

####################################################
                #pages/path_changed.py
####################################################
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




