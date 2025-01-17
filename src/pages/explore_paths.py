import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import pandas as pd

import plotly.graph_objects as go
import plotly.express as px

import utils.helpers as hp
import model.queries as qrs
from model.Alarms import Alarms
from utils.parquet import Parquet

from utils.helpers import timer


def title():
    return f"Search & explore"



def description(q=None):
    return f"Explore the 'Path changed' alarms"

pq = Parquet()

dash.register_page(
    __name__,
    path_template="/explore-paths",
    title=title,
    description=description,
)


def layout(**other_unknown_query_strings):
    period = hp.defaultTimeRange(days=3, datesOnly=True)
    return dbc.Row([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.H1(f"Short term path deviations between sites"),
                        html.P('The data is based on the alarms of type "path changed"', style={"font-size": "1.2rem"})
                    ], className="l-h-3 p-2"),
                    dcc.Loading(
                        html.Div(id="asn-sankey"), color='#00245A'),
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=6, lg=12, md=12, sm=12, className=" mb-1 flex-grow-1",
            ),
            dbc.Col([
                html.Div(id="asn-alarms-container", children=[
                    html.Div([
                        html.H1(f"ASN path anomalies"),
                        html.P('The data is based on the alarms of type "ASN path anomalies', style={"font-size": "1.2rem"})
                    ], className="l-h-3 p-2"),
                    dcc.Loading(
                        html.Div(create_anomalies_heatmap(period), id="asn-alarms-heatmap", style={"max-width": "1000px", "margin": "0 auto"}),
                        color='#00245A')
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=6, lg=12, md=12, sm=12, className="mb-1 flex-grow-1")
        ], className="gx-4 d-flex", justify="between", align="start"),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H1(f"Search the \"Path changed\" alarms",
                                className="l-h-3 pl-2"),
                        html.P(
                            f'Alarms generated in the period: {period[0]} - {period[1]} ',
                            style={"padding-left": "1.5%", "font-size": "14px"})
                    ], align="center", className="text-left pair-details rounded-border-1"),
                ], justify="start", align="center"),
                html.Br(),
                html.Br(),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-sites-dropdown',
                                     placeholder="Search for a site"),
                    ]),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-asn-dropdown',
                                     placeholder="Search ASNs"),
                    ]),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("Search", id="search-button", color="secondary", 
                                   className="mlr-2", style={"width": "100%", "font-size": "1.5em"})
                    ])
                ]),
            ], lg=12, md=12, className="pl-1 pr-1 p-1"),
        ], className="p-1 site boxwithshadow page-cont mb-1", justify="center", align="center"),
        html.Br(),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H1(f"List of alarms", className="text-center"),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='paths-results-table'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-1 g-0", justify="center", align="center"),
        html.Br(),
        html.Br(),
    ], className=' main-cont', align="center", style={"padding": "0.5% 1.5%"})


def colorMap(eventTypes):
  colors = ['#75cbe6', '#3b6d8f', '#75E6DA', '#189AB4', '#2E8BC0', '#145DA0', '#05445E', '#0C2D48',
            '#5EACE0', '#d6ebff', '#498bcc', '#82cbf9',
            '#2894f8', '#fee838', '#3e6595', '#4adfe1', '#b14ae1'
            '#1f77b4', '#ff7f0e', '#2ca02c', '#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            ]

  paletteDict = {}
  for i, e in enumerate(eventTypes):
      paletteDict[e] = colors[i]

  return paletteDict


@dash.callback(
    [
        Output("paths-sites-dropdown", "options"),
        Output("paths-asn-dropdown", "options"),
        Output('asn-sankey', 'children'),
        Output('paths-results-table', 'children'),
        Output('asn-alarms-container', 'style')
    ],
    [
        Input("search-button", "n_clicks"),
        Input("paths-asn-dropdown", "search_value"),
        Input("paths-asn-dropdown", "value"),
        Input("paths-sites-dropdown", "search_value"),
        Input("paths-sites-dropdown", "value"),
    ],
    State("paths-sites-dropdown", "value"),
    State("paths-asn-dropdown", "value")
)
def update_output(n_clicks, asn, asnState, sites, sitesState, sitesStateValue, asnStateValue):

    # that period should match the one in the layout, 
    # as well as the range of the cached data
    period = hp.defaultTimeRange(3)

    # Load all data initially
    if n_clicks is None:
        sitesState = []
        asnState = []
    else:
        sitesState = [] if sitesStateValue is None else sitesStateValue
        asnState = [] if asnStateValue is None else asnStateValue

    alarmsInst = Alarms()
    frames, pivotFrames = alarmsInst.loadData(period[0], period[1])
    dataTables = []
    sitesDropdownData = []

    if 'path changed between sites' in frames.keys() and 'path changed' in frames.keys():            
        selected_keys = ['path changed between sites', 'path changed', 'ASN path anomalies']
        frames = {key: frames[key] for key in selected_keys if key in frames}
        pivotFrames = {key: pivotFrames[key] for key in selected_keys if key in pivotFrames}

        df =  pivotFrames['path changed between sites']
        scntdf = df[df['tag'] != ''].groupby('tag')[['id']].count().reset_index().rename(columns={'id': 'cnt', 'tag': 'site'})

        # sites
        graphData = scntdf.copy()
        graphData = graphData[graphData['site'].isin(sitesState)]

        for s in sorted(pivotFrames['path changed'].tag.unique().tolist()):
            sitesDropdownData.append({"label": s.upper(), "value": s.upper()})

        # data tables
        for event in sorted(['path changed', 'path changed between sites', 'ASN path anomalies']):
            df = pivotFrames[event]
            
            df = df[df['tag'].isin(sitesState)] if len(sitesState) > 0 else df
            if 'diff' in df.columns and len(asnState) > 0:
                df = df[df['diff'].isin(asnState)]
            elif 'asn' in df.columns and len(asnState) > 0:
                df = df[df['asn'].isin(asnState)]
            elif 'asn_list' in df.columns and len(asnState) > 0:
                df = df[df['asn_list'].isin(asnState)]
                anomalous_asns = list(df['asn_list'].explode().unique())
            
            if 'src_site' in df.columns and 'dest_site' in df.columns and len(sitesState) > 0:
                df = df[(df['src_site'].isin(sitesState)) | (df['dest_site'].isin(sitesState))]

            if len(df) > 0:
                dataTables.append(generate_tables(frames[event], df, event, alarmsInst))
        
        if len(dataTables)==0:
            dataTables.append(html.P(f'There are no alarms related to the selected criteria', 
                                    style={"padding-left": "1.5%", "font-size": "14px"}))
    
    dataTables = html.Div(dataTables)


    # graph
    changeDf = pq.readFile('parquet/prev_next_asn.parquet')
    asnsDropdownData = []
    container_style = {"display": "none"}

    if len(changeDf) == 0:
        fig = go.Figure()
        fig.update_layout(
            annotations=[
                dict(
                    text="No data available for the selected criteria.",
                    showarrow=False,
                    font=dict(size=16),
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                )
            ]
        )
    else:
        changeDf['jumpedFrom'] = changeDf['jumpedFrom'].astype(int)
        changeDf['diff'] = changeDf['diff'].astype(int)
        
        sortedDf = changeDf[changeDf['jumpedFrom'] > 0].sort_values('count')
        asnsDropdownData = list(set(sortedDf['diff'].unique().tolist() +
                                    sortedDf['jumpedFrom'].unique().tolist()))
        asnsDropdownData = list(set(asnsDropdownData + anomalous_asns)) if 'anomalous_asns' in locals() else asnsDropdownData
        
        changeDf.loc[changeDf['jumpedFrom'] == 0] = 'No data'
        fig = buildSankey(sitesState, asnState, changeDf)
        container_style = {"display": "block"}

    return [sitesDropdownData, asnsDropdownData, dcc.Graph(figure=fig), dataTables, container_style]


def create_anomalies_heatmap(period):
    df = pq.readFile('parquet/frames/ASN_path_anomalies.parquet')
    # df = df[df['to_date'] >= period[0]]
    print(period, len(df))
    # Aggregate the number of connections for the heatmap
    heatmap_data = df.groupby(['src_netsite', 'dest_netsite'])['asn_count'].sum().reset_index(name='count')

    # Pivot the data to create a matrix
    heatmap_pivot = heatmap_data.pivot(index='src_netsite', columns='dest_netsite', values='count').fillna(0)

    # Plot with the custom color scale
    fig = px.imshow(
        heatmap_pivot,
        labels=dict(x="Destination", y="Source", color="Count"),
        color_continuous_scale="BuPu",
        text_auto=True
    )

    # Update layout for better appearance
    fig.update_layout(
        xaxis_title="Destination",
        yaxis_title="Source",
        autosize=True,  # Enable responsive autosizing
        height=600,    # Let the container height define the plot size
        width=None,     # Let the container width define the plot size
        # margin=dict(t=10, b=180, l=70, r=0),
        coloraxis_showscale=True
    )

    return dcc.Graph(figure=fig)



# '''Takes the sites from the dropdown list and generates a Dash datatable'''
def generate_tables(frame, pivotFrames, event, alarmsInst):
    ids = pivotFrames['id'].values
    dfr = frame[frame.index.isin(ids)]
    dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)

    element = html.Div([
        html.Br(),
        html.H3(event.upper()),
        dash_table.DataTable(
            data=dfr.to_dict('records'),
            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
            markdown_options={"html": True},
            id=f'paths-search-tbl-{event}',
            page_current=0,
            page_size=20,
            style_cell={
                'padding': '2px',
                'whiteSpace': 'pre-line',
                "font-size": "1.4rem",
            },
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_data={
                'height': 'auto',
                'lineHeight': '1.5rem',
                'overflowX': 'auto'
            },
            style_table={
                    'overflowY': 'auto',
                    'overflowX': 'auto'
            },
            filter_action="native",
            filter_options={"case": "insensitive"},
            sort_action="native",
        ),
    ], className='single-table')

    return element


# '''Creates a list of labels with network providers'''
def addNetworkOwners(df, labels):
  asns = list(set(df['jumpedFrom'].unique().tolist() +
              df['diff'].unique().tolist()))
  owners = qrs.getASNInfo(asns)

  customdata = []
  for l in labels:
    if l in owners.keys():
      customdata.append(f'<b>{l}</b>: {owners[l]}')
    else:
      customdata.append(l)

  return customdata


# ''' Prepares the data for the Sankey diagram'''
def data4Sankey(sandf):
    typical = [f't{n}' for n in sandf['jumpedFrom'].unique().tolist()]
    diff = [f'd{n}' for n in sandf['diff'].unique().tolist()]
    src = [f'src_{n}' for n in sandf['src_site'].unique().tolist()]
    dst = [f'dst_{n}' for n in sandf['dest_site'].unique().tolist()]
    nodes = src + dst + typical + diff
    nodesdict = {n: i for i, n in enumerate(nodes)}

    labels = [n.replace('src_', '').replace('dst_', '').replace(
        'd', '').replace('t', '') for n in nodes]
    customdata = addNetworkOwners(sandf, labels)

    so, ta, vals, tuples = [], [], [], []
    for index, row in sandf.iterrows():
        typ = f"t{row['jumpedFrom']}"
        diff = f"d{row['diff']}"
        ssite = f"src_{row['src_site']}"
        dsite = f"dst_{row['dest_site']}"
        cnt = row['count']
        if (ssite, typ) not in tuples:
            so.append(nodesdict[ssite])
            ta.append(nodesdict[typ])
            vals.append(cnt)
            tuples.append((ssite, typ))

        if (typ, diff) not in tuples:
            so.append(nodesdict[typ])
            ta.append(nodesdict[diff])
            vals.append(cnt)
            tuples.append((typ, diff))

        if (diff, dsite) not in tuples:
            so.append(nodesdict[diff])
            ta.append(nodesdict[dsite])
            vals.append(cnt)
            tuples.append((diff, dsite))

    return labels, so, ta, vals, customdata


# '''Creates a Sankey diagram'''
def buildSankey(sitesState, asnState, df):

    if len(sitesState) > 0 and len(asnState) > 0:
        df = df[((df['src_site'].isin(sitesState)) |
                (df['dest_site'].isin(sitesState))) & ((df['jumpedFrom'].isin(asnState)) |
                (df['diff'].isin(asnState)))]

    elif len(sitesState) > 0:
        df = df[(df['src_site'].isin(sitesState)) |
                (df['dest_site'].isin(sitesState))]
    elif len(asnState) > 0:
        df = df[(df['jumpedFrom'].isin(asnState)) |
                (df['diff'].isin(asnState))]

    labels, sources, targets, vals, customdata = data4Sankey(df)

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="grey", width=0.5),
            label=labels,
            customdata=customdata,
            hovertemplate='%{customdata}<extra></extra>',
            color="rgb(4, 111, 137)"
        ),
        link=dict(
            # indices correspond to labels
            source=sources,
            target=targets,
            value=vals
        ))])

    for x_coordinate, column_name in enumerate(["Source site", "Previously used ASN", "New ASN", "Desination site"]):
        fig.add_annotation(
            x=x_coordinate,
            y=1.15,
            xref="x",
            yref="paper",
            text=column_name,
            showarrow=False,
            font=dict(
                size=16,
            ),
            #   align="center",
        )

    fig.update_layout(
        height=600,
        # title_text=f"Short term path deviations between sites",
        xaxis={
            'showgrid': False,  # thin lines in the background
            'zeroline': False,  # thick line at x=0
            'visible': False,  # numbers below
        },
        yaxis={
            'showgrid': False,  # thin lines in the background
            'zeroline': False,  # thick line at x=0
            'visible': False,  # numbers below
        },
        # margin=dict(b=2, l=0, r=0),
        plot_bgcolor='rgba(0,0,0,0)',
        font_size=10)

    return fig
