import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd

import plotly.graph_objects as go

import utils.helpers as hp
import model.queries as qrs
from model.Alarms import Alarms
from utils.parquet import Parquet



def title():
    return f"Search & explore"



def description(q=None):
    return f"Explore the 'Path changed' alarms"



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
            dbc.Col(
                dcc.Loading(
                    html.Div(id="paths-alarms-sunburst"),
                    style={'height': '0.5rem'}, color='#00245A'),
                align="start", width='8', className="mr-2"),
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H1(f"Search the \"Path changed\" alarms",
                                className="l-h-3 pl-2"),
                        html.P(
                            f'Alarms generated in the period: {period[0]} - {period[1]} ',
                            style={"padding-left": "1.5%","font-size": "14px"}),
                        html.P('Rounded to the day', style={"padding-left": "1.5%"})
                    ], width=10, align="center", className="text-left pair-details rounded-border-1"),
                    
                ], justify="start", align="center"),
                html.Br(),
                html.Br(),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-sites-dropdown',
                                     placeholder="Search for a site"),
                    ], width=10),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-asn-dropdown',
                                     placeholder="Search ASNs"),
                    ], width=10),
                ]),
                
            ]),
        ], className="p-1 site boxwithshadow page-cont mb-1 g-0", justify="center", align="center"),
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
    ], className='g-0 main-cont', align="start", style={"padding": "0.5% 1.5%"})


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
        Output('paths-alarms-sunburst', 'children'),
        Output('paths-results-table', 'children'),
    ],
    [
        Input("paths-asn-dropdown", "search_value"),
        Input("paths-asn-dropdown", "value"),
        Input("paths-sites-dropdown", "search_value"),
        Input("paths-sites-dropdown", "value"),
    ])
def update_output(asn, asnState, sites, sitesState):

    # that period should match the one in the layout, 
    # as well as the range of the cached data
    period = hp.defaultTimeRange(3)

    sitesState = [] if sitesState is None else sitesState
    asnState = [] if asnState is None else asnState

    alarmsInst = Alarms()
    frames, pivotFrames = alarmsInst.loadData(period[0], period[1])
    frames = {'path changed between sites': frames['path changed between sites'],
              'path changed': frames['path changed']}
    pivotFrames = {'path changed between sites': pivotFrames['path changed between sites'],
                   'path changed': pivotFrames['path changed'], }


    df =  pivotFrames['path changed between sites']
    scntdf = df[df['tag'] != ''].groupby('tag')[['id']].count().reset_index().rename(columns={'id': 'cnt', 'tag': 'site'})

    # sites
    graphData = scntdf.copy()
    graphData = graphData[graphData['site'].isin(sitesState)]

    sitesDropdownData = []
    for s in sorted(pivotFrames['path changed'].tag.unique().tolist()):
        sitesDropdownData.append({"label": s.upper(), "value": s.upper()})

    # data tables
    dataTables = []
    for event in sorted(['path changed','path changed between sites']):
        df = pivotFrames[event]
        
        df = df[df['tag'].isin(sitesState)] if len(sitesState) > 0 else df
        if 'diff' in df.columns and len(asnState) > 0:
            df = df[df['diff'].isin(asnState)]
        elif 'asn' in df.columns and len(asnState) > 0:
            df = df[df['asn'].isin(asnState)]
        
        if 'src_site' in df.columns and 'dest_site' in df.columns and len(sitesState) > 0:
            df = df[(df['src_site'].isin(sitesState)) | (df['dest_site'].isin(sitesState))]

        if len(df) > 0:
            dataTables.append(generate_tables(frames[event], df, event, alarmsInst))
    
    if len(dataTables)==0:
        dataTables.append(html.P(f'There are no alarms related to the selected criteria', 
                                style={"padding-left": "1.5%", "font-size": "14px"}))
    
    dataTables = html.Div(dataTables)


    # graph
    pq = Parquet()
    changeDf = pq.readFile('parquet/frames/prev_next_asn')
    asnsDropdownData = []

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
        
        changeDf.loc[changeDf['jumpedFrom'] == 0] = 'No data'
        fig = buildSankey(sitesState, asnState, changeDf)

    return [sitesDropdownData, asnsDropdownData, dcc.Graph(figure=fig), dataTables]


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
            filter_action="native",
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
            color="blue"
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
        # title_text=f"Track path changes related to ASN {asn}",
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

        plot_bgcolor='rgba(0,0,0,0)', font_size=10)

    # fig.show()
    return fig
