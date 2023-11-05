import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from plotly.tools import mpl_to_plotly

from datetime import datetime
from datetime import timedelta
from datetime import date
import pandas as pd
import matplotlib.pyplot as plt
from elasticsearch.helpers import scan

import utils.helpers as hp

import urllib3
urllib3.disable_warnings()

from ml.packet_loss_one_month_onehot import one_month_data
from ml.create_packet_loss_dataset import createPcktDataset
from ml.packet_loss_train_model import packet_loss_train_model
from ml.packet_loss_preprocess_data import packet_loss_preprocess

def title():
    return f"Search & explore"

def description(q=None):
    return f"Packet loss alarms using ML"

dash.register_page(
    __name__,
    path_template="/ml-alarms/packet-loss",
    title=title,
    description=description,
)

def convertTime(ts):
    stripped = datetime.strptime(ts, '%Y-%m-%d %H:%M')
    return int((stripped - datetime(1970, 1, 1)).total_seconds()*1000)

def layout(**other_unknown_query_strings):
    now = hp.defaultTimeRange(days=60, datesOnly=True)

    #Packet loss alarms page
    return \
    dbc.Nav(
        [
            dbc.NavItem(dbc.NavLink("bandwidth alarms", href="/ml-alarms/throughput", id='')),
            html.Div(style={'padding-left': '10px', 'background-color': 'white'}),
            dbc.NavItem(dbc.NavLink("packet loss alarms", href="/ml-alarms/packet-loss", id='')),
        ], fill=True, justified=True, id='navbar',style={'margin-left': '10px', 'margin-right': '10px'}
    ), \
    dbc.Row([
        dbc.Row([
            dbc.Row([
                dbc.Col([
                    html.H1(f"Packet loss alarms using ML", className="p-1"),
                ], align="center", className="text-left pair-details rounded-border-1")
            ], justify="start", align="center"),
            html.Br(),
            html.Br(),
            dbc.Row([
                html.H4('Note: choose a period of time between two and six month for the best analysis', style={"padding-top":"1%"}),
                dcc.DatePickerRange(
                    id='date-picker-range-pl',
                    month_format='YYYY-MMM-DD',
                    min_date_allowed=date(2022, 8, 1),
                    initial_visible_month=now[0],
                    start_date=now[0],
                    end_date=now[1]
                ),
                html.P()
            ]),
            html.Br(),
        html.Br(),
            dbc.Row([
                html.H1(f"List of alarms for the period selected", className="text-center", style={"padding-top": "1%"}),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div([
                        dash_table.DataTable(
                            id="results-list-pl",
                            # style_as_list_view=True,
                            style_header={'border': '1px solid grey'},
                            style_cell={'padding-right': '10px', 'padding-left': '10px', 'padding-bottom': '10px', 'padding-top': '10px',
                                        'border-top': '1px solid grey', 'border-bottom': '1px solid grey'},
                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': '#ebf1fd',
                                }
                            ],
                        ),
                    ],style={'font-size': '14px', "overflow": "scroll"}),
                style={'height': '0.5rem','font-size': '14px', "margin-bottom": "2%"}, color='#00245A')

            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0"),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H4('You can select a single site from the list above and see the more detailed analysis for it.',
                        style={"padding-bottom": "1%"}),
                dbc.Col([
                    dcc.Dropdown(multi=False, id='sites-dropdown-pl', placeholder="Choose a specific site for analysis"),
                ], width=5),
            ]),
        html.Br(),
            dbc.Row([
                html.H1(f"Daily numbers of the alarms created", className="text-center", style={"padding-top": "1%"}),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-mean'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),

        html.Br(),

            dbc.Row([
                html.H1(f"Alarms from the site as a source and a destination", className="text-center"),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl'),
                style={'height':'0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0", justify="center", align="center"),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H4("Now if the plot above shows a visible period of time with constant packet loss,"
                        " than there's probably something wrong with the site itself. If not then try and look below at the separate cases when the site "
                        "selected is either a source or destination ONLY. There can be a case when the increased packet loss period can be seen only "
                        "at the cases where the chosen site is a destination. Then the problem probably resides somewhere on the node near that site. "
                        "On the other hand, if the the increased packet loss period is seen only when the selected site is a source, then the problem probably occurs "
                        "while trying to connect to multiple sites and is located somewhere on the common way to them from this site. The problem "
                        "may be in bad optics or bad nodes on the way, bad electronics in the measuring equipment etc."
                        # " See the list of all source-destination pairs with faulty measurements below the corresponding graphs to see where the problem may reside."
                        ,
                         style={"padding-bottom": "1%"}),
            ]),
        html.Br(),
            dbc.Row([
                html.H1(f"Alarms from the site as a source only", className="text-center", style={"padding-top": "1%"}),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-src'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        html.Br(),
            dbc.Row([
                html.H1(f"Daily numbers of the alarms created", className="text-center", style={"padding-top": "1%"}),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-mean-src'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0", justify="center", align="center"),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H1(f"Alarms from the site as a destination only", className="text-center"),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-dest'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        html.Br(),
            dbc.Row([
                html.H1(f"Daily numbers of the alarms created", className="text-center", style={"padding-top": "1%"}),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-mean-dest'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0", justify="center", align="center"),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H4("Finaly you can play around by setting both a source and destination sites and plotting a graph for them:",
                        style={"padding-bottom": "1%"}),
            ]),
            dbc.Col([
                dcc.Dropdown(multi=False, id='sites-dropdown-pl-src', placeholder="Choose a source site for analysis"),
            ], width=5),
            dbc.Col([
                dcc.Dropdown(multi=False, id='sites-dropdown-pl-dest', placeholder="Choose a destination site for analysis"),
            ], width=5, style={"padding-left": "1%"}),
            html.Br(),
            dbc.Row([
                html.H1(f"Alarms from the chosen source-destination pair", className="text-center"),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-dest-src'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2", style={"padding-top": "1%"}),
            dbc.Row([
                html.H1(f"Daily numbers of the alarms created", className="text-center", style={"padding-top": "1%"}),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-pl-mean-dest-src'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0", align="center"),
        html.Br(),
    ], className='g-0 main-cont', align="start", style={"padding": "0.5% 1.5%"})

def colorMap(eventTypes):
  colors = ['#75cbe6', '#3b6d8f', '#75E6DA', '#189AB4', '#2E8BC0', '#145DA0', '#05445E', '#0C2D48',
          '#5EACE0', '#d6ebff', '#498bcc', '#82cbf9', 
          '#2894f8', '#fee838', '#3e6595', '#4adfe1', '#b14ae1'
          '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
          ]

  paletteDict = {}
  for i,e in enumerate(eventTypes):
      paletteDict[e] = colors[i]
  
  return paletteDict

# a callback for the first section of a page with the list of Major alarms
@dash.callback(
    [
        # the list of all the major alarms
        Output(component_id="results-list-pl", component_property="data"),
        Output(component_id="results-list-pl", component_property='columns'),

        # the list of all the sites (which can be both a source and dest) to use later for plotting
        Output("sites-dropdown-pl", "options"),
        # the list of all the source sites to use later for plotting
        Output("sites-dropdown-pl-src", "options"),
        # the list of all the dest sites to use later for plotting
        Output("sites-dropdown-pl-dest", "options"),
    ],
    [
      Input('date-picker-range-pl', 'start_date'),
      Input('date-picker-range-pl', 'end_date'),
    ],
    State("sites-dropdown-pl", "value"))
def update_output(start_date, end_date, sitesState):

    if start_date and end_date:
        start_date, end_date = [f'{start_date} 00:01', f'{end_date} 23:59']
    else: start_date, end_date = hp.defaultTimeRange(days=60, datesOnly=True)

    # query for the dataset
    plsDf = createPcktDataset(start_date, end_date)
    # plsDf = pd.read_csv('plsDf_jan_may.csv')

    # onehot encode the whole dataset and leave only one month for further ML training
    plsDf_onehot_month = one_month_data(plsDf)

    #train the model on one month data
    model = packet_loss_train_model(plsDf_onehot_month)
    del plsDf_onehot_month

    # predict the alarms using ML model and return the dataset with original alarms and the ML alarms
    global plsDf_onehot_plot, df_to_plot
    df_to_plot, plsDf_onehot_plot = packet_loss_preprocess(plsDf, model)
    del plsDf, model

    # create a list with all sites as sources
    src_sites = plsDf_onehot_plot.loc[:, plsDf_onehot_plot.columns.str.startswith("src_site")].columns.values.tolist()
    commonprefix = 'src_site_'
    src_sites = [x[len(commonprefix):] for x in src_sites]
    sdropdown_items = src_sites

    # create a list with all sites as destinations
    dest_sites = plsDf_onehot_plot.loc[:, plsDf_onehot_plot.columns.str.startswith("dest_site")].columns.values.tolist()
    commonprefix = 'dest_site_'
    dest_sites = [x[len(commonprefix):] for x in dest_sites]
    dest_dropdown_items = dest_sites

    # create a list with all sites both as destinations and sources
    src_dest_sites = list(set(dest_sites + src_sites))

    # making Major alarms
    i = 0
    j, in_a_row = 0, 0
    alarms_list = []
    queue = []
    for site_name in src_dest_sites:
        try:
            df_to_plot_site = df_to_plot.loc[
                (df_to_plot['src_site_' + site_name] == 1) | (df_to_plot['dest_site_' + site_name] == 1)]

            alarm_nums = df_to_plot_site.groupby(df_to_plot_site.loc[(df_to_plot_site['flag'] == 1), 'dt'].dt.date)[
                "flag"].sum()
            alarm_nums_mean = alarm_nums.mean()

            for date, alarm_num in alarm_nums.items():
                if alarm_num > alarm_nums_mean * 5:
                    # print(site_name, 'alarm mean:', alarm_nums_mean)
                    # print(alarm_num, 'alarms on', site_name, date)
                    in_a_row += 1
                    if in_a_row == 1:
                        queue.append(site_name)
                        queue.append(str(alarm_num) + ' alarms at ' + str(date))
                    else:
                        queue.append(str(alarm_num) + ' alarms at ' + str(date))

                    if in_a_row == 3:
                        alarms_list.append(queue)
                        # print(alarm_num, 'alarms on', site_name, date)
                else:
                    in_a_row = 0
                    queue = []
            i += 1
        except:
            j += 1

    print("\nnumber of successful occurrences of host being both a src and dest:", i)
    print("number of unsuccessful occurrences of host being both a src and dest:", j)

    # making a pretty df and preparing it for converting to a plotly DataTable
    data = pd.DataFrame(alarms_list).fillna(value='-')
    data_dict = data.to_dict('records')
    columns = [{"name": str(i), "id": str(i)} for i in data]

    # sort the sites list
    src_dest_sites.sort()
    return [data_dict, columns, src_dest_sites, sdropdown_items, dest_dropdown_items]

# a callback for the second section of a page with all the automatic plots for the chosen site
@dash.callback(
    [
        Output('results-table-pl', 'children'),
        Output('results-table-pl-src', 'children'),
        Output('results-table-pl-dest', 'children'),
        Output('results-table-pl-mean', 'children'),
        Output('results-table-pl-mean-src', 'children'),
        Output('results-table-pl-mean-dest', 'children'),
    ],
    [
      Input('date-picker-range-pl', 'start_date'),
      Input('date-picker-range-pl', 'end_date'),
      Input("sites-dropdown-pl", "search_value"),
      Input("sites-dropdown-pl", "value"),
    ],
    State("sites-dropdown-pl", "value"))
def update_analysis(start_date, end_date, sites, allsites, sitesState):

    start_date, end_date = [f'{start_date} 00:01', f'{end_date} 23:59']
    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M')

    # creating a global layout for the plots
    global layout, layout_mean
    layout = dict(xaxis_range=[start_date - timedelta(days=2), end_date + timedelta(days=2)],
            showlegend=True,
            margin=dict(l=5, r=5, t=50, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ))
    layout_mean = layout.copy()
    layout_mean.pop('showlegend','legend')

    # creating the first plot
    plotly_fig = {}
    if (sitesState is not None and len(sitesState) > 0):
        plsDf_onehot_site_plot = plsDf_onehot_plot.loc[
            (plsDf_onehot_plot['src_site_' + allsites] == 1) | (plsDf_onehot_plot['dest_site_' + allsites] == 1)]
        df_to_plot_site = df_to_plot.loc[
            (df_to_plot['src_site_' + allsites] == 1) | (df_to_plot['dest_site_' + allsites] == 1)]

        fig = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms for the ' + allsites + ' site')
        plt.xlabel('timestamp')
        plt.ylabel('packet loss')

        plt.plot(plsDf_onehot_site_plot['dt'], plsDf_onehot_site_plot['avg_value'], 'co',
                 label="all packet loss measurements")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 2, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 2, 'avg_value'], 'go',
                 label="complete loss alarms using ML")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 1, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 1, 'avg_value'], 'ro',
                 label="partial loss alarms using ML")
        plt.plot(df_to_plot_site.groupby(df_to_plot_site['dt'].dt.date)["avg_value"].mean(),
                 label='daily packet loss mean')

        plotly_fig = mpl_to_plotly(fig)
        plotly_fig.update_layout(layout)

    plotly_fig_mean = {}
    if (sitesState is not None and len(sitesState) > 0):
        fig_mean = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms aggregated by days for the ' + allsites + ' site')
        plt.xlabel('timestamp')
        plt.ylabel('number of daily alarms')

        plt.plot(df_to_plot_site.groupby(df_to_plot_site.loc[(df_to_plot_site['flag'] == 1), 'dt'].dt.date)["flag"].sum())
        plotly_fig_mean = mpl_to_plotly(fig_mean)
        plotly_fig_mean.update_layout(layout_mean)

    plotly_fig_src = {}
    if (sitesState is not None and len(sitesState) > 0):
        plsDf_onehot_site_plot = plsDf_onehot_plot.loc[(plsDf_onehot_plot['src_site_' + allsites] == 1)]
        df_to_plot_site = df_to_plot.loc[(df_to_plot['src_site_' + allsites] == 1)]

        fig_src = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms for the ' + allsites + ' site as a source only')
        plt.xlabel('timestamp')
        plt.ylabel('packet loss')

        plt.plot(plsDf_onehot_site_plot['dt'], plsDf_onehot_site_plot['avg_value'], 'co',
                 label="all packet loss measurements")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 2, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 2, 'avg_value'], 'go',
                 label="complete loss alarms using ML")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 1, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 1, 'avg_value'], 'ro',
                 label="partial loss alarms using ML")
        plt.plot(df_to_plot_site.groupby(df_to_plot_site['dt'].dt.date)["avg_value"].mean(),
                 label='daily packet loss mean')

        plotly_fig_src = mpl_to_plotly(fig_src)
        plotly_fig_src.update_layout(layout)

    plotly_fig_mean_src = {}
    if (sitesState is not None and len(sitesState) > 0):
        fig_mean = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms aggregated by days for the ' + allsites + ' site')
        plt.xlabel('timestamp')
        plt.ylabel('number of daily alarms')

        plt.plot(df_to_plot_site.groupby(df_to_plot_site.loc[(df_to_plot_site['flag'] == 1), 'dt'].dt.date)["flag"].sum())
        plotly_fig_mean_src = mpl_to_plotly(fig_mean)
        plotly_fig_mean_src.update_layout(layout_mean)

    plotly_fig_dest = {}
    if (sitesState is not None and len(sitesState) > 0):

        plsDf_onehot_site_plot = plsDf_onehot_plot.loc[(plsDf_onehot_plot['dest_site_' + allsites] == 1)]
        df_to_plot_site = df_to_plot.loc[(df_to_plot['dest_site_' + allsites] == 1)]


        fig_dest = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms for the ' + allsites + ' site as a destination only')
        plt.xlabel('timestamp')
        plt.ylabel('packet loss')

        plt.plot(plsDf_onehot_site_plot['dt'], plsDf_onehot_site_plot['avg_value'], 'co',
                 label="all packet loss measurements")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 2, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 2, 'avg_value'], 'go',
                 label="complete loss alarms using ML")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 1, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 1, 'avg_value'], 'ro',
                 label="partial loss alarms using ML")
        plt.plot(df_to_plot_site.groupby(df_to_plot_site['dt'].dt.date)["avg_value"].mean(),
                 label='daily packet loss mean')

        plotly_fig_dest = mpl_to_plotly(fig_dest)
        plotly_fig_dest.update_layout(layout)

    plotly_fig_mean_dest = {}
    if (sitesState is not None and len(sitesState) > 0):
        fig_mean = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms aggregated by days for the ' + allsites + ' site')
        plt.xlabel('timestamp')
        plt.ylabel('number of daily alarms')

        plt.plot(df_to_plot_site.groupby(df_to_plot_site.loc[(df_to_plot_site['flag'] == 1), 'dt'].dt.date)["flag"].sum())
        plotly_fig_mean_dest = mpl_to_plotly(fig_mean)
        plotly_fig_mean_dest.update_layout(layout_mean)

    return [dcc.Graph(figure=plotly_fig),
            dcc.Graph(figure=plotly_fig_src),dcc.Graph(figure=plotly_fig_dest), dcc.Graph(figure=plotly_fig_mean),
            dcc.Graph(figure=plotly_fig_mean_src), dcc.Graph(figure=plotly_fig_mean_dest)]

# a callback for the third section of a page with two plots for a chosen destination-source pair
@dash.callback(
    [
        Output('results-table-pl-dest-src', 'children'),
        Output('results-table-pl-mean-dest-src', 'children'),
    ],
    [
        Input("sites-dropdown-pl-src", "value"),
        Input("sites-dropdown-pl-dest", "value"),
    ],
    State("sites-dropdown-pl-src", "value"),
    State("sites-dropdown-pl-dest", "value"))
def update_output(src_site, dest_site, sites_src_State, sites_dest_State):

    plotly_fig_scr_dest = {}
    if (sites_src_State is not None and len(sites_src_State) > 0) & (sites_dest_State is not None and len(sites_dest_State) > 0):
        plsDf_onehot_site_plot = plsDf_onehot_plot.loc[
            (plsDf_onehot_plot['src_site_' + src_site] == 1) & (plsDf_onehot_plot['dest_site_' + dest_site] == 1)]
        df_to_plot_site = df_to_plot.loc[
            (df_to_plot['src_site_' + src_site] == 1) & (df_to_plot['dest_site_' + dest_site] == 1)]

        fig = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms for the ' + src_site + ' and ' + dest_site + ' sites pair')
        plt.xlabel('timestamp')
        plt.ylabel('packet loss')

        plt.plot(plsDf_onehot_site_plot['dt'], plsDf_onehot_site_plot['avg_value'], 'co',
                 label="all packet loss measurements")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 2, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 2, 'avg_value'], 'go',
                 label="complete loss alarms using ML")
        plt.plot(df_to_plot_site.loc[df_to_plot['flag'] == 1, 'dt'],
                 df_to_plot_site.loc[df_to_plot_site['flag'] == 1, 'avg_value'], 'ro',
                 label="partial loss alarms using ML")
        plt.plot(df_to_plot_site.groupby(df_to_plot_site['dt'].dt.date)["avg_value"].mean(),
                 label='daily packet loss mean')

        plotly_fig_scr_dest = mpl_to_plotly(fig)
        plotly_fig_scr_dest.update_layout(layout)

    plotly_fig_mean_src_dest = {}
    if (sites_src_State is not None and len(sites_src_State) > 0) & (sites_dest_State is not None and len(sites_dest_State) > 0):
        fig_mean = plt.figure(figsize=(14, 4))
        plt.title('Packet loss alarms aggregated by days for the ' + src_site + ' and ' + dest_site + ' sites pair')
        plt.xlabel('timestamp')
        plt.ylabel('number of daily alarms')

        plt.plot(df_to_plot_site.groupby(df_to_plot_site.loc[(df_to_plot_site['flag'] == 1), 'dt'].dt.date)["flag"].sum())
        plotly_fig_mean_src_dest = mpl_to_plotly(fig_mean)
        plotly_fig_mean_src_dest.update_layout(layout_mean)

    return [dcc.Graph(figure=plotly_fig_scr_dest),dcc.Graph(figure=plotly_fig_mean_src_dest)]