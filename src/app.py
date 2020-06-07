from urllib.parse import urlparse, parse_qs

import plotly.express as px
import flask
import dash
import dash_table
import dash_daq as daq
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import pandas as pd
from datetime import datetime, timedelta

import templates as tmpl
import DatasetBuilder as build

import index
import pair_plots_page
import subplots
import removed_hosts

build.StartCron()

external_stylesheets = [dbc.themes.BOOTSTRAP]
server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True, prevent_initial_callbacks=True)

def serve_layouts(layout_dict):
    def serve_layouts_closure():
        if not flask.has_request_context():
            return html.Div()

        referer = flask.request.headers.get('Referer', '')

        path = urlparse(referer).path
        pages = path.split('/')

        query = ''
        query_string = urlparse(referer).query
        if query_string:
            query = parse_qs(query_string)

        if len(pages) < 2 or not pages[1]:
            return layout_dict['index']

        page = pages[1]
        if page in layout_dict:
            if page == 'pair':
                return layout_dict[page](query)
            else: return layout_dict[page]

    return serve_layouts_closure

# Here we pass the URL query parameters to the pair-plot page
def show_pair_plots(pair):
    dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    dateFrom = datetime.strftime(datetime.now() - timedelta(hours = 24), '%Y-%m-%d %H:%M')
    src = pair['src'][0]
    dest = pair['dest'][0]

    data = build.LossDelaySubplotData(dateFrom, dateTo, [src, dest])
    fig = subplots.PacketlossDelaySubplots(data = data, title = str("Report for "+src+" -> "+dest+" Period: "+dateFrom+"  -  "+dateTo))

    return pair_plots_page.GenerateLayout(fig)


app.layout = serve_layouts({'index': index.layout,
                            'pair': show_pair_plots,
                            'removed': removed_hosts.layout})



@app.callback([Output('count', 'children'),
               Output('datatable-row-ids', 'data'),
               Output('datatable-row-ids', 'columns'),
               Output('loader1', 'loading_state')],
              [Input('radioitems-period', 'value'),
               Input('read_from_db', 'value')],
             prevent_initial_call=False)
def update_hostsTable(period, read_from_db):
    if (read_from_db == True):
        dateTo = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
        dateFrom = datetime.strftime(datetime.now() - timedelta(hours = period), '%Y-%m-%d %H:%M')
        df_tab = build.LossDelayTestCountGroupedbyHost(dateFrom, dateTo)
    else:
        df_tab = pd.read_csv("data/LossDelayTestCountGroupedbyHost-"+str(period)+".csv")
    df_tab['id'] = df_tab['host']
    df_tab.set_index('host', inplace=True, drop=False)
    columns=[{"name": i, "id": i} for i in df_tab.columns]
    return u'''Number of hosts for the period: {}'''.format(len(df_tab)), df_tab.to_dict('records'), columns, False


@app.callback(
    [Output('selected-host', 'children'),
     Output('selected', 'style'),
     Output('description', 'style'),
     Output('as_src_txt', 'style'),
     Output('as_dest_txt', 'style'),],
    [Input('datatable-row-ids', 'active_cell')])
def get_host(active_cell):
    active_row_id = active_cell['row_id'] if active_cell else None
    if active_row_id:
        show_css = {'display': 'block', 'padding': '0 5px'} 
        return active_row_id, show_css, show_css, show_css, show_css
    else: raise dash.exceptions.PreventUpdate

@app.callback(
     [Output('datatable-src', 'data'),
      Output('datatable-src', 'columns'),
      Output('datatable-dest', 'data'),
      Output('datatable-dest', 'columns'),
      Output('loader', 'loading_state')],
     [Input('selected-host', 'children')])
def get_details(host):
    if host is not None:
        data = build.SrcDestTables(host)
        as_source = data[0]
        as_destination = data[1]
        columns = [{"name": i, "id": i} for i in as_source.columns]
        return as_source.to_dict('records'), columns, as_destination.to_dict('records'), columns, False
    else:
        raise dash.exceptions.PreventUpdate

@app.callback(
    [Output('change-url', 'href')],
    [Input('datatable-src', 'active_cell'), Input('datatable-src', 'data'),
     Input('datatable-dest', 'active_cell'), Input('datatable-dest', 'data')])
def changeURL(active_cell_src, src_data, active_cell_dest, dest_data):
    if active_cell_src is not None and src_data is not None:
        df = pd.DataFrame(src_data)
        pair = df.loc[active_cell_src['row']][['src_host', 'dest_host']].to_list()
        return [str("pair/?src="+pair[0]+"&dest="+pair[1])]
    elif active_cell_dest is not None and dest_data is not None:
        df = pd.DataFrame(dest_data)
        pair = df.loc[active_cell_dest['row']][['src_host', 'dest_host']].to_list()
        return [str("pair/?src="+pair[0]+"&dest="+pair[1])]
    else:
        raise dash.exceptions.PreventUpdate


if __name__ == '__main__':
    app.run_server()