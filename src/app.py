import pandas as pd
import plotly_express as px
import plotly.express as px
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output

import getpass
import DatasetBuilder as build


def LoadBubbleChartData(isDev):
    if isDev:
        mdf = pd.read_csv('data.csv')
    else:
        mdf = build.BubbleChartDataset()
    return mdf


username = getpass.getuser()
isDev = True if username == 'petya' else False

df = LoadBubbleChartData(isDev)

host_df = CountTestsGroupedByHost()

def generate_table(dataframe, max_rows=1000):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])


app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)

app.layout = html.Div(
    generate_table(host_df)
#     [
#         html.H3("Demo: Plotly Express in Dash"),

#         html.Div(
#             dcc.Graph(figure=px.scatter(df, x="period", y="host",
#                                         size=df["mean"].fillna(value=0), color="host",
#                                         hover_name="host", size_max=45, height=700)
#                       .update(layout={
#                           'title': 'Avg Packet Loss from 01-12-2019 to 22-01-2020',
#                           'xaxis': {'title': 'Period'},
#                           'yaxis': {'title': 'Hosts'},
#                           'paper_bgcolor': 'rgba(0,0,0,0)',
#                           'plot_bgcolor': 'rgba(0,0,0,0)'
#                       }),
#                       ),)
#     ]
)

app.run_server(port=80, host='0.0.0.0', debug=False)
