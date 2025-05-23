import dash
from dash import html
import dash_bootstrap_components as dbc

from datetime import datetime
import urllib3

import utils.helpers as hp
from utils.helpers import DATE_FORMAT
from model.Alarms import Alarms
import model.queries as qrs

from utils.parquet import Parquet
from utils.components import loss_delay_kibana
urllib3.disable_warnings()



def title(q=None):
    return f"Latency alarm {q}"



def description(q=None):
    return f"Visual represention on a selected packetloss alarms {q}"



dash.register_page(
    __name__,
    path_template="/loss-delay/<q>",
    title=title,
    description=description,
)



def layout(q=None, **other_unknown_query_strings):
    if q:
      alarm = qrs.getAlarm(q)
      print('URL query:', q)
      print()
      print('Alarm content:', alarm)
      alarmsInst = Alarms()
      alrmContent = alarm['source']
      event = alarm['event']
      kibana_row = loss_delay_kibana(alrmContent, event)
      return html.Div([
              dbc.Row([
                dbc.Row([
                  dbc.Col([
                    html.H3(event.upper(), className="text-center bold"),
                    html.H5(alrmContent['to'], className="text-center bold"),
                  ], lg=2, md=12, className="p-3"),
                  dbc.Col(
                      html.Div(
                        [
                          dbc.Row([
                            dbc.Row([
                              html.H1(f"Summary", className="text-left"),
                              html.Hr(className="my-2")]),
                            dbc.Row([
                                html.P(alarmsInst.buildSummary(alarm), className='subtitle'),
                              ], justify="start"),
                            ])
                        ],
                      ),
                  lg=10, md=12, className="p-3")
                ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center")
              ], style={"padding": "0.5% 1.5%"}, className='g-0'),
            kibana_row,
      ], className='mb-5')



