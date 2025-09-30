import dash
from dash import html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, MATCH, State


import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3


import utils.helpers as hp
from utils.helpers import timer
from model.Alarms import Alarms
import model.queries as qrs
from utils.components import bandwidth_increased_decreased, throughput_graph_components
from utils.utils import getSitePairs, getRawDataFromES


urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def title(q=None):
    return f"Throughput alarm {q}"



def description(q=None):
    return f"Visual represention on a selected througput alarm {q}"



dash.register_page(
    __name__,
    path_template="/throughput/<q>",
    title=title,
    description=description,
)


alarmsInst = Alarms()

def layout(q=None, **other_unknown_query_strings):
  if q:
    alarm = qrs.getAlarm(q)
    print('URL query:', q)
    print()

    sitePairs = getSitePairs(alarm)
    alarmData = alarm['source']
    dateFrom, dateTo = hp.getPriorNhPeriod(alarmData['to'])
    print('Alarm\'s content:', alarmData)
    pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]

    data = alarmsInst.getOtherAlarms(
                                    currEvent=alarm['event'],
                                    alarmEnd=alarmData['to'],
                                    pivotFrames=pivotFrames,
                                    site=alarmData['site'] if 'site' in alarmData.keys() else None,
                                    src_site=alarmData['src_site'] if 'src_site' in alarmData.keys() else None,
                                    dest_site=alarmData['dest_site'] if 'dest_site' in alarmData.keys() else None
                                    )

    otherAlarms = alarmsInst.formatOtherAlarms(data)

    expand = True
    alarmsIn48h = ''
    if alarm['event'] not in ['bandwidth decreased', 'bandwidth increased']:
      expand = False
      alarmsIn48h = dbc.Row([
                      dbc.Row([
                            html.P(f'Site {alarmData["site"]} takes part in the following alarms in the period 24h prior \
                              and up to 24h after the current alarm end ({alarmData["to"]})', className='subtitle'),
                            html.B(otherAlarms, className='subtitle')
                        ], className="boxwithshadow alarm-header pair-details g-0 p-3", justify="between", align="center"),
                    ], style={"padding": "0.5% 1.5%"}, className='g-0')


    return bandwidth_increased_decreased(alarm, alarmData, alarmsInst,alarmsIn48h, sitePairs, expand)


@dash.callback(
    [
      Output({'type': 'tp-collapse', 'index': MATCH},  "is_open"),
      Output({'type': 'tp-collapse', 'index': MATCH},  "children")
    ],
    [
      Input({'type': 'tp-collapse-button', 'index': MATCH}, "n_clicks"),
      Input({'type': 'tp-collapse-button', 'index': MATCH}, "value"),
      Input('alarm-store', 'data')
    ],
    [State({'type': 'tp-collapse', 'index': MATCH},  "is_open")],
)
def toggle_collapse(n, alarmData, alarm, is_open):
  data = ''

  dateFrom, dateTo = hp.getPriorNhPeriod(alarm['source']['to'])
  pivotFrames = alarmsInst.loadData(dateFrom, dateTo)[1]
  if n:
    if is_open==False:
      data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'], pivotFrames)
    return [not is_open, data]
  if is_open==True:
    data = buildGraphComponents(alarmData, alarm['source']['from'], alarm['source']['to'], alarm['event'], pivotFrames)
    return [is_open, data]
  return [is_open, data]



@timer
def buildGraphComponents(alarmData, dateFrom, dateTo, event, pivotFrames):

  df = getRawDataFromES(alarmData['src_site'], alarmData['dest_site'], alarmData['ipv6'], dateFrom, dateTo)
  df.loc[:, 'MBps'] = df['throughput'].apply(lambda x: round(x/1e+6, 2))

  data = alarmsInst.getOtherAlarms(currEvent=event, alarmEnd=dateTo, pivotFrames=pivotFrames,
                                   src_site=alarmData['src_site'], dest_site=alarmData['dest_site'])
  otherAlarms = alarmsInst.formatOtherAlarms(data)
  return throughput_graph_components(alarmData, df, otherAlarms)
