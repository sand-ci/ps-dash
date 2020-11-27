colors = {'warning': '#f7dba7',
          'ok': '#9bc191'}

host_table_cell = [
                    {
                        'if': {
                            'column_id': 'id',
                        },
                        'display': 'none'
                    }
                ]

gen_info_table_cell = [
                    {
                        'if': {
                            'column_id': 'ip',
                        },
                        'width': '300px'
                    }
                ]

host_table_cond = [
            {
                'if': {
                    'column_id': 'packet_loss-total_dests',
                    'filter_query': '{packet_loss-total_dests} < {total_num_of_dests}'
                },
                'backgroundColor': colors['warning'],
                'color': 'black',
            },
            {
                'if': {
                    'column_id': 'packet_loss-total_dests',
                    'filter_query': '{packet_loss-total_dests} eq {total_num_of_dests}'
                },
                'backgroundColor': colors['ok'],
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'owd-total_dests',
                    'filter_query': '{owd-total_dests} < {total_num_of_dests}'
                },
                'backgroundColor': colors['warning'],
                'color': 'black',
            },
            {
                'if': {
                    'column_id': 'owd-total_dests',
                    'filter_query': '{owd-total_dests} eq {total_num_of_dests}'
                },
                'backgroundColor': colors['ok'],
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'delay_mean',
                    'filter_query': '{delay_mean} < 0'
                },
                'backgroundColor': colors['warning'],
                'color': 'black',
            },
            {
                'if': {
                    'column_id': 'packet_loss',
                    'filter_query': '{packet_loss} > 0.1'
                },
                'backgroundColor': colors['warning'],
                'color': 'black',
            },
            {
                'if': {
                    'column_id': 'packet_loss',
                    'filter_query': '{packet_loss} eq 1'
                },
                'backgroundColor': colors['warning'],
                'color': 'black',
            }
        ]

host_table_header = {
    'backgroundColor': 'rgb(230, 230, 230)',
    'fontWeight': 'bold',
    'font-family':'emoji'
}

bubble_chart_layout = {
                      'title':'Average packet loss from 01-12-2019 to 22-01-2020',
                      'xaxis':{'title':'Period in days'},
                      'yaxis':{'title':'Hosts'},
                      'paper_bgcolor':'rgba(0,0,0,0)',
                      'plot_bgcolor':'rgba(0,0,0,0)',
                      'font': {'size': 10}
                  }

