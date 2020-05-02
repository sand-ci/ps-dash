host_table_cond = [
#             {
#                 'if': {'row_index': 'odd'},
#                 'backgroundColor': 'rgb(248, 248, 248)'
#             },
            {
                'if': {
                    'column_id': 'packet_loss-total_dests',
                    'filter_query': '{packet_loss-total_dests} < {total_num_of_dests}'
                },
                'backgroundColor': 'rgb(153, 61, 61)',
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'packet_loss-total_dests',
                    'filter_query': '{packet_loss-total_dests} eq {total_num_of_dests}'
                },
                'backgroundColor': '#3D9970',
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'owd-total_dests',
                    'filter_query': '{owd-total_dests} < {total_num_of_dests}'
                },
                'backgroundColor': 'rgb(153, 61, 61)',
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'owd-total_dests',
                    'filter_query': '{owd-total_dests} eq {total_num_of_dests}'
                },
                'backgroundColor': '#3D9970',
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'delay_mean',
                    'filter_query': '{delay_mean} < 0'
                },
                'backgroundColor': 'rgb(153, 61, 61)',
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'packet_loss',
                    'filter_query': '{packet_loss} > 0.1'
                },
                'backgroundColor': 'rgb(230, 165, 3)',
                'color': 'white',
            },
            {
                'if': {
                    'column_id': 'packet_loss',
                    'filter_query': '{packet_loss} eq 1'
                },
                'backgroundColor': 'rgb(153, 61, 61)',
                'color': 'white',
            }
        ]

host_table_header = {
    'backgroundColor': 'rgb(230, 230, 230)',
    'fontWeight': 'bold',
    'font-family':'sans-serif'
}

bubble_chart_layout = {
                      'title':'Average packet loss from 01-12-2019 to 22-01-2020',
                      'xaxis':{'title':'Period in days'},
                      'yaxis':{'title':'Hosts'},
                      'paper_bgcolor':'rgba(0,0,0,0)',
                      'plot_bgcolor':'rgba(0,0,0,0)',
                      'font': {'size': 10}
                  }