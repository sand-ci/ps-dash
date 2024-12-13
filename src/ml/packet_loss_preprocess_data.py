import pandas as pd
from utils.helpers import timer

@timer
def packet_loss_preprocess(plsDf_custom_x, model):

    plsDf_custom_y = plsDf_custom_x['flag']
    plsDf_custom_x = plsDf_custom_x.drop(['flag'], axis=1)

    #predict the packet loss alarms using the loaded ML model
    y = model.predict(plsDf_custom_x)

    df_to_plot = plsDf_custom_x.copy()
    df_to_plot['flag'] = y
    df_to_plot['dt'] = (pd.to_datetime(df_to_plot['dt'], utc=True))

    print('df_to_plot', df_to_plot.shape)

    del plsDf_custom_x

    # convert timestamp back to datetime
    plsDf_onehot_plot = df_to_plot.copy()
    plsDf_onehot_plot['flag'] = plsDf_custom_y.copy()
    plsDf_onehot_plot['dt'] = (pd.to_datetime(plsDf_onehot_plot['dt'], utc=True))

    print('plsDf_onehot_plot', plsDf_onehot_plot.shape)

    del plsDf_custom_y
    del y

    return df_to_plot, plsDf_onehot_plot



