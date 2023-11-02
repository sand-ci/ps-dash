import pandas as pd

def packet_loss_preprocess(plsDf_custom_x, model):
    #Preprocessing
    plsDf_custom_x = plsDf_custom_x.drop(['src', 'dest', 'pair', 'src_host', 'dest_host'], axis=1)
    plsDf_custom_x['dt'] = plsDf_custom_x['to']

    plsDf_custom_x['tests_done'] = plsDf_custom_x['tests_done'].str.rstrip('%').astype('float') / 100.0

    #one hot encode the dataset 'case loading a pre-encoded dataset apparently crashes a jupyter notebook kernel
    plsDf_custom_x = pd.get_dummies(plsDf_custom_x, dtype=int)

    plsDf_custom_y = plsDf_custom_x['flag']
    plsDf_custom_x = plsDf_custom_x.drop(['flag'], axis=1)

    #predict the packet loss alarms using the loaded ML model
    y = model.predict(plsDf_custom_x)

    df_to_plot = plsDf_custom_x.copy()
    df_to_plot['flag'] = y
    df_to_plot['dt'] = (pd.to_datetime(df_to_plot['dt'], unit='ms'))

    del plsDf_custom_x

    # convert timestamp back to datetime
    plsDf_onehot_plot = df_to_plot.copy()
    plsDf_onehot_plot['flag'] = plsDf_custom_y.copy()
    plsDf_onehot_plot['dt'] = (pd.to_datetime(plsDf_onehot_plot['dt'], unit='ms'))

    return df_to_plot, plsDf_onehot_plot



