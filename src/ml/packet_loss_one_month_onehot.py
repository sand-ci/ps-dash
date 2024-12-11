import pandas as pd
import datetime

def one_month_data(plsDf_custom):

    #Preprocessing
    plsDf_custom = plsDf_custom.drop(['src','dest','pair','src_host','dest_host'], axis=1)
    plsDf_custom['dt'] = plsDf_custom['to']

    plsDf_custom['tests_done'] = plsDf_custom['tests_done'].str.rstrip('%').astype('float') / 100.0

    #ONE HOT encoding
    plsDf_onehot = pd.get_dummies(plsDf_custom,dtype=int)

    # taking the index of the first 28 days for further training
    date_s = list(pd.to_datetime(plsDf_onehot['dt'], utc=True)[:1])[0]
    date_s = date_s.date()
    date_s = (date_s + datetime.timedelta(days=28))
    try:
        end_index = plsDf_onehot.loc[(pd.to_datetime(plsDf_onehot['dt'], utc=True).dt.date == date_s)][:1].index[0]
        percentile = plsDf_onehot.index.get_loc(end_index) / len(plsDf_onehot)
    except:
        percentile = 0.8

    first_month_n = round(len(plsDf_onehot.index)*percentile)

    return plsDf_onehot.iloc[:first_month_n], plsDf_onehot

