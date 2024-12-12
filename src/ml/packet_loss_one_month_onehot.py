import pandas as pd
import datetime

def one_month_data(plsDf_custom):
    # Preprocessing
    plsDf_custom = plsDf_custom.drop(['src', 'dest', 'pair', 'src_host', 'dest_host'], axis=1)
    plsDf_custom['dt'] = pd.to_datetime(plsDf_custom['to'], utc=True)

    plsDf_custom['tests_done'] = plsDf_custom['tests_done'].str.rstrip('%').astype('float') / 100.0

    # ONE HOT encoding
    plsDf_onehot = pd.get_dummies(plsDf_custom, dtype=int)

    # Determine the number of days in the dataframe
    num_days = (plsDf_custom['dt'].max() - plsDf_custom['dt'].min()).days

    if num_days >= 60:
        first_month_n = (plsDf_custom['dt'] < (plsDf_custom['dt'].min() + pd.Timedelta(days=28))).sum()
    else:
        first_month_n = int(len(plsDf_custom) * 0.8)

    return plsDf_onehot.iloc[:first_month_n], plsDf_onehot

