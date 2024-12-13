import pandas as pd
import datetime

def one_month_data(plsDf_custom, batch_size=10000):
    # Preprocessing
    plsDf_custom = plsDf_custom.drop(['src', 'dest', 'pair', 'src_host', 'dest_host'], axis=1)
    plsDf_custom['dt'] = pd.to_datetime(plsDf_custom['to'], utc=True)
    plsDf_custom['tests_done'] = plsDf_custom['tests_done'].str.rstrip('%').astype('float') / 100.0

    # ONE HOT encoding in batches
    plsDf_onehot = pd.DataFrame()
    for i in range(0, len(plsDf_custom), batch_size):
        batch = plsDf_custom.iloc[i:i+batch_size]
        batch_onehot = pd.get_dummies(batch, dtype=int)
        plsDf_onehot = pd.concat([plsDf_onehot, batch_onehot], ignore_index=True)

    # Determine the number of days in the dataframe
    num_days = (plsDf_custom['dt'].max() - plsDf_custom['dt'].min()).days
    plsDf_onehot.reset_index(drop=True, inplace=True)
    plsDf_custom = plsDf_custom.sort_values(by='dt')

    if num_days >= 60:
        first_month_n = (plsDf_custom['dt'] < (plsDf_custom['dt'].min() + pd.Timedelta(days=28))).sum()
    else:
        first_month_n = int(len(plsDf_custom) * 0.8)

    return plsDf_onehot.iloc[:first_month_n], plsDf_onehot

