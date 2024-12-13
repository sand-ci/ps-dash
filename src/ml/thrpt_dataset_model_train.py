import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,  f1_score
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
import xgboost as xgb

def preprocess(rawDf_custom):
    # label the measurements
    rawDf_custom['z'] = rawDf_custom.groupby(['src_site','dest_site'], group_keys=False)['value'].apply(lambda x: round((x - x.mean())/x.std(),2))
    threshold = 2
    rawDf_custom['alarm_created'] = np.where(((rawDf_custom['z']<=-threshold)), 1, 0)
    rawDf_custom = rawDf_custom.drop(['z'], axis=1)

    #Creating timestamps instead of dates
    rawDf_custom['dt'] = (pd.to_datetime(rawDf_custom['dt']).astype('int64')/ 10**9).astype('int64')

    rawDf_custom = rawDf_custom.drop(['hash','src','dest'], axis=1)

    #onehot encoding the dataset
    rawDf_onehot = pd.get_dummies(rawDf_custom,dtype=int)
    rawDf_onehot.ipv6 = rawDf_onehot.ipv6.replace({True: 1, False: 0}).infer_objects(copy=False)
    return rawDf_onehot

def trainMLmodel(rawDf):
    print('Starting trainMLmodel')
    #taking the index of the first 50 days for further training
    rawDf['dt'] = pd.to_datetime(rawDf['dt'].astype(str))
    date_s = list(rawDf['dt'][:1])[0]
    date_s = date_s.date()
    date_s = (date_s + datetime.timedelta(days=50))
    try:
        end_index = rawDf.loc[(rawDf['dt'].dt.date == date_s)][:1].index[0]
        percentile = rawDf.index.get_loc(end_index) / len(rawDf)
    except:
        percentile = 0.8

    #preprocessing the dataset
    rawDf_onehot = preprocess(rawDf)
    print('Preprocessed dataset')

    # preparing the datasets for ml training
    rawDf_custom_y = rawDf_onehot['alarm_created']
    rawDf_custom_x = rawDf_onehot.drop(['alarm_created'], axis=1)

    #Train test split (training on 50 days)
    X = rawDf_custom_x
    y = rawDf_custom_y
    X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=1-percentile,random_state=0,shuffle=False)

    # TRAINING ML model (using one hot dataset)
    model = xgb.XGBClassifier(random_state=0, n_estimators = 115, max_depth = 11, learning_rate = 0.25, gamma = 0.1)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    # evaluation metrics of the model
    print('Trained XGBClassifier model')
    print("Accuracy of the XGBClassifier classifier: %.2f %%", round(accuracy_score(y_test, y_pred)*100,2))
    print("F1 score of the XGB Classifier: %f", f1_score(y_test, y_pred))
    print(classification_report(y_test, y_pred))
    print(confusion_matrix(y_test, y_pred))

    del X_train, X_test, y_train, y_test

    return rawDf_onehot, model

def predictData(rawDf_onehot, model):
    rawDf_custom_x = rawDf_onehot.drop(['alarm_created'], axis=1)

    #preparing final datasets for further analysis
    y = model.predict(rawDf_custom_x)
    df_to_plot = rawDf_custom_x.copy()
    # convert timestamp back to datetime
    df_to_plot['dt'] = (pd.to_datetime(df_to_plot['dt'],unit='s'))
    df_to_plot['alarm_created'] = y

    rawDf_onehot_plot = rawDf_onehot.copy()
    rawDf_onehot_plot['dt'] = (pd.to_datetime(rawDf_onehot_plot['dt'],unit='s'))

    return rawDf_onehot_plot, df_to_plot







