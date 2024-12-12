from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import xgboost as xgb
import matplotlib.pyplot as plt

def packet_loss_train_model(plsDf_onehot, batch_size=10000):
    plsDf_custom_y = plsDf_onehot['flag']
    plsDf_custom_x = plsDf_onehot.drop(['flag'], axis=1)
    del plsDf_onehot

    # Train test split (training on one month)
    X_train, X_test, y_train, y_test = train_test_split(plsDf_custom_x, plsDf_custom_y, test_size=0.20, random_state=0, shuffle=False)

    del plsDf_custom_y
    del plsDf_custom_x

    # Training the XGB Classifier in batches
    model = xgb.XGBClassifier(random_state=0, objective='multi:softmax')
    for i in range(0, len(X_train), batch_size):
        end = i + batch_size
        model.fit(X_train[i:end], y_train[i:end], xgb_model=model if i > 0 else None)

    y_pred = model.predict(X_test)

    # Evaluation metrics
    print("Accuracy of the XGB Classifier:", round(accuracy_score(y_test, y_pred) * 100, 2), "%")
    print(classification_report(y_test, y_pred))
    confusion_matrix_data = confusion_matrix(y_test, y_pred, labels=model.classes_)
    print(confusion_matrix_data, "\n")

    del X_train, X_test, y_train, y_test

    return model