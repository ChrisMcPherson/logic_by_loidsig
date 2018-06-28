import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.feature_selection import SelectPercentile, f_regression
from sklearn.linear_model import LassoLarsCV
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline, make_union
from sklearn.preprocessing import MaxAbsScaler, Normalizer, StandardScaler
from sklearn.svm import LinearSVR
from tpot.builtins import OneHotEncoder, StackingEstimator
from xgboost import XGBRegressor

# NOTE: Make sure that the class is labeled 'target' in the data file
tpot_data = pd.read_csv('PATH/TO/DATA/FILE', sep='COLUMN_SEPARATOR', dtype=np.float64)
features = tpot_data.drop('target', axis=1).values
training_features, testing_features, training_target, testing_target = \
            train_test_split(features, tpot_data['target'].values, random_state=42)

# Score on the training set was:-0.05315395710812345
exported_pipeline = make_pipeline(
    OneHotEncoder(minimum_fraction=0.15, sparse=False),
    StackingEstimator(estimator=XGBRegressor(learning_rate=0.01, max_depth=7, min_child_weight=7, n_estimators=100, nthread=1, subsample=0.9000000000000001)),
    MaxAbsScaler(),
    StackingEstimator(estimator=XGBRegressor(learning_rate=0.01, max_depth=10, min_child_weight=7, n_estimators=100, nthread=1, subsample=0.7000000000000001)),
    StandardScaler(),
    SelectPercentile(score_func=f_regression, percentile=83),
    OneHotEncoder(minimum_fraction=0.15, sparse=False),
    SelectPercentile(score_func=f_regression, percentile=30),
    StackingEstimator(estimator=LinearSVR(C=0.01, dual=True, epsilon=0.1, loss="squared_epsilon_insensitive", tol=0.01)),
    StackingEstimator(estimator=LassoLarsCV(normalize=True)),
    Normalizer(norm="l2"),
    StackingEstimator(estimator=RandomForestRegressor(bootstrap=True, max_features=0.6500000000000001, min_samples_leaf=11, min_samples_split=9, n_estimators=100)),
    SelectPercentile(score_func=f_regression, percentile=70),
    GradientBoostingRegressor(alpha=0.95, learning_rate=0.01, loss="ls", max_depth=10, max_features=0.7500000000000001, min_samples_leaf=12, min_samples_split=12, n_estimators=100, subsample=0.5)
)

exported_pipeline.fit(training_features, training_target)
results = exported_pipeline.predict(testing_features)
