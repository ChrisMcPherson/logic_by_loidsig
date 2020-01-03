import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline, make_union
from sklearn.tree import DecisionTreeRegressor
from tpot.builtins import StackingEstimator

# NOTE: Make sure that the class is labeled 'target' in the data file
tpot_data = pd.read_csv('PATH/TO/DATA/FILE', sep='COLUMN_SEPARATOR', dtype=np.float64)
features = tpot_data.drop('target', axis=1).values
training_features, testing_features, training_target, testing_target = \
            train_test_split(features, tpot_data['target'].values, random_state=42)

# Score on the training set was:-0.3347786150665041
exported_pipeline = make_pipeline(
    StackingEstimator(estimator=GradientBoostingRegressor(alpha=0.95, learning_rate=1.0, loss="quantile", max_depth=1, max_features=0.4, min_samples_leaf=11, min_samples_split=18, n_estimators=100, subsample=0.5)),
    DecisionTreeRegressor(max_depth=2, min_samples_leaf=9, min_samples_split=18)
)

exported_pipeline.fit(training_features, training_target)
results = exported_pipeline.predict(testing_features)
