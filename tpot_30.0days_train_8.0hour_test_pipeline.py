import numpy as np
import pandas as pd
from sklearn.feature_selection import SelectFwe, VarianceThreshold, f_regression
from sklearn.linear_model import LassoLarsCV, RidgeCV
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline, make_union
from tpot.builtins import StackingEstimator

# NOTE: Make sure that the class is labeled 'target' in the data file
tpot_data = pd.read_csv('PATH/TO/DATA/FILE', sep='COLUMN_SEPARATOR', dtype=np.float64)
features = tpot_data.drop('target', axis=1).values
training_features, testing_features, training_target, testing_target = \
            train_test_split(features, tpot_data['target'].values, random_state=42)

# Score on the training set was:-0.13632650010318756
exported_pipeline = make_pipeline(
    SelectFwe(score_func=f_regression, alpha=0.01),
    VarianceThreshold(threshold=0.01),
    StackingEstimator(estimator=RidgeCV()),
    LassoLarsCV(normalize=True)
)

exported_pipeline.fit(training_features, training_target)
results = exported_pipeline.predict(testing_features)
