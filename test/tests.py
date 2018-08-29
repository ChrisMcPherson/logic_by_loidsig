import pandas as pd
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(sys.path[0], '..', 'src', 'lib')))
import market_maker_training
import market_maker_scoring

def test_training():
    coin_pair_dict = {'target':'btcusdt',
                    'alt':'ethusdt',
                    'through':'trxeth',
                    'excharb':'ethusdt'}

    mm_training = market_maker_training.CobinhoodTraining(coin_pair_dict, [5], [10])
    mm_training.training_data_sql

def test_scoring():
    mm_scoring = market_maker_scoring.CobinhoodScoring()
    # Set scoring data and retrieve the most recent minutes features
    mm_scoring.set_scoring_data(in_parallel=False)
    recent_df = mm_scoring.scoring_features_df.sort_values('open_time')
    # Check time diff
    latest_timestamp = recent_df.iloc[-1:]['close_time_x'].item() / 1000
    print(f"Last timestamp in scoring data: {latest_timestamp} compared to current: {time.time()} with {time.time() - latest_timestamp}")


if __name__ == '__main__':
    test_scoring()