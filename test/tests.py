import market_maker_training

coin_pair_dict = {'target':'btcusdt',
                  'alt':'ethusdt',
                  'through':'trxeth',
                  'excharb':'ethusdt'}

mm_training = market_maker_training.CobinhoodTraining(coin_pair_dict, [5], [10])
mm_training.training_data_sql