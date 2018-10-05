#### Start docker with same machine used in Lambda [new terminal]
docker run -it dacut/amazon-linux-python-3.6

#### Now, create a new directory and cd into it to install all required packages [docker]
pip3 install pandas -t ./

#### Optional steps for git based repo install [docker]
yum install git 
pip3 install git+https://github.com/CliffLin/python-cobinhood -t ./

#### Zip [docker]
zip -r cobinhood_orderbook_events_lambda.zip * 

#### Copy to local machine {docker cp <CONTAINER_ID>:<DOCKER_PATH_TO_ZIP_FILE> <LOCAL_PATH>} [local]
docker ps -a
docker cp 79ec9f41a509:/cobinhood_orderbook_events/cobinhood_orderbook_events_lambda.zip /Users/puter/OneDrive/Projects/logic_by_loidsig/src/data_pipeline/lambda_resources

#### Combine code files with zip [local -in dir with zip]
cd /Users/puter/OneDrive/Projects/logic_by_loidsig/src/data_pipeline/orderbook
zip -ur ../lambda_resources/cobinhood_orderbook_events_lambda.zip cobinhood_orderbook_events.py


## staging
pip3 install python-binance -t ./
zip -r binance_orderbook_events_lambda.zip * 
docker cp 79ec9f41a509:/binance_orderbook_events/binance_orderbook_events_lambda.zip /Users/puter/OneDrive/Projects/logic_by_loidsig/src/data_pipeline/lambda_resources
zip -ur ../lambda_resources/binance_orderbook_events_lambda.zip binance_orderbook_events.py


## staging
pip3 install pandas -t ./
pip3 install psycopg2 -t ./
zip -r cobinhood_orderbook_features_lambda.zip * 
docker cp 79ec9f41a509:/cobinhood_orderbook_features/cobinhood_orderbook_features_lambda.zip /Users/puter/OneDrive/Projects/logic_by_loidsig/src/data_pipeline/lambda_resources
zip -ur ../lambda_resources/cobinhood_orderbook_features_lambda.zip cobinhood_orderbook_features.py