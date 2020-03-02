from time import gmtime, strftime, sleep
import sagemaker
import boto3
from sagemaker import get_execution_role

# Setup
region = boto3.Session().region_name

session = sagemaker.Session()
bucket = session.default_bucket()
prefix = 'sagemaker/autopilot-dm'

role = get_execution_role()

sm = boto3.Session().client(service_name='sagemaker',region_name=region)

# Load data
train_file = 'train_data.csv';
train_data.to_csv(train_file, index=False, header=True)
train_data_s3_path = session.upload_data(path=train_file, key_prefix=prefix + "/train")
print('Train data uploaded to: ' + train_data_s3_path)

test_file = 'test_data.csv';
test_data_no_target.to_csv(test_file, index=False, header=False)
test_data_s3_path = session.upload_data(path=test_file, key_prefix=prefix + "/test")
print('Test data uploaded to: ' + test_data_s3_path)

# Autopilot data config
input_data_config = [{
      'DataSource': {
        'S3DataSource': {
          'S3DataType': 'S3Prefix',
          'S3Uri': 's3://{}/{}/train'.format(bucket,prefix)
        }
      },
      'TargetAttributeName': 'y'
    }
  ]

output_data_config = {
    'S3OutputPath': f's3://{bucket}/{prefix}/output'
  }
  
# Launch job
timestamp_suffix = strftime('%d-%H-%M-%S', gmtime())

auto_ml_job_name = 'logic-by-loidsig-automl-' + timestamp_suffix
print('AutoMLJobName: ' + auto_ml_job_name)

sm.create_auto_ml_job(AutoMLJobName=auto_ml_job_name,
                      InputDataConfig=input_data_config,
                      OutputDataConfig=output_data_config,
                      RoleArn=role)
