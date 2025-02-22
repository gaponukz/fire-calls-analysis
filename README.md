# fire-calls-analysis
PySpark application for fire calls analysis. Original data source: https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/data.

# Running local

```sh
spark-submit app.py --input sf-fire-calls.csv --output result
```

# Running on AWS EMR

Copy input dataset to S3
```sh
aws s3 cp sf-fire-calls.csv s3://bucket-name/sf-fire-calls.csv
```

Copy PySpark application code to S3
```sh
aws s3 cp app.py s3://bucket-name/app.py
```

Run PySpark application on EMR cluster.
```sh
aws emr create-cluster \
  --name "Spark Cluster" \
  --release-label "emr-6.12.0" \
  --applications Name=Spark \
  --use-default-roles \
  --instance-groups '[
      {
        "InstanceGroupType": "MASTER",
        "InstanceCount": 1,
        "InstanceType": "m5.xlarge"
      },
      {
        "InstanceGroupType": "CORE",
        "InstanceCount": 2,
        "InstanceType": "m5.xlarge"
      }
    ]' \
  --steps '[
    {
      "Type": "CUSTOM_JAR",
      "Name": "Run PySpark App",
      "ActionOnFailure": "CONTINUE",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "s3://bucket-name/app.py",
        "--input", "s3://bucket-name/sf-fire-calls.csv",
        "--output", "s3://bucket-name/result"
      ]
    }
  ]' \
  --auto-terminate
```

Download execution result from S3
```sh
aws s3 sync s3://bucket-name/result result
```
