# Keto Diet ETL Data Pipeline through Requests:

### Introduction:
This is an ETL(Extract, Transform, Load) Data Pipeline project using Keto Diet API from Rapid API. In this we extracted the data from the Keto Diet API through requests and stored it on AWS S3 bucket using boto3 library, then transformed the data to desired format using Pandas library, and then finally load it on target S3 folder.

### About Dataset/Spotify API:
Keto Diet API contains the data of recipes, category, ingredients and direction to prepare the receipe. This data can be extracted from API using requests which will be invoked using URL and API Key from [Rapid API - Keto Diet API](https://rapidapi.com/encurate/api/keto-diet/)

### Architecture Flow:
**Keto Diet API** -> **AWS Cloudwatch** trigger and invoke extract function in specified intervals -> Exracts raw data to **AWS S3** bucket raw folder (through **AWS Lambda** extract function) -> **AWS S3** trigger and invoke transformation function when new file created in raw folder -> Loads transformed data into **AWS S3** bucket transformed folder (through **AWS Lambda** transform function) -> Crawl the data and edit the datatypes accordingly using **AWS Crawler** and **AWS Glue Catalog** -> Query the data using **AWS Athena**

### AWS Services Used:
1. **AWS S3 :** Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. You can use Amazon S3 to store and retrieve any amount of data at any time, from anywhere.

2. **AWS Lambda :** AWS Lambda is a serverless, event-driven compute service that lets you run code for virtually any type of application or backend service without provisioning or managing servers. You can trigger Lambda from over 200 AWS services and software as a service (SaaS) applications, and only pay for what you use.

3. **AWS CloudWatch :** CloudWatch enables you to monitor your complete stack (applications, infrastructure, network, and services) and use alarms, logs, and events data to take automated actions and reduce mean time to resolution (MTTR). This frees up important resources and allows you to focus on building applications and business value.

4. **AWS Crawler :** A crawler can crawl multiple data stores in a single run. Upon completion, the crawler creates or updates one or more tables in your Data Catalog. Extract, transform, and load (ETL) jobs that you define in AWS Glue use these Data Catalog tables as sources and targets.

5. **AWS Glue Catalog :** The AWS Glue Data Catalog is organized into databases and tables to provide a logical structure for storing and managing metadata. This structure supports precise data access control at a table or database level by using AWS Identity and Access Management (IAM) policies.

6. **AWS Athena :** Amazon Athena is a serverless, interactive analytics service built on open-source frameworks, supporting open-table and file formats. Athena provides a simplified, flexible way to analyze petabytes of data where it lives.

### Imported packages:
```
import json
import os
import boto3
import requests
from datetime import datetime
from io import StringIO
import pandas
```

