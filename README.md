# Project - AWS Data Lake for Sparkify

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides on AWS S3 in JSON logs on user activity on the app, as well as with JSON metadata on the songs in their app also on AWS S3.

This project creates a data lake in AWS using AWS S3 to build an ETL pipeline. In this project, a star schema was used where fact and dimension tables have been defined for a particular analytic focus. The ETL pipeline is used to transfer data from AWS S3 into spark script for data extraction, then transformation is applied before loading the data into tables back in another AWS S3 bucket for analytic team to use.


## Getting Started

The details below will get you a summary of the project's data lake for development and testing purposes.

### Prerequisites

You need to create an AWS S3 bucket and include the Amazon credentials information in dl.cfg for accessing this bucket in aws. Some information on where the data resides in AWS S3 are provided in the etl.py as input_data. Before running etl.py, make sure add the link to the AWS S3 created as output_data in the etl.py's main function.


### Installing

All files can be downloaded and stored on local machine. Then you can run the etl.py. etl.py executes queries that reads and copy a single file from song_data and log_data and loads the data for spark. etl.py also executes queries that insert data into AWS S3 bucket for analytics team. The file etl.ipynb was added to help navigate the execution step by step. Results for each step of the whole processes can be observed and evaluated.

files included:
* dl.cfg
* etl.py
* etl.ipynb
* README.md

Methods defined in etl.py:

```
def create_spark_session():
    creates if not exist already, else updates, and returns a spark session.

def process_song_data(spark, input_data, output_data):
    handles all the extraction, processing, and storing output of the song data file.
    doesn't return anything.

def process_log_data(spark, input_data, output_data):
    handles all the extraction, processing, and storing output of the log data file.
    doesn't return anything.

def main():
    declares data sources (input_data and output_data) and calls the methods above.
```

## Running the tests

Run etl.py. Logon to AWS and go to AWS S3 bucket created for this project and obseve the data in tables created with their appropriate partitions.


## Authors

* **Adrien Ndikumana** - [adrien19](https://github.com/adrien19)


## Acknowledgments

* Inspiration from Udacity Team
