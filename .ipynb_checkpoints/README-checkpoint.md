Completed: 5-Jun-2019

# Project: Build an ETL pipeline for a database hosted on Redshift
Load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Available information
#1 - Dataset: 2 datasets that reside in S3:
     - Song data: s3://udacity-dend/song_data
     - Log data: s3://udacity-dend/log_data
       Log data json path: s3://udacity-dend/log_json_path.json

#2 - Template files
     The project template includes 4 files:
     - create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
     - etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
     - sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
     - README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

# Setup Instructions
For this project, I used the Udacity workspace.
 . Started Redshift cluster
 . After starting the cluster, completed the config file (dwh.cfg) with relevant cluster and role details
 
# Program execution
 (1) Create IAM Role and give it Read permission on S3 - AmazonS3ReadOnlyAccess
 (2) Fill in your AWS account id and name of the role created in step 1 in the following statement in dwh.cfg:                    
     arn:aws:iam::<account_id>:role/<role>
 (3) Create Redshift cluster. Note the cluster name, DB name, username/password established at the time of creating the cluster.
 (4) Fill in the details noted in step 3 in the following statements of dwh.cfg file:
     DB_NAME='<DB name>'
     DB_USER='<username>'
     DB_PASSWORD='<password>'
     DB_PORT='5439'
 (5) Copy and paste the ENDPOINT information from the up and running Redshift cluster in the following statement of dwh.cfg file:
     HOST='<ENDPOINT>'
     Remove :PORT from the ENDPOINT copied from the running Redshift cluster
 (6) Start a new terminal in the udacity workspace.
 (7) Execute the following commands in the terminal window to create tables and load data in them:
     - python create_tables.py
     - python etl.py
    
# Testing the program
Launch Query Editor in Redshift on AWS. Run any query on the tables created under 'Public' schema. A few queries that I ran and their resuts are as follows:
     SELECT COUNT(*) FROM staging_events;
     [8056]

    SELECT COUNT(*) FROM staging_songs;
    [14896]

    SELECT COUNT(*) FROM songplay;
   [1144]

    SELECT COUNT(*) FROM users;
    [104]

    SELECT COUNT(*) FROM song;
    [14896]

    SELECT COUNT(*) FROM artist;
    [10025]

    SELECT COUNT(*) FROM time;
    [8023]
    
# Delete the Redshift Cluster
Remember to delete the cluster after executing the program and completing the relevant analysis.

# Reference
(1) Udacity lessons
(2) /stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift