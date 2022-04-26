# Sparkify AWS Redshift Data Warehouse

This Data Warehouse was created as a means to access the Sparkify data for analytical purposes.

## Project structure

The project has been created with the following structure:

```bash

├── README.md
├── check_s3.py
├── create_tables.py
├── etl.py
├── iac_redshift.py
├── requirements.txt
└── sql_queries.py
```

- check_s3.py: Python utility to download sample files.
- create_tables.py: 
- etl.py: 
- iac_redshift.py: Python utility that creates and deletes an AWS Redshift Cluster (IaC)
- requirements.txt: requirements for python env.
- sql_queries.py: Sql queries to create the Data Warehouse

### Notes

The repo needs a `dwh.cfg` config file that contains the secrets and enviroment variables needed for the project to run.