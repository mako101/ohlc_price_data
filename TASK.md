Historical OHLC Price Data

Background
We have just purchased a large amount of historical OHLC price data that were shared to us in CSV files
format. We need to start centralising and digitalising those data. These files can be ranging from a few GBs to
a couple of TBs.
Example of the CSV data
UNIX,SYMBOL,OPEN,HIGH,LOW,CLOSE
1644719700000,BTCUSDT,42123.29000000,42148.32000000,42120.82000000,42146.06000000
1644719640000,BTCUSDT,42113.08000000,42126.32000000,42113.07000000,42123.30000000
1644719580000,BTCUSDT,42120.80000000,42130.23000000,42111.01000000,42113.07000000
1644719520000,BTCUSDT,42114.47000000,42123.31000000,42102.22000000,42120.80000000
1644719460000,BTCUSDT,42148.23000000,42148.24000000,42114.04000000,42114.48000000

Speed is critical for us, we need to process these files as soon as possible.
Requirements
Create a RESTful HTTP APIs to centralise and digitalise these data.
APIs
POST /data
Specification
Upload and Process the CSV file
Only be able to receive CSV files with correct format
Validation is a must
Data must be stored on a SQL database
GET /data
Specification
Query the data from database
Pagination and search using query string for data must be available
Definition of Done
Application must run in a docker container
All responses must be in JSON format

Documentation in the README.md file
Nice to have
Unit and/or Integration Test of the solution
Scripts, build systems or helper functions to help with installation and initialisation
Important to us
Nice and clean code
The code can be a production ready solution