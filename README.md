# Genetic Algorithm for Data Warehouses
A Master's Research using Genetic Algorithm to optimize selection queries on SQL databases.
We used the DEAP library, which has a lot of genetic functions that we can reuse.

-------------------------------------------------------
### Sql files are at tpch-qsrf folder
-------------------------------------------------------


## To initialize the database

000A_PRE_CREATE_TABLES_TEMP
- creates temporary tables

-------------------------------------------------------

000B_PRE_LOAD_DATA_TEMP
- charge data in temporary tables

-------------------------------------------------------

000C_PRE_RF_INSERT
- refresh function to insert data:
- login
- insert itens in orders
- insert itens in lineitem
- commit

-------------------------------------------------------

01_CREATE_VIEWS
- create 22 views

-------------------------------------------------------

02_CREATE_PROCEDURE_QUERY_STREAM
- create procedure calling 22 views

-------------------------------------------------------

03_INSERT_DATA: 
- refresh function to insert data from temporaru to definitive tables

-------------------------------------------------------

04A_DELETE_DATA: refresh function to delete data:
- login
- delete itens in lineitem where id in temporary table
- delete itens in orders where id in temporary table
- commit

-------------------------------------------------------

04B_RF_DELETE: refresh function to delete data:
- login
- call 04A_DELETE_DATA
- commit

-------------------------------------------------------

05_CLEAR_TABLES: 
- clear temporary tables

-------------------------------------------------------
CONFIGURING ENVIRONMENT:
- copy the folder TPCH-QSRF to /usr/local/
- change database password in files 01, 03, and 04
Open shell:
	- $ chmod -R 777 /usr/local/TPCH-QSRF
	- $ chmod +x ./03_RF_INSERT.sh
	- $ chmod +x ./04_RF_DELETE.sh
	- run content on _01_CREATE_VIEWS.txt
Open mysql: 
	$ mysql -u root -p
	$ input your password:
To check if the views are created:
	mysql> SHOW FULL TABLES IN tpch WHERE TABLE_TYPE LIKE 'VIEW';
To create the procedure:
	mysql> use tpch;
	- on mysql shell, run content of 02_CREATE_PROCEDURE_QUERY_STREAM

-------------------------------------------------------
	
RUNNING REFRESH FUNCTIONS AND QUERY STREAM:
To insert data:
Open shell, go to /usr/local/TPCH-QSRF:
	$ sh 03_RF_INSERT.sh
	
To run QueryStream:
	$ mysql -u root -pDATABASE_PASSWORD 
	mysql> call QUERY_STREAM ();
	
To delete data:
Open shell, go to /usr/local/TPCH-QSRF:
	$ sh 04_RF_DELETE.sh
	
-------------------------------------------------------

## TO RUN the project

`python train.py --pop_size 50 --elite_size 4 --generations 50 --mutation_rate 0.05 --mutation_prob 0.1 --crossover_prob 0.25 --outpath runs/experiment_1 --fitness default --debug`
