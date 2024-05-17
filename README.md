# Assignment 4 – MSDS460 – Comparing PostgreSQL Backends
This repository contains files associated with assignment 4 for MSDS460, focusing on comparing processing times for R with RPostgreSQL and Python and psycopg2 through all CRUD operations using a Monte Carlo simulation and randomized data

## Simulation Models

We created a database in PostgreSQL called ExCompany with a single table employees. The table architecture can be found in MSDS_460_Assignment_4_SQL_Table_Creation.sql and an example output can be found in employees_populated_table.csv. We used a fake data generation library of faker in Python and charlatan in R to populate a list of rows of randomized data entries. Our data table included data types of integer (employee_id and age), text (first_name and last_name), float (rating), JSON (json_contact_info), JSONB (bjson_contact_info), and GEOMETRY (address). We tested CRUD operations on the table in order to compare processing times of both backend languages.

## Programming

We created functions in each programming language to generate the fake data, using this fake data to insert rows into the database table, read from the data table, update the data table and delete from the data table. We generated 10,000 rows of fake data from which to pull for each insert cycle, randomly sampled 1000 entries from this data and inserted them into the table. We measured the processing time of inserting 1000 entries and repeated this 1000 times and analyzed the processing data. Between each run we truncated the table to remove all the data to give us a reinitialized empty table.
For the read queries, we inserted 10000 entries into the data table and took a random sample of 1000 rows from which to use in our read queries, measuring the time it took to read 1000 rows. We repeated this 1000 times and analyzed the processing data.
For the update queries, we inserted 10000 entries into the data table and took an initial random sample of 1000 rows. We took a second random sample of 1000 rows and updated these rows using the data from the initial random sample. We repeated this process 1000 times and analyzed the processing data.
For the delete queries, we inserted 10000 entries into the data table and took a random sample of 1000 rows. We used each row of the randomly selected data to delete from the table and rolled back our executed delete query to preserve the table size throughout the experiment. We repeated this process 1000 times and analyzed the processing data.
