#!/usr/bin/python3

import csv
import numpy as np
import sqlite3


conn = sqlite3.connect("raw_table.db1")
c = conn.cursor()
# Create Staging table 
print("Creating staging Raw table")
sql_command = """ CREATE TABLE RAWTABLE 
                    (
                            Name varchar(255) NOT NULL,
                            Cust_I varchar(18) NOT NULL,
                            Open_Dt DATE NOT NULL,
                            Consul_Dt DATE,
                            VAC_ID varchar(5),
                            DR_Name varchar(255),
                            State varchar(5),
                            Country varchar(5),
                            Postal_code INT,
                            DOB DATE,
                            FLAG VARCHAR(1),
                            PRIMARY KEY(Name) 
                     )
              """


c.execute(sql_command)

# Insert data into Staging table
"""
data.csv:
|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|State|Country|DOB|Is_Active
|D|Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A
|D|John|123458|20101012|20121013|MVD|Paul|TN|IND|06031987|A
|D|Mathew|123459|20101012|20121013|MVD|Paul|WAS|PHIL|06031987|A
|D|Matt|12345|20101012|20121013|MVD|Paul|BOS|NYC|06031987|A
|D|Jacob|1256|20101012|20121013|MVD|Paul|VIC|AU|06031987|A
|D|Allis|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A
|D|Johnny|123458|20101012|20121013|MVD|Paul|TN|IND|06031987|A
|D|Mark|123459|20101012|20121013|MVD|Paul|WAS|PHIL|06031987|A
|D|Marcel|12345|20101012|20121013|MVD|Paul|BOS|NYC|06031987|A
|D|Josline|1256|20101012|20121013|MVD|Paul|VIC|AU|06031987|A
"""
print("Loading data from data.csv")
with open ("data.csv") as rdata:
      ncols = len(rdata.readline().split("|"))
      rwdata = csv.reader(rdata,delimiter = "|") # Read the csv
      for row in rwdata:
         col = row[2:]
         insert_query = """
                            INSERT INTO RAWTABLE(Name, Cust_I, Open_Dt, Consul_Dt, VAC_ID, DR_Name, State, Country, DOB, Flag )
                            values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                       """.format(col[0],col[1],col[2],col[3],col[4],col[5],col[6],col[7],col[8],col[9])
#        print(insert_query)
         c.execute(insert_query)

print("Data inserted into RAW table.")

#Fetch distinct countries

distinct_query = " SELECT DISTINCT Country FROM RAWTABLE"
c.execute(distinct_query)

# Iterate on the distinct country list and create table for each  country.
print("Creating Country tables")
for row in c.fetchall():
    Country_name = row[0] 
    print("Creating TABLE_{}".format(Country_name))
    create_query = """  
                      CREATE TABLE TABLE_{} AS SELECT * FROM RAWTABLE WHERE Country = '{}'
                   """.format(Country_name,Country_name) 
#   print(create_query)
    c.execute(create_query) 
    
# List of all tables that have been created so far. 

c.execute("Select * from SQLite_master")
tables = c.fetchall()

print("Listing of tables from main database:")
for table in tables:
        print("Type of database object: %s"%(table[0]))
        print("Name of the database object: %s"%(table[1]))

# Close the databsae connection
print("Closing DB connection")
conn.close()
