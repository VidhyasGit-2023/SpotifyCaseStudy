# Using the Canadian Tobacco and Nicotine Survey: Public Use Microdata File from Statistics Canada - Vidhya Venugopal - 8902970
# Import Packages for data load and save into DB

import pandas as pd
import numpy as np
import re
from datetime import datetime
import mysql.connector

# Load the dataset CTNS2022_P.csv
CTNS2022_P = pd.read_csv('E:\\Canada\Conestoga College\\Programming in BigData - PROG8420 - 23S - Sec2\\PythonPrograms\\source\\repos\\CTNS2022_P.csv')

# Validate if the data has been uploaded properly
# Prints number of rows and columns in dataframe
print(CTNS2022_P.shape)
# Prints the datatypes of all the columns in dataframe
print(CTNS2022_P.dtypes)
# Prints first n rows of the DataFrame
print(CTNS2022_P.head(15))
# Prints last n rows of the DataFrame
print(CTNS2022_P.tail(15))
# Index, Datatype and Memory information
print(CTNS2022_P.info())

# Replace with your database connection details
host = 'localhost'
user = 'root'
password = 'password'
database = 'case_study_spotify_db'

# Establish a connection
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Define the SQL query to create the table
create_table_query = """
CREATE TABLE IF NOT EXISTS CTNS2022_P (
    id INT AUTO_INCREMENT PRIMARY KEY,
    PUMFID INT,
    TBC_05A INT,
    TBC_05BR INT,
    TBC_10AR INT,
    TBC_10BR INT,
    TBC_15 INT,
    OTP_05AR INT,
    OTP_05BR INT,
    VAP_05AR INT,
    VAP_05BR INT,
    VAP_10R INT,
    CAN_05A INT,
    CAN_05BR INT,
    CAN_10A	INT,
    CAN_10BR INT,
    IU_05R INT,
    ALC_05 INT,
    GENDER INT,
    AGEGROUP INT,
    PROV_C INT,
    ED_05R INT,
    FIRSTTRR INT,
    CIGWAVR	INT,
    DV_ALC30 INT
)
"""

# Create the table
cursor = connection.cursor()
cursor.execute(create_table_query)

# Insert data into the table
insert_query = """
INSERT INTO CTNS2022_P (
    PUMFID, TBC_05A, TBC_05BR, TBC_10AR, TBC_10BR, TBC_15, OTP_05AR, OTP_05BR, VAP_05AR, VAP_05BR,
    VAP_10R, CAN_05A, CAN_05BR, CAN_10A, CAN_10BR, IU_05R, ALC_05, GENDER, AGEGROUP, PROV_C, ED_05R,
    FIRSTTRR, CIGWAVR, DV_ALC30
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Iterate through the DataFrame and insert each row
for index, row in CTNS2022_P.iterrows():
    values = (
        row['PUMFID'],
        row['TBC_05A'],
        row['TBC_05BR'],
        row['TBC_10AR'],
        row['TBC_10BR'],
        row['TBC_15'],
        row['OTP_05AR'],
        row['OTP_05BR'],
        row['VAP_05AR'],
        row['VAP_05BR'],
        row['VAP_10R'],
		row['CAN_05A'],
		row['CAN_05BR'],
		row['CAN_10A'],
		row['CAN_10BR'],
		row['IU_05R'],
		row['ALC_05'],
		row['GENDER'],
		row['AGEGROUP'],
		row['PROV_C'],
		row['ED_05R'],
		row['FIRSTTRR'],
		row['CIGWAVR'],
		row['DV_ALC30']
    )
    cursor.execute(insert_query, values)

# Commit the changes
connection.commit()


connection.close()