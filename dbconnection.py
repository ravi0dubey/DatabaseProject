import mysql.connector as connection
import pandas as pd
import csv

def connect_db():
    try:
        mydb = connection.connect(host="localhost", user="devuser", passwd="Logitech1234#", use_pure=True)
        show_query = "SHOW DATABASES"
        cursor = mydb.cursor()  # create a cursor to execute queries
        cursor.execute(show_query)
        return "connection success"
    except Exception as e:
            mydb.close()
            return (str(e))

def create_table():
    try:
        mydb = connection.connect(host="localhost", database= "projectdb",user="devuser", passwd="Logitech1234#", use_pure=True)
        cursor = mydb.cursor()  # create a cursor to execute queries
        create_table_query1 = "CREATE TABLE if not exists projectdb.dresssale(`Dress_ID` int(30),`29/8/2013` int(5) default 0,`31/8/2013`	int(5) default 0,`2/9/2013` int(5) default 0,`4/9/2013` int(5) default 0,`6/9/2013` int(5) default 0,`8/9/2013` int(5) default 0,`10/9/2013` int(5) default 0,`12/9/2013` int(5) default 0,`14/9/2013` int(5) default 0,`16/9/2013` int(5) default 0,`18/9/2013` int(5) default 0,`20/9/2013` int(5) default 0,`22/9/2013`	int(5) default 0,`24/9/2013` int(5) default 0,`26/9/2013` int(5) default 0,`28/9/2013` int(5) default 0,`30/9/2013`	int(5) default 0,`2/10/2013` int(5) default 0,`4/10/2013` int(5) default 0,`6/10/2013` int(5) default 0,`8/10/2010`	int(5) default 0,`10/10/2013` int(5)  default 0,`12/10/2013` int(5)  default 0);"
        cursor.execute(create_table_query1)
        create_table_query2 = "create table if not exists projectdb.attributedataset(`Dress_ID` int(30),	`Style`	varchar(30),	`Price`	varchar(30),	`Rating`	float(2,1),	`Size`	varchar(30),	`Season`	varchar(30),	`NeckLine`	varchar(30),	`SleeveLength` varchar(30),		`waistline`	varchar(30),	`Material`	varchar(30),	`FabricType`	varchar(30),	`Decoration`	varchar(30),	`Pattern Type` varchar(30),		`Recommendation` int(1)); "
        cursor.execute(create_table_query2)
        return "connection success"
    except Exception as e:
        print(str(e))
        return (str(e))


def one_time_load():
    try:
        mydb = connection.connect(host="localhost", database= "projectdb",user="devuser", passwd="Logitech1234#", use_pure=True)
        cursor = mydb.cursor()  # create a cursor to execute queries
        load_data_query1 = "load data infile 'D:\Dress Sales.csv'  into table projectdb.dresssale fields terminated by ',' Enclosed by '""' IGNORE 1 ROWS ;"
        print(load_data_query1)
        cursor.execute(load_data_query1)
        load_data_query2 = "load data infile 'D:\Attribute DataSet.csv'  into table projectdb.attributedataset fields terminated by ',' Enclosed by '""' IGNORE 1 ROWS ;"
        print(load_data_query2)
        cursor.execute(load_data_query2)
        mydb.commit()
        print("load successful")
        return "Load success"
    except Exception as e:
        print(str(e))
        return (str(e))

def read_excel():
    try:
        df_excel = pd.read_excel("D:\Study\Data Science\Python\ineuron\SampleExcelforPandas\Sample1.xls")
        print(df_excel)
    except Exception as e:
        print(str(e))
        return (str(e))