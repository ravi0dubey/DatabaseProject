
# from sqlalchemy import create_engine
# import pymysql
import mysql.connector as connection
import pandas as pd
import pymongo

def read_attributedataset():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df1 = pd.read_sql("select * from projectdb.attributedataset", mydb)
        print(df1)
        df2 = pd.read_sql("select * from projectdb.dresssale", mydb)
        print(df2)
        return(df1,df2)
    except Exception as e:
        print(str(e))
        return (str(e))

def convert_json(df1,df2):
    try:
        # mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
        #                           use_pure=True)
        # df1 = pd.read_sql("select * from projectdb.attributedataset", mydb)
        # df2 = pd.read_sql("select * from projectdb.dresssale", mydb)
        dj1= df1.to_json("D:\\attributedataset1.json")
        dj2= df2.to_json("D:\\dresssale1.json")
        return(dj1,dj2)
        print("Conversion to JSON format done")
    except Exception as e:
        print(str(e))
        return (str(e))

# def storetomongodb(dj1,dj2):
def storetomongodb():
    try:
        client_connect = pymongo.MongoClient(
            "mongodb+srv://ravi0dubey:Logiw@cluster0.9hjidow.mongodb.net/?retryWrites=true&w=majority")
        database1 = client_connect["projectdb"]
        collection1 = database1["attributedataset"]
        dj1 = pd.read_json("D:\\attributedataset.json")
        # Inserting attributeset into collection
        print(dj1)
        collection1.insert_many("D:\\attributedataset.json")
        # collection2 = database1["dresssale"]
        # # Inserting attributeset into collection
        # dj2=  pd.read_json("D:\\dresssale.json")
        # print(dj2)
        # collection2.insert_many(dj2)
        print("Data stored in MongoDb")
    except Exception as e:
        print(str(e))
        return (str(e))

def leftjoin_operation():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df1_join = pd.read_sql("select * from projectdb.attributedataset a left join projectdb.dresssale b on a.Dress_ID = b.Dress_ID", mydb)
        print("Data after Join operations\n")
        print(df1_join)
        print("After for loop")
        for i in df1_join.columns:
            print(df1_join[[i]])
    except Exception as e:
        print("join issue")
        print(str(e))
        return (str(e))


def uniquedress_view():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df1_unique_dress = pd.read_sql("select distinct * from projectdb.attributedataset where dress_id in (select  dress_id from projectdb.attributedataset); ", mydb)
        print("Unique Dress record\n")
        print(df1_unique_dress)
    except Exception as e:
        print("unique record issue ")
        print(str(e))
        return (str(e))

def zero_recommendation_record():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        count_recommendation_zero = pd.read_sql("select count(*) from projectdb.attributedataset where recommendation = 0; ", mydb)
        print(f"zero recommendation record: {count_recommendation_zero} ")
    except Exception as e:
        print("unique record issue")
        print(str(e))
        return (str(e))

def sum_of_sale():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df_dress = pd.read_sql("select * from projectdb.dresssale ; ", mydb)
        print(df_dress)
        column_names = list(df_dress.columns)
        df_dress['sum_of_sale'] = df_dress[column_names[1:]].sum(axis=1)
        print(df_dress)
        return(df_dress)
    except Exception as e:
        print("Sum of Sales issue ")
        print(str(e))
        return (str(e))

def third_highest_sum_of_sale():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df_dress = pd.read_sql("select * from projectdb.dresssale ; ", mydb)
        column_names = list(df_dress.columns)
        df_dress['sum_of_sale'] = df_dress[column_names[1:]].sum(axis=1)
        df_sorted = df_dress.sort_values(by=['sum_of_sale'], ascending=False)
        print(df_sorted)
        number1 = int(input("What highest number Total Sales value you want to print: "))
        print(df_sorted[number1:number1+1])
    except Exception as e:
        print("Third Highest issue")
        print(str(e))
        return (str(e))

