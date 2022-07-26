# mysql:
#     1. Create a table attribute dataset and dress dataset
#     2. Do a bulk load for these two table for respective dataset using code
#     3. Read these dataset in pandas as a dataframe
#     4. convert attribute dataset in json format
#     5. Store this dataset into Mongodb
#     6. in sql task try to perform left join operation with attribute dataset and dress dataset on column Dress_ID
#     7. Write a sql query to find out how many unique dress that we have based on dress id
#     8. Try to find out how many dress is having recommendation 0 in attribute dataset
#     9. Try to find out total dress sell for individual dressid. Sum all columns value
#     10. Try to find out a third highest most selling dress


import dbconnection
import datahandling

while True:
    choice = int(input("\n1. Create a table attribute dataset and dress dataset\n2. Do a bulk load for these two table for respective dataset using code\n3. Read these dataset in pandas as a dataframe\n4. convert attribute dataset in json format\n5. Store this dataset into Mongodb\n6. in sql task try to perform left join operation with attribute dataset and dress dataset on column Dress_ID\n7. Write a sql query to find out how many unique dress that we have based on dress id\n8. Try to find out how many dress is having recommendation 0 in attribute dataset\n9. Try to find out total dress sell for individual dressid. Sum all columns value\n10. Try to find out a third highest most selling dress\n11. Read Excel\n"))
    if choice   == 1:
        dbconnection.create_table()
    elif choice == 2:
        dbconnection.one_time_load()
    elif choice == 3:
        df1,df2= datahandling.read_attributedataset()
    elif choice == 4:
        dj1,dj2= datahandling.convert_json(df1,df2)
    elif choice == 5:
        # datahandling.storetomongodb(dj1,dj2)
        datahandling.storetomongodb()
    elif choice == 6:
        datahandling.leftjoin_operation()
    elif choice == 7:
        datahandling.uniquedress_view()
    elif choice == 8:
        datahandling.zero_recommendation_record()
    elif choice == 9:
        df_dress= datahandling.sum_of_sale()
    elif choice == 10:
        df_dress= datahandling.third_highest_sum_of_sale()
    elif choice == 11:
        dbconnection.read_excel()
    else:
        print("Have a great day")