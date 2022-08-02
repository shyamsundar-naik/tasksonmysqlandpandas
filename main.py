import logging
import pandas as pd
import json
import Util.mydbtask
logging.basicConfig(filename="Tasks.log",level=logging.DEBUG ,format='%(levelname)s %(asctime)s %(name)s  %(message)s')

logging.info('Create database called mysqltasks')
try:
    db = Util.mydbtask.mysqlWorks()
    wb = Util.mydbtask.dbTablesQuery()
    db.executeQuery("create database mysqltasks")
    db.executeQuery("use mysqltasks")
    logging.info("We have created mysqltasks database and currently using mysqltasks database")
except  Exception as e:
    logging.error(e)

logging.info('Task1'.center(100,'*'))
logging.info("1. Create a  table attribute dataset and dress dataset")

try:
    db.executeQuery(wb.getCQA()) # Creating attribute table
    db.executeQuery(wb.getCQD()) # Creating Dress table
    logging.info("We have successfully created tables attribute and dress under database mysqltasks")
except Exception as e:
    logging.error(e)

logging.info('Task2'.center(100,'*'))
logging.info("2. Do a bulk load for these two table for respective dataset")

try:
    db.executeQuery(wb.prepInsAtt("C://Users//Shyam//Downloads//Attribute DataSet.csv"))# Bulk Insert in attribute table
    db.commit()
    db.executeQuery(wb.prepInsDress("C://Users//Shyam//Downloads//Dress Sales.csv"))  # Bulk Insert in attribute table
    db.commit()
    logging.info("We have successfully performed bulk insert into tables attribute and dress under database mysqltasks")
except Exception as e:
    logging.error(e)

logging.info('Task3'.center(100, '*'))
logging.info("3. read these dataset in pandas as a dataframe")

try:
    df = pd.DataFrame(db.selExecuteQuery("Select * from attribute"))
    listAttrCol = db.getColumnNames('mysqltasks','attribute')
    laCol = [i[0] for i in listAttrCol]
    logging.info("reading data from attribute data into dataframe")
    df.columns = laCol
    logging.info(df.head())
    df1 = pd.DataFrame(db.selExecuteQuery("Select * from Dress"))
    listDressCol = db.getColumnNames('mysqltasks', 'Dress')
    ldCol = [i[0] for i in listDressCol]
    df1.columns = ldCol
    logging.info("reading data from Dress data into dataframe")
    logging.info(df1.head())
except Exception as e:
    logging.error(e)

logging.info('Task4'.center(100, '*'))
logging.info("4. Convert attribute dataset in json format")

try:
    attr_json = df.to_json()
    logging.info(attr_json)
except Exception as e:
    logging.error(e)

print()

logging.info('Task5'.center(100, '*'))
logging.info("5. Store this dataset into mongodb")
attr_json = "[" + attr_json + "]"
f = json.loads(attr_json)

try:
    mgw = Util.mydbtask.mongoWorks()
    mgw.addValues(f)
    logging.info("We have inserted values into mongodb")
except Exception as e:
    logging.error(e)


logging.info('Task6'.center(100, '*'))
logging.info("6. in sql task try to perform left join operation with attrubute dataset and dress dataset on column Dress_ID")

try:
    leftJoinQuery = "select * from attribute a left join Dress d on a.Dress_ID = d.Dress_ID"
    df2 = pd.DataFrame(db.selExecuteQuery(leftJoinQuery))
    df2.columns = laCol + ldCol
    logging.info("We have written left join query. Please find the results")
    logging.info(df2.head())
except Exception as e:
    logging.error(e)


logging.info('Task7'.center(100, '*'))
logging.info("7. Write a sql query to find out how many unique dress that we have based on dress id")

try:
    uniqQuery = "select count(distinct Dress_ID) from attribute"
    count = str(db.selExecuteQuery(uniqQuery ))
    logging.info("We have written query. Please find the results")
    count = count.replace('[(','')
    count = count.replace(',)]', '')
    logging.info(count)
except Exception as e:
    logging.error(e)

logging.info('Task8'.center(100, '*'))
logging.info("8. Try to find out how mnay dress is having recommendation 0")

try:
    recQuery = "select * from attribute where Recommendation = 0"
    recDf = pd.DataFrame(db.selExecuteQuery(recQuery ))
    recDf.columns = laCol
    logging.info("We have written query. Please find the results")
    logging.info(recDf.head())
except Exception as e:
    logging.error(e)

logging.info('Task9'.center(100, '*'))
logging.info("9. Try to find out total dress sell for individual dress id")

try:
    sumQuery = wb.sumQuery(ldCol)
    sumDf = pd.DataFrame(db.selExecuteQuery(sumQuery ))
    sumDf.columns = ['Dress_ID','Total sell']
    logging.info("We have written query. Please find the results")
    logging.info(sumDf.head())
except Exception as e:
    logging.error(e)


logging.info('Task10'.center(100, '*'))
logging.info("10. Try to find out a third highest most selling dress id ")

try:
    mQuery = wb.maxQuery(ldCol)
    mDf = pd.DataFrame(db.selExecuteQuery(mQuery))
    mDf.columns = ['Dress_ID','Total sell']
    logging.info("We have written query. Please find the results")
    logging.info(mDf)
except Exception as e:
    logging.error(e)
