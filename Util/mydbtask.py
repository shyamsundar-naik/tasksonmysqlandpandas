import mysql.connector as conn
import pymongo

class dbTablesQuery:
    """This class meant for preparing queries"""
    def __init__(self):
        self.__createQueryAttr = """create table attribute(Dress_ID varchar(30), Style varchar(30),Price varchar(30), Rating float, 
                                            Size varchar(20), Season varchar(30),	NeckLine varchar(30),	SleeveLength varchar(30),	
                                            waiseline varchar(30), Material varchar(30),	FabricType varchar(30),	Decoration varchar(30),
                                            	Pattern_Type varchar(30),	Recommendation int)"""
        self.__createQueryDress = """create table Dress(Dress_ID varchar(40), 29_8_2013 int,	31_8_2013 int,	09_02_2013 int,	
                                            09_04_2013 int,	09_06_2013 int,	09_08_2013 int,	09_10_2013 int,	09_12_2013 int,	14_9_2013 int,
                                            16_9_2013 int,	18_9_2013 int,	20_9_2013 int,	22_9_2013 int,	24_9_2013 int,	26_9_2013 int,
                                            28_9_2013 int,	30_9_2013 int,	10_02_2013 int,  10_04_2013 int, 10_06_2013 int,
                                             10_08_2010	int, 10_10_2013	int, 10_12_2013 int)"""

    def getCQA(self):
        return self.__createQueryAttr

    def getCQD(self):
        return self.__createQueryDress

    def prepInsAtt(self,csvfilepath):
        f = open(csvfilepath,"r")
        t = f.read()#Read Csv file
        sp = t.split('\n')#Split all lines
        str = ""#Create an empty string
        for i in sp:#loop each line
            if not (i.find('Dress_ID') >= 0) and i.strip() != '':#Check if a line doesn't have header information and an empty string
                s = i.split(',')#Split individual column values
                dt = '('# Start of new record
                for ind in range(len(s)):
                    if ind == 3:#if index is 3 then the float value
                        dt = dt + s[ind] + ","
                    elif ind == (len(s) - 1):#if index is the last value then do not consider comma
                        dt = dt + s[ind]
                    else:#Add as string value with comma separated
                        dt = dt + "'" + s[ind] + "',"
                dt = dt + "),"#End of record
                str += dt #combine entire records
        str= str[0:-1]
        query = "insert into attribute values " + str + " "
        return query

    def prepInsDress(self,csvfilepath):
        f = open(csvfilepath, 'r')
        t = f.read()#read csv file
        sp = t.split('\n')#split all records
        str1 = ""#Empty string
        for i in sp:#Iterate through individual records
            if not (i.find('Dress_ID') >= 0) and i.strip() != '':#check if not header line and empty string
                s = i.split(',')#Split into individual values
                dt = '('#New Record start
                for ind in range(len(s)):#Iterate all values
                    if ind == 0 and ind <= 23:#First index
                        dt = dt + "'" + s[ind] + "',"#String value
                    elif ind == (len(s) - 1) and ind <= 23:
                        dt = dt + s[ind]#last record then ignore comma
                    elif s[ind].strip() != '' and ind <= 23:#if no value
                        dt = dt + s[ind] + ","#Integer value
                    elif s[ind].strip() == '' and ind <= 23:#if no value
                        dt = dt + str(0) + ","#Integer value

                dt = dt + "),"#End of record
                dt = dt.replace(',),', '),')#Extra values created at end removing it
                str1 += dt #combining all records
        str1 = str1[0:-1]
        query = "insert into Dress values " + str1 + ""
        return query

    def __prepQuerySumDressID(self,listCol):#Combine all columns for addition
        query = ''
        for i in listCol:
            if i.strip() != 'Dress_ID':
                query = query + " " + i + " +"
        query = query[0:-1]
        return query

    def sumQuery(self,listCol):#prepare query for adding of sales
        query = self.__prepQuerySumDressID(listCol)
        query = "select Dress_ID, sum(" + query +") as sum from Dress group by Dress_ID"
        return query

    def maxQuery(self,listCol):#Prepare query for
        query = self.__prepQuerySumDressID(listCol)
        query = "select Dress_ID, min(sum) from (select Dress_ID, sum(" + query + ") as sum from Dress group by Dress_ID order by sum(" + query +") desc limit 3) a"
        return query

class mysqlWorks:
    """Any mysql related works are accomplished here"""
    def __init__(self):
        self.__mydb = conn.connect(host="localhost", user="root", passwd="shyam")


    def executeQuery(self,query):
        cursor = self.__mydb.cursor()
        if self.__mydb.is_connected():
            cursor.execute(query)
        else:
            self.__mydb.cmd_reset_connection()
            cursor = self.__mydb.cursor()
            cursor.execute(query)


    def commit(self):
        self.__mydb.commit()

    def selExecuteQuery(self,query):
        cursor = self.__mydb.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def getColumnNames(self,databasename, tablename):
        query = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='"+ databasename +"' AND `TABLE_NAME`='"+tablename+"';"
        list1 = list(self.selExecuteQuery(query))
        return list1


class mongoWorks:
    def __init__(self):
        self.__con = pymongo.MongoClient("mongodb+srv://shyam:shyam@cluster0.xzmbj.mongodb.net/?retryWrites=true&w=majority")
        self.__database = self.__con["mongoworks"]
        self._collections = self.__database['attribute']

    def addValues(self,json):
        self._collections.insert_many(json)
