import os
import csv
from pymongo import *
from pymongo.objectid import ObjectId
import MySQLdb
import sys
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import *
import dateutil.parser
from BeautifulSoup import BeautifulSoup as BSHTML


todayDate=date.today()
todayDate = todayDate.strftime("%d-%b-%Y")
todayDate = datetime.strptime(todayDate,"%d-%b-%Y")

previousDate=todayDate+relativedelta(days=-1)
previousDate = previousDate.strftime("%d-%b-%Y")
previousDate = datetime.strptime(previousDate,"%d-%b-%Y")

day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(todayDate, time(0, 0))

    ############################## Mongo Connector Code #############################
    #################################################################################
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

    ############################# MySQL Connector Code###############################
    #################################################################################
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)


try:
    mongo_conn_1 = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)
except:
    mongo_conn_1 = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)

mongo_conn_2 = getMongoConnection('172.22.65.88',27018, 'sumoplus',True, True)

static_collection = getattr(mongo_conn_1,'CandidateStatic')
hobbies_collection = getattr(mongo_conn_2,'XmlDump')
    
mysql_conn = getMySqlConnection('localhost', 3306, 'root', 'mysql321', 'INDExportDB')
cursor = mysql_conn.cursor()

def getDataFromMongoBulk():
    
    truncate_table_query = ''' Truncate Table Candidates_Data_New_V5 '''
    cursor.execute(truncate_table_query)
    mysql_conn.commit()
    
    id = '0'		
    #id = '1e292d92ccf8a24e6600036f'
    start = 0
    
    while True:
        
        
        
        
        Static_Data = static_collection.find({'_id':{'$gt':id}},{'_id':1,'e':1,'cp':1,'rsd':1,'red':1,'svi':1,'evi':1,'rm':1,'mo':1}).sort('_id',1).limit(1000)	

        #Static_Data = static_collection.find({'_id':{'$gt':ObjectId(id)}},{'_id':1,'e':1,'cp':1,'rsd':1,'red':1,'svi':1,'evi':1,'rm':1,'mo':1}).sort('_id',1).limit(1000)
        #Static_Data = static_collection.find({'_id':ObjectId("58ee0af33f4bfd7d57158f8c")},{'_id':1})

        query_values= ''
    	User_Id_List = []
        for records in Static_Data:
            '''print str(records['_id'])
            xml_dump = hobbies_collection.find({'c':str(records['_id'])})
            for hobbies in xml_dump:
                print hobbies
                
                xml = hobbies.get('x','N/A')
                bs = BSHTML(xml)
                hobbies = bs.activities
                print hobbies
            sys.exit(0)'''
            User_Id_List.append(str(records['_id']))
            user_id = records.get('_id','N/A')
            email = records.get('e','N/A')
            cellphone = records.get('cp','N/A')
            registration_start_date = records.get('rsd',"1900-01-01 00:00:00")
            registration_end_date = records.get('red',"1900-01-01 00:00:00")
            if records.get('svi','') is None:
                start_vendor_id = -100
            else:
                start_vendor_id = records.get('svi',-100)
            if records.get('evi','') is None:
                end_vendor_id = -100
            else:
                end_vendor_id = records.get('evi',-100)
            
            rm = records.get('rm',-1)
            mo = records.get('mo',-1)
            
            try:
                query_values = query_values + '''("%s","%s","%s","%s","%s","%s","%s","%s","%s")''' % (user_id,email,cellphone,registration_start_date,registration_end_date,start_vendor_id,end_vendor_id,rm,mo) + ','
            except Exception as e:
                print e
        try:
            insert_query = '''INSERT INTO INDExportDB.Candidates_Data_New_V5 VALUES '''
            #print insert_query + query_values
            cursor.execute(insert_query + query_values[:-1])
            mysql_conn.commit()
        except:
            pass
                
        start  = start + 1000
    	id = User_Id_List[-1]
        
        print 'Total Records Processed:' +str(start)
        print 'Last Record Inserted:',records['_id']
        print 'Last Record of List:',id 
    	print 'len list:',str(len(User_Id_List))
        
        
        if len(User_Id_List) < 1000 and len(str(user_id)) > 20:
            break 
        if len(User_Id_List) < 1000 and len(str(user_id)) < 20:
            user_id = ObjectId('1e292d92ccf8a24e6600036f')
        
        if len(str(user_id)) < 20 :
            id = user_id
        else:
            id = ObjectId(str(user_id))
            
def getDataFromMongoIncremental():
    start = '0'
    batch_size = 1000
    while True:
        Static_Data = static_collection.find({'$or':[{'ll':{'$gte':day1,'$lt':day2}},{'lm':{'$gte':day1,'$lt':day2}}]},{'_id':1,'e':1,'cp':1,'rsd':1,'red':1,'svi':1,'evi':1}).sort('_id',1).skip(start).limit(batch_size)
        start += batch_size
        query_values= ''
        User_Id_List = []
        for records in Static_Data:
            User_Id_List.append(str(records['_id']))
            user_id = records.get('_id','N/A')
            email = records.get('e','N/A')
            cellphone = records.get('cp','N/A')
            registration_start_date = records.get('rsd',"1900-01-01 00:00:00")
            registration_end_date = records.get('red',"1900-01-01 00:00:00")
            if records.get('svi','') is None:
                start_vendor_id = -100
            else:
                start_vendor_id = records.get('svi',-100)
            if records.get('evi','') is None:
                end_vendor_id = -100
            else:
                end_vendor_id = records.get('evi',-100)
            try:
                
                query_values = query_values + '''("%s","%s","%s","%s","%s","%s","%s")''' % (user_id,email,cellphone,registration_start_date,registration_end_date,start_vendor_id,end_vendor_id) + ','
                incremental_part = ''' ON DUPLICATE KEY UPDATE ''' + ''' user_id = "%s",email = "%s",cellphone = "%s",registration_start_date = "%s",registration_end_date = "%s",start_vendor_id="%s",end_vendor_id= "%s" ''' %(user_id,email,cellphone,registration_start_date,registration_end_date,start_vendor_id,end_vendor_id) +','
            except Exception as e:
                print e
            
        try:
            insert_query = '''INSERT INTO INDExportDB.Candidates_Data_New_V5 VALUES '''
            cursor.execute(insert_query + query_values[:-1] + incremental_part[:-1])
            mysql_conn.commit()
        except:
            pass
        print 'Last Record Inserted :' +str(records['_id']) 
        print "Records Processed : " + str(start)         
        print "Len of List:" +str(len(User_Id_List)) 
def main():
    
    strt_time = datetime.now()
    getDataFromMongoBulk()
    finish_time = datetime.now()
    print 'Time Taken :' +str(finish_time - strt_time)
    
    
    #getDataFromMongoIncremental()

if __name__ == '__main__':
    main()
