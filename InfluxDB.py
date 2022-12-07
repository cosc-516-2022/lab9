import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from collections import OrderedDict
from csv import DictReader
import reactivex as rx
from reactivex import operators as ops

token = os.environ.get("INFLUXDB_TOKEN")
org = ""
url = ""
bucket=""

"""
TO-DO: Make a connection to the database
"""
def connect():
    global client
    #Add the client connection statement here
    client= " "

    return client

"""
TO-DO: Parse the file to return Point before loading the data.
"""
def parse_row(row: OrderedDict):
    #Return Point structure after defining the tags and fields here. 
    return 0

"""
TO-DO: Delete all data from the database. No return statement needed.
"""
def drop():
    delete_api=""

"""
TO-DO: load all the data from the vix-daily.csv into the bucket with measurement name 
financial-analysis, tag="type" with value of "vix-daily" and fields = "open","high","low" and "close".
"""
def load():
    data = ""

    """
    Create client that writes data in batches with 50_000 items and flush_interval=10,000
    """
    

"""
TO-DO : Query the data of field "open" (VIX-Open), sort by value in ascending and limit by 5
"""
def query0():
    query = ' '
    
    #Run the query
    result = ' '
    return result


"""
TO-DO : Query the maximum value of each field "high", "open", "close" and "low" from the whole data
"""
def query1():
    query = ''
    
    #Run the query
    result = ' '
    return result

"""
TO-DO : Query the data of field "high" (VIX-High) from Date 2006-12-26 to 2007-01-08, and sort by value in descending.
"""
def query2():
    query= ''

    #Run the query
    result = ' '
    return result

"""
TO-DO : Query the mean of "low" (VIX-Low) values per month starting from Date 2006-01-01 to 2006-12-31. Hint: use aggregateWindow()
"""
def query3():
    query= ''

    #Run the query
    result = ' '
    return result

"""
TO-DO : Query the total count of each field.
"""
def query4():
    query= ''

    #Run the query
    result = ' '
    return result

def result_process(query):
    """
    Processing results
    """
    res_str=[]
    print("=== results ===")
    
    if(query==" "):
        print("None")
        return 0
    
    for table in query:
        for record in table.records:
            res_str.append('{0},{1},{2}'.format(record.get_time().date(),record.get_field(),round(record.get_value(),3)))
            print('{0},{1},{2}'.format(record.get_time().date(),record.get_field(),round(record.get_value(),3)))
    print()
    return res_str

def main():
    global client
    client=connect()
    drop()

    load()

    print("Limit 5 Values")
    result_process(query0())

    print("Maximum Values")
    result_process(query1())

    print("VIX High only")
    result_process(query2())

    print("Mean of low values per month")
    result_process(query3())

    print("Count of each field")
    result=query4()
    if result!=" ":
        count={}
        for table in result:
                for record in table.records:
                    count[str(record.get_field())]=str(round(record.get_value(),3))
        print(count)
    else:
        print("None")
        
if __name__=='__main__':
    main()


