import mysql.connector

'''Python connection to MySQL localhost, which will create a database BankCustomers
   and create a table customerAccounts'''

def connect_to_server():
    connection = None
    try:
        connection = mysql.connector.connect(user = '<user>',
                                             password = '<password>',
                                             host ='localhost',
                                             port = '3306')
        print("Connection to server successful")
    
    except Exception as error:
        print("Error while connecting to server", error)
    
    return connection

def connect_database(connection):
    mycursor = connection.cursor()
    print('Creating database JobTracker')
    mycursor.execute("CREATE DATABASE IF NOT EXISTS JobTracker;")
    
    try:
        connection = mysql.connector.connect(user = '<user>',
                                             password = '<password>',
                                             host ='localhost',
                                             port = '3306',
                                             database = 'JobTracker')
        print("Connection to JobTracker successful")
    
    except Exception as error:
        print("Error while connecting to server", error)
    
    return connection

def create_table(connection):
    sql_statement = """CREATE TABLE IF NOT EXISTS jobs(
                                    job_id VARCHAR(100) NOT NULL PRIMARY KEY,
                                    status VARCHAR(10),
                                    updated_time DATETIME)"""
    
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    cursor.close()

server_connection = connect_to_server()
db_connection = connect_database(server_connection)
create_table(db_connection)