# Purpose: Test that Python can talk to our
# Azure SQL Database before we do anything else

#---import---

import pyodbc   #the tool that lets Python talk to SQL databases
from dotenv import load_dotenv # the tool that reads our .env file
import os   # the tool that lets us read environment variables


load_dotenv() # reads the .env file and loads all the variables so we can use it

# --- READ CREDENTIALS ---

def get_connection():
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")

    #--- build connection string ---

    # It tells Python exactly WHERE the database is and HOW to authenticate
    connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"encrypt=yes;"
            f"trustservercertificate=no;" 
            "connection timeout=30;"
        )

    #--- test connection ---

    try:
        conn=pyodbc.connect(connection_string) #attempt to connect to the database using the connection string we built
        return conn
        # print("connection successful!Azure SQL Database is ready to use.")
        # conn.close() #close the connection after testing it, we don't want to leave open connections hanging around
    except Exception as e:
        print(f"database connection failed:   {e}")
        
def test_connection():
    try:
        conn =get_connection()
        print("connection successful! Azure SQL Database is ready to use.")
        conn.close() #close the connection after testing it, we don't want to leave open connections hanging around
        return True
    except Exception as e:
        print(f"connection failed. here is the error:   {e}")
        return False

if __name__ == "__main__":
    test_connection()