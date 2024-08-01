#read data from the file and handle null values

import pandas as pd
df=pd.read_csv('orders.csv',na_values=['Not Available','unknown'])
df.head(20)

df['Ship Mode'].unique()

#df.rename(columns={'Order Id':'order_id','City':'city'})
#df.columns
df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(' ','_')

df.head(5)

df['discount']=df['list_price']*df['discount_percent']*.01
df['sale_price']=df['list_price']-df['discount']
df['profit']=df['sale_price']-df['cost_price']
df

#df.dtypes
#convert order date from object data type to datetime
df['order_date']=pd.to_datetime(df['order_date'],format="%Y-%m-%d")
df

#drop cost price list price and discount percent columns
df.drop(columns=['list_price','cost_price','discount_percent'],inplace=True)
df

#load the data into sql server using replace option

import sqlalchemy as sal

# Define server and database names
server_name = r'LAPTOP-NNNCLTMV\SQLSERVER'
database_name = 'master'

# Define the connection string using the correct format
connection_string = (
    f'mssql+pyodbc://{server_name}/{database_name}'
    '?driver=ODBC+Driver+18+for+SQL+Server'
    '&TrustServerCertificate=yes'
)


# Create the SQLAlchemy engine
engine = sal.create_engine(connection_string)

# Establish the connection
try:
    conn = engine.connect()
    print("Connection to SQL Server established successfully")
    
    # Write DataFrame to SQL table
    try:
        df.to_sql('df_orders', con=conn, index=False, if_exists='append')
        print("DataFrame written to SQL table successfully")
    except Exception as e:
        print(f"Error occurred while writing DataFrame to SQL table: {e}")
    finally:
        # Close the connection
        conn.close()
        print("Connection to SQL Server closed")
        
except Exception as e:
    print(f"Error occurred while establishing connection to SQL Server: {e}")

