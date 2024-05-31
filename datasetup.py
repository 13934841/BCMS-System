import os, pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import json
import time

load_dotenv()


username = os.environ.get('USERNAME_AZURE')
password = os.environ.get('PASSWORD')
server = os.environ.get('SERVER')
database = os.environ.get('DATABASE')
account_storage = os.environ.get('ACCOUNT_STORAGE')
connection_string = "Driver={ODBC Driver 18 for SQL Server};"+f"Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;MultipleActiveResultSets = true;"


# Using pyodbc
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server')

class AzureDB():
    def __init__(self, local_path = "./data", account_storage = account_storage):
        self.local_path = local_path
        self.account_url = f"https://{account_storage}.blob.core.windows.net"
        self.default_credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(self.account_url, credential=self.default_credential)
        
    def access_container(self, container_name): 
        # Use this function to create/access a new container
        try:
            # Creating container if not exist
            self.container_client = self.blob_service_client.create_container(container_name)
            print(f"Creating container {container_name} since not exist in database")
            self.container_name = container_name
    
        except Exception as ex:
            print(f"Acessing container {container_name}")
            # Access the container
            self.container_client = self.blob_service_client.get_container_client(container=container_name)
            self.container_name = container_name
            
    def delete_container(self):
        # Delete a container
        print("Deleting blob container...")
        self.container_client.delete_container()
        print("Done")
        
    def upload_blob(self, blob_name, blob_data = None):
        # Create a file in the local data directory to upload as blob to Azure
        local_file_name = blob_name
        upload_file_path = os.path.join(self.local_path, local_file_name)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=local_file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        if blob_data is not None:
            blob_client.create_blob_from_text(container_name=self.container_name, blob_name=blob_name, text=blob_data)
        else:
            # Upload the created file
            with open(file=upload_file_path, mode="rb") as data:
                blob_client.upload_blob(data)
                
    def list_blobs(self):
        print("\nListing blobs...")
        # List the blobs in the container
        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)  
            
    def download_blob(self, blob_name):
        # Download the blob to local storage
        download_file_path = os.path.join(self.local_path, blob_name)
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(file=download_file_path, mode="wb") as download_file:
                download_file.write(self.container_client.download_blob(blob_name).readall())
                
    def delete_blob(self, container_name: str, blob_name: str):
        # Deleting a blob
        print("\nDeleting blob " + blob_name)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()
        
    def access_blob_csv(self, blob_name):
        # Read the csv blob from Azure
        try:
            print(f"Acessing blob {blob_name}")
            
            df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8')))  
            return df      
        except Exception as ex:
            print('Exception:')
            print(ex)
            
    def upload_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists='replace', index=False)
        primary = blob_name.replace('Dim', 'ID')

        # Identify columns of type varchar(max) and convert them to varchar(255)
        alter_statements = []

        with engine.connect() as con:
            trans = con.begin()  # Use existing transaction
            try:
                columns_to_convert = con.execute(text(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{blob_name}' AND DATA_TYPE = 'varchar'"))

                for column in columns_to_convert:
                    column_name = column[0]
                    alter_statements.append(f'ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {column_name} VARCHAR(255)')
                
                if not alter_statements:
                    print("No columns of type varchar found for conversion.")
                else:
                    for alter_statement in alter_statements:
                        print(f"Executing: {alter_statement}")
                        con.execute(text(alter_statement))
                    trans.commit()

            except Exception as e:
                trans.rollback()
                print(f"Error converting columns: {e}")

            finally:
                trans.close()  # Close the transaction manually

        # Identify and set foreign keys for columns ending with "ID"
        with engine.connect() as con:
            trans = con.begin()
            foreign_keys = con.execute(text(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{blob_name}' AND COLUMN_NAME LIKE '%ID' AND COLUMN_NAME != '{primary}'"))
            
            alter_statements = []
            for foreign_key in foreign_keys:
                foreign_key_column = foreign_key[0]
                constraint_name = f'FK_{blob_name}_{foreign_key_column}'
                referenced_table = foreign_key_column[:-2]  # Removing 'ID' to get referenced table name
                referenced_table_dim = referenced_table + 'Dim'  # Append 'Dim' suffix
                
                # Build ALTER TABLE statement
                alter_statement = f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [{constraint_name}] FOREIGN KEY ({foreign_key_column}) REFERENCES [{referenced_table_dim}]([{foreign_key_column}]) ON UPDATE CASCADE ON DELETE CASCADE;'
                alter_statements.append(alter_statement)

            # Execute all ALTER TABLE statements in a single transaction
            for attempt in range(3):  # Retry 3 times
                try:
                    for alter_statement in alter_statements:
                        con.execute(text(alter_statement))
                    break  # Break out of retry loop if successful
                except Exception as e:
                    print(f"Error setting foreign key constraint ({attempt+1}/3): {e}")
                    time.sleep(3)  # Wait for 3 seconds before retrying
            else:
                print("Failed to set foreign key constraint after 3 attempts. Skipping...")

            trans.commit()

        # Determine the data type of the primary key column
        primary_key_data_type = 'int'
        try:
            primary_key_values = blob_data[primary]
            if primary_key_values.dtype == 'object':
                primary_key_data_type = 'varchar(255)'
        except KeyError:
            print(f"Primary key column {primary} not found in the dataframe.")

        # Set primary key for the table
        with engine.connect() as con:
            trans = con.begin()
            try:
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] alter column {primary} {primary_key_data_type} NOT NULL'))
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);'))
            except Exception as e:
                print(f"Error setting primary key constraint: {e}")

            trans.commit()
                
    def append_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nAppending to table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists='append', index=False)
    
    def delete_sqldatabase(self, table_name):
        with engine.connect() as con:
            trans = con.begin()
            con.execute(text(f"DROP TABLE [dbo].[{table_name}]"))
            trans.commit()
            
    def get_sql_table(self, query):        
        # Create connection and fetch data using Pandas        
        df = pd.read_sql_query(query, engine)
        # Convert DataFrame to the specified JSON format
        result = df.to_dict(orient='records')
        return result
