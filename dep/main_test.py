import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.datasetup import *

load_dotenv()

account_storage = os.environ.get('ACCOUNT_STORAGE')

azureDB = AzureDB()
azureDB.access_container("csv-files")
azureDB.list_blobs()
df = azureDB.access_blob_csv("login.csv")
print(df.head())






class MainETL():
    # List of columns need to be replaced
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
    
    def extract(self, csv_file="ETL_Example_Data.csv"):
        # Step 1: Extract: use pandas read_csv to open the csv file and extract data
        print(f'Step 1: Extracting data from csv file')
        self.fact_table = df
        print(f'We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}')
        print(f'Step 1 finished')
        
    def mainLoop(self):    
        # Step 1
        self.extract()
        ## Step 2
        self.transform()
        ## Step 3
        #try:
        #    database.delete_sqldatabase('Total_Pay_Fact')
        #    self.load()
        #except:
        #    self.load()
        
def main():
    # create an instance of MainETL
    main = MainETL()
    main.mainLoop()
    
if __name__ == '__main__':
    main()