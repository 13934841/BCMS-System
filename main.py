import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from datasetup import *
from dimension_classes import *

class MainETL():
    # List of columns need to be replaced
    def __init__(self) -> None:
        self.dimension_tables = []
    
    def extract(self, csv_file="ETL_Example_Data.csv"):
        # Step 1: Extract: use pandas read_csv to open the csv file and extract data
        print(f'Step 1: Extracting data from csv file')
        #self.fact_table = df
        #print(f'We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}')
        print(f'Step 1 finished')
    
    def transform(self):
        #fetch client
        dim_client = DimClient()
        new_dim = dim_client.dimension_table
        new_dim['reg_date'] = pd.to_datetime(new_dim['reg_date'],unit='s')
        new_dim['login'] = new_dim['login'].astype(int)
        new_dim[['country', 'account_currency']] = new_dim[['country', 'account_currency']].astype(str)
        with open('country_mapping.json', 'r') as file:
            country_mapping = json.load(file)
        new_dim['country'] = new_dim['country'].replace(country_mapping)
        new_dim.rename(inplace=True, columns={'login':'ClientID', 'country':'Country', 'account_currency':'AccountCurrency',
                                              'reg_date':'RegistrationDate'})
        dim_client.dimension_table = new_dim
        self.dimension_tables.append(dim_client)
        print(dim_client.dimension_table)
        
        #fetch symbol
        dim_symbol = DimSymbol()
        new_dim = dim_symbol.dimension_table
        new_dim[['symbol', 'description', 'type']] = new_dim[['symbol', 'description', 'type']].astype(str)
        new_dim.rename(inplace=True, columns={'symbol':'SymbolID', 'description':'SymbolDescription', 'type':'Type'})
        dim_symbol.dimension_table = new_dim
        self.dimension_tables.append(dim_symbol)
        print(dim_symbol.dimension_table)
        
        #fetch reason
        dim_reason = DimReason()
        new_dim = dim_reason.dimension_table
        new_dim['reason'] = new_dim['reason'].astype(str)
        new_dim['code'] = new_dim['code'].astype(int)
        new_dim.rename(inplace=True, columns={'code':'ReasonID', 'reason':'ReasonDescription'})
        dim_reason.dimension_table = new_dim
        self.dimension_tables.append(dim_reason)
        print(dim_reason.dimension_table)
        
        #fetch trades
        dim_trades = DimTrades()
        new_dim = dim_trades.dimension_table
        int_col = ['ticket', 'login', 'cmd', 'reason']
        float_col = ['volume', 'open_price', 'close_price', 'tp', 'sl', 'commission', 'swaps', 'profit', 'volume_usd']
        new_dim['symbol'] = new_dim['symbol'].astype(str)
        new_dim[int_col] = new_dim[int_col].astype(int)
        new_dim[float_col] = new_dim[float_col].astype(float)
        new_dim['open_time'] = pd.to_datetime(new_dim['open_time'],unit='s')
        new_dim['close_time'] = pd.to_datetime(new_dim['close_time'],unit='s')
        new_dim.rename(inplace=True, columns={'ticket':'TradeID', 'login':'ClientID', 'symbol':'SymbolID', 'cmd':'TradeDirection', 'volume':'Volume', 'open_time':'OpenTime', 'open_price':'OpenPrice', 
                                              'close_time':'CloseTime', 'close_price':'ClosePrice', 'tp':'TakeProfit', 'sl':'StopLoss', 'reason':'ReasonID', 'commission':'Commission', 'swaps':'Swaps',
                                              'profit':'Profit', 'volume_usd':'VolumeUSD'})
        dim_trades.dimension_table = new_dim
        self.dimension_tables.append(dim_trades)
        print(dim_trades.dimension_table)
        
        #fetch daily_chart
        dim_daily_chart = DimDailyChart()
        new_dim = dim_daily_chart.dimension_table
        new_dim['date'] = pd.to_datetime(new_dim['date'])
        new_dim['close'] = new_dim['close'].astype(float)
        new_dim['CurrencyID'] = new_dim['CurrencyID'].astype(str)
        new_dim.rename(inplace=True, columns={'date':'Date', 'close':'ClosePrice', 'CurrencyID':'SymbolID'})
        dim_daily_chart.dimension_table = new_dim
        self.dimension_tables.append(dim_daily_chart)
        print(dim_daily_chart.dimension_table)
        
        #fetch daily_report
        dim_daily_report = DimDailyReport()
        new_dim = dim_daily_report.dimension_table
        new_dim['record_time'] = pd.to_datetime(new_dim['record_time'])
        new_dim['login'] = new_dim['login'].astype(int)
        float_col = ['net_deposit', 'balance', 'equity', 'credit', 'profit_closed', 'profit_floating', 'margin']
        new_dim[float_col] = new_dim[float_col].astype(float)
        new_dim.rename(inplace=True, columns={'login':'ClientID', 'record_time':'RecordTime', 'net_deposit':'NetDeposit',
                                              'balance':'Balance', 'equity':'Equity', 'credit':'Credit', 'profit_closed':'ProfitClosed',
                                              'profit_floating':'ProfitFloating', 'margin':'Margin'})
        dim_daily_report.dimension_table = new_dim
        self.dimension_tables.append(dim_daily_report)
        print(dim_daily_report.dimension_table)
        
        #fetch role
        dim_role = DimRole()
        new_dim = dim_role.dimension_table
        new_dim[['RoleID', 'Description', 'Permission']] = new_dim[['RoleID', 'Description', 'Permission']].astype(str)
        dim_role.dimension_table = new_dim
        self.dimension_tables.append(dim_role)
        print(dim_role.dimension_table)
        
        #fetch staff
        dim_staff = DimStaff()
        new_dim = dim_staff.dimension_table
        new_dim['StaffID'] = new_dim['StaffID'].astype(int)
        print(new_dim.columns)
        new_dim[['RoleID', 'Password', 'FirstName', 'LastName', 'Phone', 'Email']] = new_dim[['RoleID', 'Password', 'FirstName', 'LastName', 'Phone', 'Email']].astype(str)
        dim_staff.dimension_table = new_dim
        self.dimension_tables.append(dim_staff)
        print(dim_staff.dimension_table)
        
        # #fetch request
        # dim_request = DimRequest()
        # new_dim = dim_request.dimension_table
        # new_dim['StaffID'] = new_dim['StaffID'].astype(int)
        # new_dim[['RoleID', 'Password', 'FirstName', 'LastName', 'Phone', 'Email']] = new_dim[['RoleID', 'Password', 'FirstName', 'LastName', 'Phone', 'Email']].astype(str)
        # dim_staff.dimension_table = new_dim
        # self.dimension_tables.append(dim_staff)
        # print(dim_staff.dimension_table)
        
        #fetch login_record
        dim_login_record = DimLoginRecord()
        new_dim = dim_login_record.dimension_table
        new_dim[['RecordID', 'StaffID']] = new_dim[['RecordID', 'StaffID']].astype(int)
        new_dim['IP'] = new_dim['IP'].astype(str)
        new_dim['LoginTime'] = pd.to_datetime(new_dim['LoginTime'],unit='s')
        new_dim.rename(inplace=True, columns={'RecordID':'LoginRecordID'})
        dim_login_record.dimension_table = new_dim.drop(columns='LoginTime')
        self.dimension_tables.append(dim_login_record)
        print(dim_login_record.dimension_table)
        
        #fetch failed_login_attempt
        dim_failed_login_attempt = DimFailedLoginAttempt()
        new_dim = dim_failed_login_attempt.dimension_table
        new_dim[['AttemptID', 'StaffID']] = new_dim[['AttemptID', 'StaffID']].astype(int)
        new_dim['IP'] = new_dim['IP'].astype(str)
        new_dim['AttemptTime'] = pd.to_datetime(new_dim['AttemptTime'],unit='s')
        new_dim.rename(inplace=True, columns={'AttemptID':'FailedLoginAttemptID'})
        dim_failed_login_attempt.dimension_table = new_dim
        self.dimension_tables.append(dim_failed_login_attempt)
        print(dim_failed_login_attempt.dimension_table)
        
        
    def load(self):
        for table in self.dimension_tables:
            table.load()
            
        print(f'Step 3 finished')
        
        
    def mainLoop(self):    
        # Step 1
        self.extract()
        # Step 2
        self.transform()
        # Step 3
        for table in ['DailyReportDim', 'TradeDim', 'DailyChartDim', 'ClientDim', 'ReasonDim', 'SymbolDim', 'LoginRecordDim', 'FailedLoginAttemptDim', 'StaffDim', 'RoleDim']:
            try:
                database.delete_sqldatabase(table)
                print(f"Table {table} deleted.")
            except:
                print(f"Table {table} not deleted.")
                pass
        self.load()
        
        
        
def main():
    # create an instance of MainETL
    main = MainETL()
    main.mainLoop()
    
if __name__ == '__main__':
    main()