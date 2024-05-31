from datasetup import *
import pandas as pd
from pathlib import Path

database=AzureDB()
database.access_container("csv-files")
login_df = database.access_blob_csv(blob_name='login.csv')
symbol_df = database.access_blob_csv(blob_name='symbol.csv')
reason_df = database.access_blob_csv(blob_name='reason.csv')
AUDUSD = database.access_blob_csv(blob_name='daily_chart/AUDUSD.csv')
EURUSD = database.access_blob_csv(blob_name='daily_chart/EURUSD.csv')
GBPUSD = database.access_blob_csv(blob_name='daily_chart/GBPUSD.csv')
NZDUSD = database.access_blob_csv(blob_name='daily_chart/NZDUSD.csv')
USDCAD = database.access_blob_csv(blob_name='daily_chart/USDCAD.csv')
USDCHF = database.access_blob_csv(blob_name='daily_chart/USDCHF.csv')
USDCNH = database.access_blob_csv(blob_name='daily_chart/USDCNH.csv')
USDHKD = database.access_blob_csv(blob_name='daily_chart/USDHKD.csv')
USDHUF = database.access_blob_csv(blob_name='daily_chart/USDHUF.csv')
USDJPY = database.access_blob_csv(blob_name='daily_chart/USDJPY.csv')
USDMXN = database.access_blob_csv(blob_name='daily_chart/USDMXN.csv')
USDNOK = database.access_blob_csv(blob_name='daily_chart/USDNOK.csv')
USDPLN = database.access_blob_csv(blob_name='daily_chart/USDPLN.csv')
USDSGD = database.access_blob_csv(blob_name='daily_chart/USDSGD.csv')
USDTHB = database.access_blob_csv(blob_name='daily_chart/USDTHB.csv')
USDTRY = database.access_blob_csv(blob_name='daily_chart/USDTRY.csv')
USDZAR = database.access_blob_csv(blob_name='daily_chart/USDZAR.csv')
USDSEK = database.access_blob_csv(blob_name='daily_chart/USDSEK.csv')
charts = {'AUDUSD':AUDUSD, 'EURUSD':EURUSD, 'GBPUSD':GBPUSD, 'NZDUSD':NZDUSD, 'USDCAD':USDCAD, 'USDCHF':USDCHF, 'USDCNH':USDCNH,
          'USDHKD':USDHKD, 'USDHUF':USDHUF, 'USDJPY':USDJPY, 'USDMXN':USDMXN, 'USDNOK':USDNOK, 'USDPLN':USDPLN, 'USDSEK':USDSEK,
          'USDSGD':USDSGD, 'USDTHB':USDTHB, 'USDTRY':USDTRY, 'USDZAR':USDZAR}
for name, chart in charts.items():
    chart['CurrencyID'] = name
daily_chart_df = pd.concat(charts.values())
trades_df = database.access_blob_csv(blob_name='trades.csv')
daily_report_df = database.access_blob_csv(blob_name='daily_report.csv')

staff_df = database.access_blob_csv(blob_name='staff.csv')
role_df = database.access_blob_csv(blob_name='role.csv')
request_df = database.access_blob_csv(blob_name='request.csv')
login_record_df = database.access_blob_csv(blob_name='login_record.csv')
failed_login_attempt_df = database.access_blob_csv(blob_name='failed_login_attempt.csv')

class ModelAbstract():
    def __init__(self):
        self.columns = None
        self.dimension_table = None
        
    def dimension_generator(self, frame, name:str, columns:list, pk=False):
        df = frame
        dim = df[columns]
        dim = dim.drop_duplicates()
        # Creating primary key for dimension table
        if pk == False:
            dim[f'{name}ID'] = range(1, len(dim) + 1)

        self.dimension_table = dim
        self.name = name
        self.columns = columns
        
    def load(self):
        if self.dimension_table is not None:
            # Upload dimension table to data warehouse
            database.upload_dataframe_sqldatabase(f'{self.name}Dim', blob_data=self.dimension_table)
        
            # Saving dimension table as separate file
            self.dimension_table.to_csv(f'./data/{self.name}Dim.csv')
        else:
            print("Please create a dimension table first using dimension_generator") 
        
class DimClient(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(login_df, 'Client', login_df.columns, True)
        
class DimSymbol(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(symbol_df, 'Symbol', symbol_df.columns, True)
        
class DimReason(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(reason_df, 'Reason', reason_df.columns, True)
        
class DimDailyChart(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(daily_chart_df, 'DailyChart', daily_chart_df.columns)

class DimTrades(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(trades_df, 'Trade', trades_df.columns, True)
        
class DimDailyReport(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(daily_report_df, 'DailyReport', daily_report_df.columns)
        
class DimStaff(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(staff_df, 'Staff', staff_df.columns, True)
        
class DimRole(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(role_df, 'Role', role_df.columns, True)
        
class DimRequest(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(request_df, 'Request', request_df.columns)
        
class DimLoginRecord(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(login_record_df, 'LoginRecord', login_record_df.columns, True)
        
class DimFailedLoginAttempt(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator(failed_login_attempt_df, 'FailedLoginAttempt', failed_login_attempt_df.columns, True)