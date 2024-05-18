from sqlalchemy import create_engine, text

# Establish the database connection
engine = create_engine('your_database_connection_string')

# Get a list of tables in the database
with engine.connect() as con:
    tables = con.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='dbo'")

# Loop through each table
for table in tables:
    blob_name = table[0]  # Extract table name
    primary = f'{blob_name}ID'  # Assuming primary key follows the convention TableID

    # Alter table to set primary key if not already set
    with engine.connect() as con:
        trans = con.begin()
        con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);'))
        trans.commit()

    # Identify and set foreign keys for columns ending with "ID"
    with engine.connect() as con:
        trans = con.begin()
        # Assuming the convention that foreign key columns end with "ID"
        foreign_keys = con.execute(text(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{blob_name}' AND COLUMN_NAME LIKE '%ID' AND COLUMN_NAME != '{primary}'"))
        for foreign_key in foreign_keys:
            foreign_key_column = foreign_key[0]
            constraint_name = f'FK_{blob_name}_{foreign_key_column}'
            referenced_table = foreign_key_column[:-2]  # Removing 'ID' to get referenced table name
            con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [{constraint_name}] FOREIGN KEY ({foreign_key_column}) REFERENCES [{referenced_table}]([{foreign_key_column}]);'))
        trans.commit()




def upload_dataframe_sqldatabase(self, blob_name, blob_data):
    print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
    blob_data.to_sql(blob_name, engine, if_exists='replace', index=False)
    primary = blob_name.replace('Dim', 'ID')
    
    # Identify and set foreign keys for columns ending with "ID"
    with engine.connect() as con:
        trans = con.begin()
        foreign_keys = con.execute(text(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{blob_name}' AND COLUMN_NAME LIKE '%ID' AND COLUMN_NAME != '{primary}'"))
        for foreign_key in foreign_keys:
            foreign_key_column = foreign_key[0]
            constraint_name = f'FK_{blob_name}_{foreign_key_column}'
            referenced_table = foreign_key_column[:-2]  # Removing 'ID' to get referenced table name
            con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [{constraint_name}] FOREIGN KEY ({foreign_key_column}) REFERENCES [{referenced_table}]([{foreign_key_column}]);'))
        trans.commit()
    
    # Set primary key
    if 'fact' in blob_name.lower():
        with engine.connect() as con:
            trans = con.begin()
            con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] alter column {blob_name}ID bigint NOT NULL'))
            con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{blob_name}ID] ASC);'))
            trans.commit() 
    else:
        try:        
            with engine.connect() as con:
                trans = con.begin()
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] alter column {primary} bigint NOT NULL'))
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);'))
                trans.commit() 
        except:
            with engine.connect() as con:
                trans = con.begin()
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] alter column {primary} varchar(255) NOT NULL'))
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);'))
                trans.commit()
