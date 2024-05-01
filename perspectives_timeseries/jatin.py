# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from datetime import datetime
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


time_slice_length=15
time_slice_part='MINUTES'
source_table1='UGC.PERSPECTIVES.PROD_DASHBOARD_METERS'
source_table2='UGC.PERSPECTIVES.PROD_DASHBOARD_WELLS'
target_table = 'PROD_DASHBOARD.UGC_BI.LEGACY_PROD_METERS_WELLS'
audit="PROD_DASHBOARD.UGC_BI.LEGACY_METERS_WELL_AUDIT"
dest_action = 'append'

# Define the aggregating calculation column parameters
calc_defs=[
    {"OT":"Moving_diff","target_column":"TIME_DIFF_MINUTES","source_column":"TIMESTAMP" , "periods":'1',"type":'diff'},
    {"OT":"Moving_diff","target_column":"PROD_RATE"        ,"source_column":"FLOW_TODAY", "periods":'1',"type":'diff_rate'},
    {"OT":"SMA","source_column":"DIFFERENTIAL_PRESSURE","target_column": "DIFF_PRES_30MEAN", "interval": "30 minutes","min_periods": 1, "calc_type": "mean"},
    {"OT":"SMA","source_column":'DIFFERENTIAL_PRESSURE',"target_column": "DIFF_PRES_30STD",  "interval": "30 minutes","min_periods": 1, "calc_type": "std"},
    {"OT":"SMA","source_column":"PROD_RATE"            ,"target_column": "PROD_RATE_4H_MEAN", "interval":"4 hours",   "min_periods": 1, "calc_type": "mean"},
    {"OT":"SMA","source_column":"PROD_RATE"            ,"target_column": "PROD_RATE_12H_MEAN","interval":"12 hours",  "min_periods": 1, "calc_type": "mean"}
]


def main(session: snowpark.Session): 
    current_time = datetime.now().time()
    print("Current time1 Session start:", current_time)
    
    #Fetch start_date from AUDIT 
    if dest_action=='TRUNCATE':
        start_date_timestamp= datetime.strptime("2006-01-01 00:00:00.000",'%Y-%m-%d %H:%M:%S.%f')
    else:
        fetch_start_date_query = f"""
        SELECT END_DATE FROM {audit};
        """
        start_date_result = session.sql(fetch_start_date_query).to_pandas()
        start_date_timestamp = start_date_result['END_DATE'][0]
    
    # Convert the Timestamp object to a string representation
    start_date = start_date_timestamp
    start_timestamp = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    print(f"Start date from {audit}:", start_date)

     
    end_timestamp=start_date+ timedelta(days=30)
    end_date =end_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    #end_date = datetime.now()
    #end_timestamp = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    
    merged_df = merged_Data(session,source_table1,source_table2)
    
    pass_through_columns = ['METER_NAME', 'CONTAINER_ID','ROUTE','METERS_LAT','METERS_LONG','WELLS_LAT','WELLS_LONG','METER_FUNCTION','LOCATION','WELLS_NAME']

    # get columns with point IDs that we will need to get the history
    
    columns_with_point_ids = [column for column in merged_df.columns if column not in pass_through_columns]
    pd_points_df = merged_df[columns_with_point_ids]

    flattened_point_ids = [str(int(value)) if pd.notnull(value) else str(value) for column in pd_points_df.columns  for value in pd_points_df[column]]
    filtered_flattened_point_ids = [item for item in flattened_point_ids if item not in ['nan','None']]
    
    sql_query = f"""
    SELECT
        POINT_ID, 
        TIME_SLICE(CAPTURE_DATE, {time_slice_length}, '{time_slice_part}', 'START') AS "TIME_SLICE",
        AVG(value) AS "VALUE"
    FROM 
        UGC.HISTORY.HISTORY
    WHERE 
        POINT_ID IN ({', '.join(filtered_flattened_point_ids)}) AND
        CAPTURE_DATE > '{start_date}' AND
        CAPTURE_DATE <= '{end_date}'
    GROUP BY 
        POINT_ID, "TIME_SLICE"
    """
    current_time = datetime.now().time()
    print("End time for history:", current_time)
    # Show what SQL Injection can do to a composed statement.
    
    histories = session.sql(sql_query)
    histories.show()
    pd_histories = histories.to_pandas()

    # Remove the values Greater than 10k   `
    pd_histories.rename(columns={'TIME_SLICE':'TIMESTAMP'},inplace=True)
    pd_histories=pd_histories[pd_histories['VALUE']<10000]
    
    
    if dest_action=='append':
        pd_histories=pd_histories[pd_histories['TIMESTAMP']!=start_date]

    # Generate a series of timestamps at 15-minute intervals
    timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='15T')

    merged_columns=list(merged_df.columns)
    column_data_types = merged_df.dtypes.apply(lambda x: x.name).to_dict()
    
    # Get DDL of the table
    table_ddl=get_table_ddl(column_data_types,target_table)
    table_ddl=table_ddl.replace('TABLE','HYBRID TABLE')
    table_ddl=table_ddl.replace('CONTAINER_ID NUMBER (38,2)','CONTAINER_ID NUMBER(38,0)')
    table_ddl=table_ddl.replace(');',',\n TIMESTAMP TIMESTAMP_NTZ(9),\n PRIMARY KEY (CONTAINER_ID,TIMESTAMP))')

    table_exists = does_table_exist(session,target_table)
    match = False
    
    if table_exists:
        
        table_columns = get_table_columns(session,target_table)
        ddl_columns = parse_columns_from_ddl(table_ddl)
        ddl_columns.remove('PRIMARY')
        match = compare_ddl_with_definition(ddl_columns, table_columns)
        print("DDL matches table definition:", match)


        if not match:
            print('Columns dont match, canceling import')
            print('ddl columns',ddl_columns)
            print('table columns', table_columns)
            return 0
        
    if not table_exists:
        print('Adding PROD table :', target_table)
        result = session.sql(table_ddl)
        create_index=session.sql(f'CREATE INDEX IF NOT EXISTS PMW (CONTAINER_ID,TIMESTAMP) on {target_table}')
        result.show()
        create_index.collect()

    if dest_action=='TRUNCATE':
        Truncate_BI=session.sql(f"truncate table {target_table}")
        Truncate_BI.show()

    perspective_data_sf_df = session.sql(f"SELECT * FROM {target_table} where 1=0")
    perspective_data_pd_df = perspective_data_sf_df.to_pandas()


    
    #column names on which we unpivot the data
    id_bars=pass_through_columns
    
    #Unpivot the Merged sourced data
    merged_depivot_df = merged_df.melt(id_vars=id_bars, value_vars=columns_with_point_ids,var_name='OPERATION', value_name='POINT_ID')

    #join data with histories using POINT IDS
    histories_depivot=pd.merge(pd_histories,merged_depivot_df,how='left',on='POINT_ID')
    id_bars.append('TIMESTAMP')
    
    #Convert the data into original form by Pivoting it.
    pivoted_df = histories_depivot.pivot(index=id_bars, columns='OPERATION', values='VALUE').reset_index()

    # Create dataframe to contain timestamp values with column name in a given time interval
    merged_df_cate=merged_df.drop(columns=columns_with_point_ids)
    merged_df_cate = pd.concat([merged_df_cate.assign(timestamp=ts) for ts in timestamps], ignore_index=True)
    merged_df_cate.rename(columns={'timestamp':'TIMESTAMP'},inplace=True)
    
    if dest_action=='append':
        merged_df_cate=merged_df_cate[merged_df_cate['TIMESTAMP']!=start_date]

    #Merge both pivoted and Null values table
    perspective_data_pd_df=pd.merge(merged_df_cate,pivoted_df,how='left',on=id_bars)
    perspective_data_pd_df.sort_values(by=['CONTAINER_ID','TIMESTAMP'],ascending=True,inplace=True)
    
    
    # Append missing columns with Nan
    for column in columns_with_point_ids:
        if column not in list(perspective_data_pd_df.columns):
            perspective_data_pd_df[column]=None

    
    merged_columns.append('TIMESTAMP')
    perspective_data_pd_df=perspective_data_pd_df[merged_columns]

    #Carry Forward the Previous Values to replace Null Values
    perspective_data_pd_df=carry_foward(session,perspective_data_pd_df,columns_with_point_ids)
    
    #Call calc_column function  
    perspective_data_pd_df=create_calc_columns(session,perspective_data_pd_df,start_date)

    #Appending data into table
    session.create_dataframe(perspective_data_pd_df).write.save_as_table( table_name=f'{target_table}', mode='append')
    

    # Call perespective audit function
    perespective_audit(session, audit,target_table)

    return session.create_dataframe(perspective_data_pd_df)

def carry_foward(session,perspective_data_pd_df,columns_with_point_ids):
    
    if dest_action=='TRUNCATE':
        for column in columns_with_point_ids:
            perspective_data_pd_df[column] = perspective_data_pd_df.groupby('CONTAINER_ID')[column].fillna(method='ffill')
    else:
        sql_query=f"""SELECT * FROM (
        SELECT *,
         ROW_NUMBER() OVER(PARTITION BY CONTAINER_ID ORDER BY TIMESTAMP DESC ) as row_rank
        FROM 
        {target_table}) WHERE row_rank=1
        """
        # Execute the SQL query and fetch data into a DataFrame
        histories = session.sql(sql_query)
        bi_data_pd_df = histories.to_pandas()
        perspective_data_pd_df=pd.concat([perspective_data_pd_df,bi_data_pd_df],ignore_index=True)

        for column in columns_with_point_ids:
            perspective_data_pd_df[column] = perspective_data_pd_df.groupby('CONTAINER_ID')[column].fillna(method='ffill')

        perspective_data_pd_df=perspective_data_pd_df[perspective_data_pd_df['ROW_RANK'].isnull()]
        perspective_data_pd_df.drop(columns=['ROW_RANK'],inplace=True)
    return perspective_data_pd_df
    
def merged_Data(session,source_table1,source_table2):

    sql_query=f"""
        select
           m.container_id,
           m.container_name as meter_name,
           m.route,
           m.location,
           m.meter_function,
           m.lat as meters_lat,
           m.long as meters_long,
           m.differential_pressure,
           m.static_pressure,
           m.flow_rate,
           m.flow_today,
           m.flow_yesterday,
           w.lat as wells_lat,
           w.long as wells_long,
           w.name as wells_name,
           w.casing_pressure,
           w.tubing_pressure
        from {source_table1} m 
        left join {source_table2} w on m.well_id=w.container_id
        """
    perspective_update=session.sql(sql_query)
    perspective_update.show()
    perspective_df=perspective_update.to_pandas()
    return perspective_df
 
    
def get_table_ddl(column_data_types,target_table):
    table_query="""
    CREATE TABLE IF NOT EXISTS {}
    ( {});""".format(target_table,','.join('\n{} {}'.format(col_name, data_type) for col_name, data_type in column_data_types.items()))
    
    re_dict={'datetime64[ns]':'Timestamp_NTZ(9)','int32':'NUMBER (38,2)','object':'varchar','float64':'FLOAT'}
    for old,new in re_dict.items():
        table_query=table_query.replace(old,new)
    return table_query

def does_table_exist(session,table_name):
    # Check if the table exists in the schema
    table_name=table_name.split('.')[-1]
    result = session.sql(f"""SELECT 
    COUNT(*) AS table_exists
FROM 
    INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_NAME = '{table_name}'"""
).to_pandas()
    return result['TABLE_EXISTS'][0] > 0

def get_table_columns(session,table_name):
    cols_list = session.sql(f"DESCRIBE TABLE {table_name}").collect()
    column_names = [col.name for col in cols_list]
    return column_names

def parse_columns_from_ddl(ddl):
    # Split the DDL statement by lines
    ddl_lines = ddl.split('\n')
    print(ddl_lines)
    # Parse column names from each line
    column_names = []
    for line in ddl_lines:
        # Skip lines that don't contain column definitions
        if '(' in line and ')' in line:
            # Extract column name by splitting at the space
            column_name = line.split()[0]
            # Remove leading comma and tab characters, if any
            column_name = column_name.lstrip(',\t')
            # Add column name to the list
            column_names.append(column_name)
    return column_names

def compare_ddl_with_definition(ddl_columns, columns):
    return all(item in columns for item in ddl_columns)

def table_contains_column(session,table_name, column_name):
    table_columns = get_table_columns(session,table_name)
    return column_name in table_columns
def text_to_duration(text):
    parts = text.split()
    if len(parts) != 2:
        raise ValueError("Invalid input format. Use '<number> <unit>' format, e.g., '15 Minutes'")
    
    num, unit = parts
    num = int(num)
    
    if unit.lower() in ['minute', 'minutes']:
        return timedelta(minutes=num)
    elif unit.lower() in ['hour', 'hours']:
        return timedelta(hours=num)
    elif unit.lower() in ['day', 'days']:
        return timedelta(days=num)
    elif unit.lower() in ['week', 'weeks']:
        return timedelta(weeks=num)
    else:
        raise ValueError("Invalid unit. Supported units are: minute(s), hour(s), day(s), week(s)")

def duration_to_abrv_text(duration, unit):
    total_seconds = duration.total_seconds()
    
    if unit.lower() in ['minute', 'minutes']:
        if total_seconds % 3600 != 0:  # If less than an hour, represent in minutes
            value = total_seconds / 60
            return f"{value:.0f}M"
        elif total_seconds % 3600 == 0:  # If exactly an hour, represent as "1 hour"
            return "1H"
    elif unit.lower() in ['hour', 'hours']:
        value = total_seconds / 3600
        return f"{value:.1f}H"
    elif unit.lower() in ['day', 'days']:
        value = total_seconds / 86400
        return f"{value:.1f}D"
    elif unit.lower() in ['week', 'weeks']:
        value = total_seconds / 604800
        return f"{value:.1f}W"
    else:
        raise ValueError("Invalid unit. Supported units are: minute(s), hour(s), day(s), week(s)")

def create_calc_columns(session,perspective_data_pd_df,start_timestamp):

    for calc in calc_defs:
        target_column_name =calc['target_column']
        source_column_name=calc['source_column']
        
        if not table_contains_column(session,target_table,target_column_name):
        #Alter the Snowflake table to add the SMA column
            print('Adding column for calc named:', target_column_name)
            alter_sql = f"ALTER TABLE {target_table} ADD COLUMN {target_column_name} NUMBER(38,2);"
            result = session.sql(alter_sql)
            result.show()
        
        #add column if needed 
        if calc['OT']=='Moving_diff':
            perspective_data_pd_df_null=perspective_data_pd_df[perspective_data_pd_df[source_column_name].isnull()]
            perspective_data_pd_df_notnull=perspective_data_pd_df[perspective_data_pd_df[source_column_name].notnull()]
            perspective_data_pd_df_notnull=moving_diff(session,perspective_data_pd_df_notnull,source_column_name,target_column_name,start_timestamp,calc['type'])
            perspective_data_pd_df=pd.concat([perspective_data_pd_df_null,perspective_data_pd_df_notnull],ignore_index=True)
            
        elif calc['OT']=='SMA':
            perspective_data_pd_df=SMA(session,perspective_data_pd_df,source_column_name,target_column_name,calc,start_timestamp)
    
    return perspective_data_pd_df 

def moving_diff(session,perspective_data_pd_df_notnull,source_column_name,target_column_name,start_timestamp,type):
    
    if dest_action=='append':
        sql_query = f"""
        SELECT *
           FROM (
        SELECT *,
         ROW_NUMBER() OVER(PARTITION BY CONTAINER_ID ORDER BY TIMESTAMP DESC ) as row_rank
        FROM 
        {target_table}
        WHERE 
         TIMESTAMP <'{start_timestamp}' ) 
        WHERE row_rank=1
        """
    
        # Execute the SQL query and fetch data into a DataFrame
        histories = session.sql(sql_query)
        bi_data_pd_df = histories.to_pandas()

        perspective_data_pd_df_notnull=pd.concat([perspective_data_pd_df_notnull,bi_data_pd_df],ignore_index=True)
        if type=='diff':
            perspective_data_pd_df_notnull[target_column_name]=perspective_data_pd_df_notnull.groupby(['CONTAINER_ID'])[source_column_name].diff().dt.total_seconds()/60
        elif type=='diff_rate':
            perspective_data_pd_df_notnull[target_column_name]=perspective_data_pd_df_notnull.groupby(['CONTAINER_ID'])[source_column_name].diff()
            perspective_data_pd_df_notnull[target_column_name] = perspective_data_pd_df_notnull[[target_column_name,'TIME_DIFF_MINUTES']].apply(lambda x: x[target_column_name] / x['TIME_DIFF_MINUTES'] if x[target_column_name] < 0 else x[target_column_name], axis=1)
           
        perspective_data_pd_df_notnull=perspective_data_pd_df_notnull[perspective_data_pd_df_notnull['ROW_RANK'].isnull()]
        perspective_data_pd_df_notnull.drop(columns=['ROW_RANK'],inplace=True)
        
    
    else :
        if type=='diff':
            perspective_data_pd_df_notnull[target_column_name]=perspective_data_pd_df_notnull.groupby(['CONTAINER_ID'])[source_column_name].diff().dt.total_seconds()/60
        elif type=='diff_rate':
            perspective_data_pd_df_notnull[target_column_name]=perspective_data_pd_df_notnull.groupby(['CONTAINER_ID'])[source_column_name].diff()
            perspective_data_pd_df_notnull[target_column_name] = perspective_data_pd_df_notnull[[target_column_name,'TIME_DIFF_MINUTES']].apply(lambda x: x[target_column_name] / x['TIME_DIFF_MINUTES'] if x[target_column_name] < 0 else x[target_column_name], axis=1)
    return perspective_data_pd_df_notnull
                   
def SMA(session,perspective_data_pd_df,source_column_name,target_column_name,calc,start_timestamp):
    
    calc_duration = text_to_duration(calc['interval'])
    table_duration = text_to_duration(f"{time_slice_length} {time_slice_part}")

    min_periods = calc['min_periods']
    window = int(calc_duration / table_duration)
    adjusted_start_date = start_timestamp - calc_duration

    if dest_action=='append':

        sql_query = f"""
        SELECT *,
        1 as DEFAULT
        FROM 
        {target_table}
        WHERE 
        {source_column_name} IS NOT NULL AND
         TIMESTAMP >='{adjusted_start_date}' 
         AND TIMESTAMP <'{start_timestamp}'
        """
        # Execute the SQL query and fetch data into a DataFrame
        histories = session.sql(sql_query)
        bi_data_pd_df = histories.to_pandas()
        perspective_data_pd_df=pd.concat([perspective_data_pd_df,bi_data_pd_df],ignore_index=True)
        perspective_data_pd_df[target_column_name] = perspective_data_pd_df.groupby(['CONTAINER_ID'])[source_column_name].rolling(window=window, min_periods=min_periods).apply(lambda x: getattr(x, calc['calc_type'])()).sort_index(level=1).reset_index(drop=True)
        perspective_data_pd_df=perspective_data_pd_df[perspective_data_pd_df['DEFAULT'].isnull()]
        perspective_data_pd_df.drop(columns=['DEFAULT'],inplace=True)
        
    
    else:
        perspective_data_pd_df[target_column_name] = perspective_data_pd_df.groupby(['CONTAINER_ID'])[source_column_name].rolling(window=window, min_periods=min_periods).apply(lambda x: getattr(x, calc['calc_type'])()).sort_index(level=1).reset_index(drop=True)
        
    return perspective_data_pd_df


def perespective_audit(session, audit , target_table):
    """
    Updates start and end dates in AUDIT table, fetches start and end dates from target table, and updates values in AUDIT table.

    Parameters:
    - session: Snowpark session object.
    - perspective_name: Name of the perspective table.
    """

    # Create AUDIT table if it doesn't exist
    audit_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {audit} (
        START_DATE TIMESTAMP_NTZ(9),
        END_DATE TIMESTAMP_NTZ(9)
    );
    """
    #session.use_schema('BI')
    result = session.sql(audit_table_ddl)
    result.show()

    # Fetch start and end dates from the target table
    start_date_query = f"""
    SELECT MIN(TIMESTAMP) AS START_DATE FROM {target_table};
    """
    end_date_query = f"""
    SELECT MAX(TIMESTAMP) AS END_DATE FROM {target_table};
    """

    start_date_result = session.sql(start_date_query).to_pandas()
    end_date_result = session.sql(end_date_query).to_pandas()

    start_date_value = start_date_result['START_DATE'][0]
    end_date_value = end_date_result['END_DATE'][0]

    # Check if there are existing records in AUDIT table
    check_records_query = f"""
    SELECT COUNT(*) AS RECORDS_COUNT FROM {audit};
    """
    records_count_result = session.sql(check_records_query).to_pandas()
    records_count = records_count_result['RECORDS_COUNT'][0]

    if records_count == 0:
        # If no records exist, insert values into AUDIT table
        insert_query = f"""
        INSERT INTO {audit} (START_DATE, END_DATE) VALUES ('{start_date_value}', '{end_date_value}');
        """
        result = session.sql(insert_query)
        result.show()
    else:
        # If records exist, update values in AUDIT table
        update_query = f"""
        UPDATE {audit} SET START_DATE = '{start_date_value}', END_DATE = '{end_date_value}';
        """
        result = session.sql(update_query)
        result.show()
