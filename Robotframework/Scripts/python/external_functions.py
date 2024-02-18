# Databricks notebook source
# This is the bases for how an adls path is defined in the QA part
# display(dbutils.fs.ls('abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/{Folder_path}'))
# display(dbutils.fs.ls('abfss://wdsm@edaaaedle1devsrz.dfs.core.windows.net/WDSM/security_v2_3_2/'))
# display(dbutils.fs.ls('abfss://wcz0003@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0003/pear/pear_equity_common_stocks/'))

# ----------

import sys
import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DecimalType
from pyspark.sql.functions import col, coalesce, lit, when, isnull, min, abs

# ----------

# Uncoment below 2 lines for actual python scripts in dbfs
app_name = "external_functions"
spark = SparkSession.builder.appName(app_name).getOrCreate()
dbutilspkg = DBUtils(spark)

# ----------

def create_pass_fail_df(pass_fail_df, tgt_df_column, missing_column=[], ignore_column=[], threshold_col=[''], threshold_val_perc=0.002):
    """creating all the pass_fail columns(status_x and x_null) and also rearanging them(x -> qa_x -> status_x -> x_null)"""
    columns_ordered = []
    # print(f'existing columns:\n{pass_fail_df.columns}')

    for column in tgt_df_column:
        if column in threshold_col and threshold_col != ['']:
            threshold_lower = 1 - (threshold_val_perc/100)
            threshold_upper = 1 + (threshold_val_perc/100)
            pass_fail_df = pass_fail_df.withColumn(f'status_{column}', when(abs(coalesce(col(f'{column}').cast(DecimalType(31,6)), lit(0)))\
                                                                            .between((abs(coalesce(col(f'qa_{column}').cast(DecimalType(31,6)), lit(0)))*threshold_lower),
                                                                                     (abs(coalesce(col(f'qa_{column}').cast(DecimalType(31,6)), lit(0)))*threshold_upper)), 'PASS').otherwise('FAIL')) \
                                        .withColumn(f'{column}_null', when(isnull(col(f'{column}')), 1).otherwise(0))
        else:
            pass_fail_df = pass_fail_df.withColumn(f'status_{column}', when(coalesce(col(f'{column}'),lit('')) == coalesce(col(f'qa_{column}'),lit('')), 'PASS').otherwise('FAIL'))\
                                        .withColumn(f'{column}_null', when(isnull(col(f'{column}')), 1).otherwise(0))
        columns_ordered = columns_ordered + [f'{column}', f'qa_{column}', f'status_{column}', f'{column}_null']
    #     print(f'column added:status_{column}, {column}_null')

    # print(f'columns_ordered:\n{columns_ordered}')
    columns_ordered = columns_ordered + ignore_column + missing_column
    pass_fail_df = pass_fail_df.select([col for col in columns_ordered])
    pass_fail_df.createOrReplaceTempView('temp_pass_fail_final')
    return pass_fail_df

# ----------

# def create_result_summary_df(tgt_df_column, null_flag={}):
def create_result_summary_df(tgt_df_column, non_nullable_col=''):
    """creating entire result_summary_df by summarising the passes, fails and nulls in each column"""
    non_nullable_list = non_nullable_col.split(',')
    i=0
    for column in tgt_df_column:
        i+=1
        # element_null_flag = null_flag.get(column,'Y')
        element_null_flag = 'Y'
        if column in non_nullable_list:
            element_null_flag = 'N'            
        result_summary_temp_df = spark.sql(f"""
                                            select '{column}' as column_name, 
                                                '{element_null_flag}' as nullable, 
                                                SUM(CASE WHEN status_{column} = 'PASS' THEN 1 ELSE 0 END) as pass_count, 
                                                SUM(CASE WHEN status_{column} = 'FAIL' THEN 1 ELSE 0 END ) as fail_count , 
                                                Sum({column}_null) as null_count  
                                            from temp_pass_fail_final
                                        """)
        if i == 1:
            result_summary_df = result_summary_temp_df.alias('result_summary_df')
        else:
            result_summary_df = result_summary_df.unionAll(result_summary_temp_df)

    result_summary_df.createOrReplaceTempView('temp_result_summary_final')
    return result_summary_df

# ----------

# Get Source DF from ADLS files
def create_source_df_from_adls(container_name, storage_acc_name, folder_path, table_name, podium_delivery_date='MAX'):

    error_no = 0
    error_discription = dict()
    adls_base_folder = f'abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/{folder_path}/{table_name}'
    # adls_base_folder = f'abfss://{db_name}@{src_base_path}/{table_name}'
    adls_current_folder = f'{adls_base_folder}/current/'
    adls_history_folder = f'{adls_base_folder}/history/'
    
    try:
        df_cur = spark.read.format('delta').load(adls_current_folder)
        if 'ifw_effective_end_date' not in df_cur.columns:
            df_cur_final = df_cur.withColumn('ifw_effective_end_date', lit('')) \
                    .withColumn('ifw_transaction_type', lit(''))
        else:
            df_cur_final=df_cur.alias('df_cur_final')
        df_cur_final.createOrReplaceTempView('qa_current_vw')
    except:
        print(f'Error:-NO Current folder found: {adls_current_folder}')
        error_discription['error_current_folder'] = f'Error:-NO Current folder found: {adls_current_folder}'
        error_no+=1

    try:
        df_hst = spark.read.format('delta').load(adls_history_folder)
        if 'ifw_transaction_type' not in df_hst.columns:
            df_hst = df_hst.withColumn('ifw_transaction_type', lit(''))
        df_hst = df_hst.select(df_cur_final.columns)
        df_hst.createOrReplaceTempView('qa_history_vw')
    except:
        print(f'Error:-NO History folder found: {adls_history_folder}')
        error_discription['error_history_folder'] = f'Error:-NO History folder found: {adls_history_folder}'
        error_no+=1
        
    if (error_no == 1) and ('error_history_folder' in error_discription.keys()):
        print('Since only the history folder is emty. Initiating it with an empty dataframe')
        df_hst = df_cur_final.alias('df_hst').filter(lit('2') == lit('1'))
        df_hst.createOrReplaceTempView('qa_history_vw')
    elif error_no > 0:
        dbutilspkg.notebook.exit(f'There are some issues with the parameters passed. Please check the below Error message {error_discription}')
        # raise Exception(f'There are some issues with the parameters passed. Please check the below Error message {error_discription}')

# ----------

# Get Source DF from Synapse Views
def get_view_selectable_columns(database_name, table_name, srzJdbcUrl, connectionProperties):

    sql_query = f'''(
            SELECT schema_name(o.schema_id) as schema_name,o.name as table_name,c.name as column_name,pr.name as user_name,p.*
            FROM sys.database_permissions p
            JOIN sys.objects o on o.object_id=p.major_id
            JOIN sys.columns c on c.object_id=o.object_id AND c.column_id=p.minor_id
            JOIN sys.database_principals pr on pr.principal_id=p.grantee_principal_id
            WHERE 
            schema_name(o.schema_id)='{database_name}' and o.name = '{table_name}'
            )t'''
    schema_df = spark.read.jdbc(url=srzJdbcUrl,table=sql_query,properties=connectionProperties)
    # display(schema_df)
    schema_lst = schema_df.select('column_name').collect()
    # print(schema_lst)
    table_cols = [elem.column_name for elem in schema_lst]
    # print(table_cols)
    return table_cols

def generate_synapse_connection_property(synapse_conn):

    #set up connection 
    # srzJdbcUrl = "jdbc:sqlserver://p3001-eastus2-asql-3.database.windows.net:1433;database=eda-akora2-aaedl-srzpoolprd;loginTimeout=10;"
    srzJdbcUrl = f"jdbc:sqlserver://{synapse_conn['server']}:1433;database={synapse_conn['database']};loginTimeout=10;"

    # Get jdbc credentials directly from ADB scope
    jdbcUsername = dbutilspkg.secrets.get(scope = synapse_conn['scope'], key = synapse_conn['username'])
    jdbcPassword = dbutilspkg.secrets.get(scope = synapse_conn['scope'], key = synapse_conn['password'])
    
    connectionProperties = {
    "AADSecurePrincipalId" : jdbcUsername,
    "AADSecurePrincipalSecret" : jdbcPassword,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication": "ActiveDirectoryServicePrincipal" ,
    "fetchsize":"10"
    }
    
    return srzJdbcUrl, connectionProperties

def create_source_df_from_view(synapse_conn, database_name, table_name, podium_delivery_date='MAX'):

    srzJdbcUrl, connectionProperties = generate_synapse_connection_property(synapse_conn)

    error_no = 0
    error_discription = dict()
    view_current_name = f'{table_name}_view'
    view_history_name = f'{table_name}_history_view'
    
    try:
        cols_lst_cur = get_view_selectable_columns(database_name, view_current_name, srzJdbcUrl, connectionProperties)
        querry_cur = f"""(select {','.join(cols_lst_cur)} from {database_name}.{view_current_name})t"""
        # print(querry_cur)
        df_cur = spark.read.jdbc(url=srzJdbcUrl,table=querry_cur,properties=connectionProperties)
        if 'ifw_effective_end_date' not in df_cur.columns:
            df_cur_final = df_cur.withColumn('ifw_effective_end_date', lit('')) \
                    .withColumn('ifw_transaction_type', lit(''))
        else:
            df_cur_final=df_cur.alias('df_cur_final')
        df_cur_final.createOrReplaceTempView('qa_current_vw')
    except:
        print(f'Error:-NO Current view found: {database_name}.{view_current_name}')
        error_discription['error_current_folder'] = f'Error:-NO Current view found: {database_name}.{view_current_name}'
        error_no+=1

    try:
        cols_lst_hst = get_view_selectable_columns(database_name, view_history_name, srzJdbcUrl, connectionProperties)
        querry_hst = f"""(select {','.join(cols_lst_hst)} from {database_name}.{view_history_name})t"""
        # print(querry_hst)
        df_hst = spark.read.jdbc(url=srzJdbcUrl,table=querry_hst,properties=connectionProperties)
        # if 'ifw_transaction_type' not in df_hst.columns:
        #     df_hst = df_hst.withColumn('ifw_transaction_type', lit(''))
        if (len(cols_lst_hst) == 2) & (set(cols_lst_hst).difference({'ifw_effective_end_date', 'ifw_transaction_type'}) == set()):
            raise ValueError
        df_hst = df_hst.select(df_cur_final.columns)
        df_hst.createOrReplaceTempView('qa_history_vw')
    except ValueError:
        print(f'Error:-History view foundonly has 2 columns: {database_name}.{view_history_name}')
        error_discription['error_history_view'] = f'Error:-History view foundonly has 2 columns: {database_name}.{view_history_name}'
        error_no+=1
    except:
        print(f'Error:-NO History view found: {database_name}.{view_history_name}')
        error_discription['error_history_view'] = f'Error:-NO History view found: {database_name}.{view_history_name}'
        error_no+=1
        
    if (error_no == 1) and ('error_history_view' in error_discription.keys()):
        print('Since only the history folder is emty. Initiating it with an empty dataframe')
        df_hst = df_cur_final.alias('df_hst').filter(lit('2') == lit('1'))
        df_hst.createOrReplaceTempView('qa_history_vw')
    elif error_no > 0:
        dbutilspkg.notebook.exit(f'There are some issues with the parameters passed. Please check the below Error message {error_discription}')
        # raise Exception(f'There are some issues with the parameters passed. Please check the below Error message {error_discription}')

# ----------

# Create combined view from the current and historic partition
def create_temp_combined_view(container_name='', storage_acc_name='', folder_path='', table_name='',
                              podium_delivery_date='LATEST', database_name='', synapse_conn={}, source='adls',
                              count_flag='Y'):

    if source == 'view':
        create_source_df_from_view(synapse_conn, database_name, table_name, podium_delivery_date)
    else:
        create_source_df_from_adls(container_name, storage_acc_name, folder_path, table_name, podium_delivery_date)
        
    # print(f'current:{df_cur_final.columns}\nhistory:{df_hst.columns}')        
    df_ful_allpart = spark.sql("""
                        select * from qa_current_vw
                        union all
                        select * from qa_history_vw
                        """)
    df_ful_allpart.createOrReplaceTempView(f'qa_{table_name}_full_allpart_vw')
    
    if podium_delivery_date.upper() == 'LATEST':
        # Only filtering the latest(current) partition date
        df_ful = spark.sql(f"""
                            select * from qa_current_vw
                        """)
    elif podium_delivery_date.upper() == 'MAX':
        # Only filtering the max partition date
        df_ful = spark.sql(f"""
                            select * from qa_{table_name}_full_allpart_vw
                            where ifw_effective_date in (select max(ifw_effective_date) from qa_{table_name}_full_allpart_vw)
                            """)
    elif podium_delivery_date.upper() == 'FULL':
        # returning full partion as it is
        df_ful = df_ful_allpart
    else:
        # Filtering the partition date required
        df_ful = spark.sql(f"""
                            select * from qa_{table_name}_full_allpart_vw
                            where ifw_effective_date <= '{podium_delivery_date}' 
                                and (ifw_effective_end_date = '' or ifw_effective_end_date > '{podium_delivery_date}')
                            """)
    # Uncoment below 2 statement to see all the partitions
    # print('All Available partitions')
    # display(df_ful_allpart.groupBy('ifw_effective_date','ifw_effective_end_date').count().orderBy('ifw_effective_date','ifw_effective_end_date'))
    
    df_ful.createOrReplaceTempView(f'qa_{table_name}_full_vw')
    # df_ful_final.write.mode("overwrite").saveAsTable(f'qa_{table_name}_full_vw')
    
    final_view_name = f'qa_{table_name}_full_vw'
    print(f'View created:{final_view_name}')
    if count_flag == 'Y':
        spark.sql(f"""select ifw_effective_date, ifw_effective_end_date, count(*) as count 
                        from {final_view_name}
                        group by ifw_effective_date, ifw_effective_end_date
                        order by ifw_effective_date desc, ifw_effective_end_date desc""").show()
    return df_ful   #final_view_name

# ----------

# Create view for cz tables
def create_temp_direct_view(container_name='', storage_acc_name='', folder_path='', table_name='',
                        filter={'process_date':'LATEST'}, database_name='', synapse_conn={}, count_flag='Y', source='adls'):

    # not using at this point as I dont remember how AZ works for cz data at this point
    # if source == 'view':
    #     create_source_df_from_view(database_name, table_name, podium_delivery_date)
    # else:
    #     create_source_df_from_adls(container_name, storage_acc_name, folder_path, table_name, podium_delivery_date)

    if source == 'view':
        srzJdbcUrl, connectionProperties = generate_synapse_connection_property(synapse_conn)
        cols_lst_cur = get_view_selectable_columns(database_name, table_name, srzJdbcUrl, connectionProperties)
        querry_cur = f"""(select {','.join(cols_lst_cur)} from {database_name}.{table_name})t"""
        # print(querry_cur)
        df_ful_allpart = spark.read.jdbc(url=srzJdbcUrl,table=querry_cur,properties=connectionProperties)
    elif database_name != '':
        df_ful_allpart = spark.sql(f"select * from {database_name}.{table_name}")
    else:
        df_ful_allpart = spark.sql(f"select * from delta.`abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/{folder_path}/{table_name}`")
    df_ful_allpart.createOrReplaceTempView(f'qa_{table_name}_full_allpart_vw')
    # print(f'df_ful_allpart:{df_ful_allpart.columns}')

    # the filter dict should only have 1 element, and getting the dict key out for easier use
    if len(filter) != 1:
        raise Exception(f"The filter({filter}) parameter should only be a single element dictionary.")
    filter_key = list(filter.keys())[0]

    # check if the key exists in the DF, if not then creating an empty value for that
    if filter_key not in df_ful_allpart.columns:
        print(f"{filter_key} dosent exist in the DF so adding the column with 'FULL' as the default.\nAlso will be returning the full DF, with no date filter applied")
        df_ful_allpart = df_ful_allpart.withColumn(filter_key, lit('FULL'))
        # changing the data filter to return full DF
        filter[filter_key] = 'FULL'

    if filter[filter_key].upper() == 'LATEST':
        # Only filtering the latest partition date
        df_ful = spark.sql(f"""
                            select * from qa_{table_name}_full_allpart_vw
                            where {filter_key} in (select max({filter_key}) from qa_{table_name}_full_allpart_vw)
                        """)
    elif filter[filter_key].upper() == 'FULL':
        # returning full partion as it is
        df_ful = df_ful_allpart
    elif ('<=' in filter[filter_key]) or ('>=' in filter[filter_key]):
        # date_val = filter[filter_key].replace('<=', '').strip()
        print(f"where {filter_key} {filter[filter_key]}")
        df_ful = spark.sql(f"""
                            select * from qa_{table_name}_full_allpart_vw
                            where {filter_key} {filter[filter_key]}
                        """)
    else:
        # Filtering the partition date required
        df_ful = spark.sql(f"""
                            select * from qa_{table_name}_full_allpart_vw
                            where {filter_key} = '{filter[filter_key]}'
                            """)
    # Uncoment below 2 statement to see all the partitions
    # print('All Available partitions')
    # display(df_ful_allpart.groupBy('ifw_effective_date','ifw_effective_end_date').count().orderBy('ifw_effective_date','ifw_effective_end_date'))

    df_ful.createOrReplaceTempView(f'qa_{table_name}_full_vw')
    # df_ful_final.write.mode("overwrite").saveAsTable(f'qa_{table_name}_full_vw')

    final_view_name = f'qa_{table_name}_full_vw'
    print(f'View created:{final_view_name}')
    if count_flag == 'Y':
        spark.sql(f"""select {filter_key}, count(*) as count 
                        from {final_view_name}
                        group by {filter_key}
                        order by {filter_key} desc""").show()
    return df_ful   #final_view_name

# ----------

def current_user_info():
    name_mapping = {'karthik':'kar', 'jeremy':'jer', 'hita':'hit', 'vaishnaviben':'vai', 'srutha':'sru', 'sridevi':'sri', 'reshma':'res', 'shitalben':'shi', 'michael':'mic', 'asha':'ash'}
    user_name = dbutilspkg.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
    user_prefix = name_mapping.get(user_name.split('.')[0])
    print(f'\nuser_name:{user_name}, user_prefix:{user_prefix}')
    return user_name, user_prefix

# ----------

def get_required_dbfs_folder_structure(dbfs_folder_base_path):
    # dbfs_folder_base_path = '/dbfs/FileStore/tables/DaaSWealth_QA/Main'
    folder_path_config = f'{dbfs_folder_base_path}/Config'
    folder_path_logs = f'{dbfs_folder_base_path}/Logs'
    folder_path_data = f'{dbfs_folder_base_path}/Data'
    folder_path_src_data = f'{dbfs_folder_base_path}/Data/Source'
    folder_path_tgt_data = f'{dbfs_folder_base_path}/Data/Target'
    folder_path_results = f'{dbfs_folder_base_path}/Results'
    folder_path_scripts = f'{dbfs_folder_base_path}/Scripts'
    print(f"folder_path_config:{folder_path_config}\nfolder_path_logs:{folder_path_logs}\nfolder_path_data:{folder_path_data}")
    print(f"folder_path_src_data:{folder_path_src_data}\nfolder_path_tgt_data:{folder_path_tgt_data}\nfolder_path_results:{folder_path_results}")
    print(f"folder_path_scripts:{folder_path_scripts}")
    return folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts
    
def create_required_dbfs_folder_structure(dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/'):
    folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts = get_required_dbfs_folder_structure(dbfs_folder_base_path)
    if dbfs_folder_base_path[:5]=='/dbfs':
        dbutilspkg.fs.mkdirs(folder_path_config[5:])
        dbutilspkg.fs.mkdirs(folder_path_logs[5:])
        dbutilspkg.fs.mkdirs(folder_path_src_data[5:])
        dbutilspkg.fs.mkdirs(folder_path_tgt_data[5:])
        dbutilspkg.fs.mkdirs(folder_path_results[5:])
        dbutilspkg.fs.mkdirs(folder_path_scripts[5:])
    else:
        dbutilspkg.fs.mkdirs(folder_path_config)
        dbutilspkg.fs.mkdirs(folder_path_logs)
        dbutilspkg.fs.mkdirs(folder_path_src_data)
        dbutilspkg.fs.mkdirs(folder_path_tgt_data)
        dbutilspkg.fs.mkdirs(folder_path_results)
        dbutilspkg.fs.mkdirs(folder_path_scripts)
    return folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts

# ----------

def append_elements_to_sys_path(dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/', folder_struct_lst = ['config', 'python', 'spark/common']):
    for folder in folder_struct_lst:
        path = f'{dbfs_folder_base_path}/{folder}'.replace('//','/')
        if (path != None) and (path not in sys.path):
            sys.path.insert(1, path)
            print(f'Added {path} to sys.path')

# ----------

def get_final_overall_status(table_name, cz_base_path='', user_prefix='', dbfs_folder_base_path='', ignore_column=[], overall_df=''):
    if overall_df == '':
        regression_df = spark.sql(f"""select type,table_name,column_name,overall_status,process_date,last_insert_timestamp
                                        from delta.`{cz_base_path}/rna_qa/rna_regression_summary_{user_prefix}`
                                        where last_insert_timestamp = (select max(last_insert_timestamp) 
                                                                        from delta.`{cz_base_path}/rna_qa/rna_regression_summary_{user_prefix}`
                                                                        where table_name='{table_name}'
                                                                            and type='Regression')""")
        # regression_df.show()
        unittest_df = spark.sql(f"""select type,table_name,column_name,overall_status,process_date,last_insert_timestamp
                                    from delta.`{cz_base_path}/rna_qa/rna_regression_summary_{user_prefix}`
                                    where last_insert_timestamp = (select max(last_insert_timestamp) 
                                                                    from delta.`{cz_base_path}/rna_qa/rna_regression_summary_{user_prefix}`
                                                                    where table_name='{table_name}'
                                                                        and type in ('Count', 'Null', 'Duplicate'))""")
        # unittest_df.show()

        # getting overall_df
        overall_df = regression_df.union(unittest_df)

    # writing overall_df to a csv file inside a folder
    # overall_df.coalesce(1).write.mode('overwrite').csv(f'{dbfs_folder_base_path}/Data/temp_qa_overall_final', header=True, sep='|')
    overall_file_path = f'{dbfs_folder_base_path}/Data/Source/temp_qa_overall_final.csv'
    overall_pd_df = overall_df.toPandas()
    overall_pd_df.to_csv(f'{overall_file_path}', sep='|', index=False)
    overall_df = overall_df.select('type','table_name','column_name','overall_Status')

    if ignore_column:
        filter_str = f"""('{"','".join(ignore_column)}')"""
        print(f'filter_str: {filter_str}')
        overall_df = overall_df.filter(f'column_name not in {filter_str} and type="Regression"')
    overall_lst = overall_df.select('overall_Status')\
                            .withColumnRenamed('overall_Status', 'overall_status')\
                            .collect()
    overall_set = set([elem.overall_status for elem in overall_lst])
    print(f'overall_set: {overall_set}')
    if 'FAIL' in overall_set:
        return 'FAIL', overall_file_path
    else:
        return 'PASS', overall_file_path

# ----------

# # Uncomment to run the Example
# # display(dbutils.fs.ls('abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/{Folder_path}'))
# container_name = 'wca'
# storage_acc_name = 'edaaaedle1devsrz'
# folder_path = 'WCA'
# table_name = 'mv_acc'
# podium_delivery_date = '20230101'  #opltional by default it will give the latest partition
# database_name='wca'
# # src_base_path = f'edaaaedle1devsrz.dfs.core.windows.net/{db_name.upper()}'  #original source
# mv_acc = create_temp_combined_view(table_name=table_name, database_name=database_name, source='view')
# # mv_acc = create_temp_combined_view(container_name, storage_acc_name, folder_path, table_name, podium_delivery_date)