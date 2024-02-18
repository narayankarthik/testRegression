# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


# COMMAND ----------

def execute(table_name='srd_tdw_edelivery', script_type='super_script', config_file_path='', env='dev', pod_name='ep1', dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/'):

    sys.path.insert(1, f'{dbfs_folder_base_path}/python'.replace('//','/'))
    sys.path.insert(1, f'{dbfs_folder_base_path}/spark/common'.replace('//','/'))
    import external_functions
    import dates_needed
    import common_variables
    import pass_fail_result_summary_part
    import target_table_unittest

    # ----------

    # Uncoment below 2 lines for actual python scripts in dbfs
    app_name = "synapse_prep"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    dbutilspkg = DBUtils(spark)

    # ----------

    #variables that can be skipped based on weather script_type='sub_script' 
    if script_type == 'super_script':
        passed_as_of_date = '20230101'
        synapse_suffix = '_base_table'

        # running common variables and extracting the required variables
        cmn_vars = common_variables.All_Env_Specific_Variables(env, pod_name, dbfs_folder_base_path)
        dbfs_folder_base_path = cmn_vars.dbfs_folder_base_path
        db_names = cmn_vars.db_names
        cz_base_path = cmn_vars.cz_base_path
        
        # calling required external functions
        folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts = external_functions.create_required_dbfs_folder_structure(dbfs_folder_base_path)
        user_name, user_prefix = external_functions.current_user_info()

        # creating config file
        config_file_path = dates_needed.all_parameters_needed(passed_as_of_date, folder_path_config, table_name, db_names, env, user_prefix, pod_name, synapse_suffix=synapse_suffix)
        cmn_vars.common_vars_add_to_config(config_file_path)

    # ----------

    conf = dict()
    with open(config_file_path, 'r') as config_file:
        for line in config_file:
            # print(line[:-1])
            line = line[:-1].split('=')
            conf[line[0]] = line[1].replace('\r', '')

    print("Yaml content:{0}".format(conf))

    as_of_date = conf.get('as_of_date')
    process_date = conf.get('process_date')
    podium_delivery_date = conf.get('podium_delivery_date')
    pk_col = conf.get('pk_col')
    synapse_conn = eval(conf.get('synapse_conn','{}'))
    if script_type == 'sub_script':
        cz_base_path = conf.get('cz_base_path')

    tgt_table_cz_path = f'{cz_base_path}/{table_name}/'
    src_table_cz_path = f'{cz_base_path}/rna_qa/qa_{table_name}{synapse_suffix}_final'
    conf = dates_needed.add_aditional_parameters(config_file_path, tgt_table_cz_path=tgt_table_cz_path, src_table_cz_path=src_table_cz_path)

    # ----------

    src_final_df = spark.sql(f"select * from wdar_qa.qa_{table_name}_final")
    src_final_df.createOrReplaceTempView(f'temp_qa_{table_name}{synapse_suffix}_final')
    print("src_final_df count: {0}".format(src_final_df.count()))

    # ----------

    # creating a file/table
    try:
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}{synapse_suffix}_final")
        src_final_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}{synapse_suffix}_final', path=f'{src_table_cz_path}')
    except:
        dbutilspkg.fs.rm(f'{src_table_cz_path}',True)
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}{synapse_suffix}_final")
        src_final_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}{synapse_suffix}_final', path=f'{src_table_cz_path}')
    print(f"Have overwriten the new records to wdar_qa.qa_{table_name}{synapse_suffix}_final.")
    # creating a view
    src_final_df.createOrReplaceGlobalTempView(f'temp_qa_{table_name}{synapse_suffix}_final')
    print(f"Have overwriten the new records to global temp view temp_qa_{table_name}{synapse_suffix}_final.")
    spark.sql(f"select count(*) from wdar_qa.qa_{table_name}{synapse_suffix}_final").show()

    # ----------

    # Tgt part
    # reverse the comments on the 2 below tgt_df statements depending on the data availability
    # # Regular: Start
    # JdbcUrl = f"jdbc:sqlserver://{synapse_conn['server']}:1433;database={synapse_conn['database']};loginTimeout=10;"
    # username = dbutilspkg.secrets.get(scope=synapse_conn['scope'], key=synapse_conn['username'])
    # password = dbutilspkg.secrets.get(scope=synapse_conn['scope'], key=synapse_conn['password'])

    # connectionProperties = {
    #     "AADSecurePrincipalId" : username,
    #     "AADSecurePrincipalSecret" : password,
    #     "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    #     "authentication": "ActiveDirectoryServicePrincipal" ,
    #     "fetchsize":"10"
    #     }
    # sql_query = f'''(SELECT * FROM  wcz0005.{table_name}{synapse_suffix})t'''
    # tgt_df = spark.read.jdbc(url=JdbcUrl,table=sql_query,properties=connectionProperties)
    # # Regular: End

    # For test: Start
    tgt_df = spark.sql(f"select * from temp_qa_{table_name}{synapse_suffix}_final")
    for col_nm in tgt_df.columns:
        tgt_df = tgt_df.withColumnRenamed(f'{col_nm}', f'{col_nm[3:]}')
    # For test: End
    print("tgt_df count: {0}".format(tgt_df.count()))
    tgt_df.createOrReplaceGlobalTempView(f'temp_dev_{table_name}{synapse_suffix}_final')


    # ----------

    pass_fail_result_summary_part.execute(table_name, 'sub_script', config_file_path, pk_col, env, pod_name, file_view_flag='view', dbfs_folder_base_path=dbfs_folder_base_path)

    # ----------

    target_table_unittest.execute(table_name, 'sub_script', config_file_path, env, pod_name, file_view_flag='view', dbfs_folder_base_path=dbfs_folder_base_path)

    # ----------

    print('Regression result:')
    spark.sql(f'''select type,table_name,column_name,overall_Status,
                    src_count,tgt_count,pass_count,fail_count,null_count,
                    src_pass_percentage,tgt_pass_percentage,
                    tgt_null_percentage,process_date,last_insert_timestamp 
                from global_temp.temp_{table_name}_regression_result_summary''').show()
    print('Unittest result:')
    spark.sql(f'''select type,table_name,column_name,overall_Status,
                    src_count,tgt_count,pass_count,fail_count,null_count,
                    src_pass_percentage,tgt_pass_percentage,
                    tgt_null_percentage,process_date,last_insert_timestamp
                from global_temp.temp_{table_name}_unittest_result_summary''').show()
