# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# COMMAND ----------

def execute(table_name='srd_tdw_iss', script_type='super_script', config_file_path='', env='dev', pod_name='ep1', file_view_flag='file', 
            dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/', passed_as_of_date='20230425', extract_prep_flag='prep'):

    sys.path.insert(1, f'{dbfs_folder_base_path}/python'.replace('//','/'))
    sys.path.insert(1, f'{dbfs_folder_base_path}/spark/common'.replace('//','/'))
    import external_functions
    import dates_needed
    import common_variables

    # ----------

    # Uncoment below 2 lines for actual python scripts in dbfs
    app_name = "target_table_unittest"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    dbutilspkg = DBUtils(spark)

    # ----------

    #variables that can be skipped based on weather script_type='sub_script'
    if script_type == 'super_script':
        # passed_as_of_date = '20230101'

        # running common variables and extracting the required variables
        cmn_vars = common_variables.All_Env_Specific_Variables(env, pod_name, dbfs_folder_base_path)
        dbfs_folder_base_path = cmn_vars.dbfs_folder_base_path
        db_names = cmn_vars.db_names
        cz_base_path = cmn_vars.cz_base_path

        # setting folder structure and getting user specific prefix
        folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts = external_functions.create_required_dbfs_folder_structure(dbfs_folder_base_path)
        user_name, user_prefix = external_functions.current_user_info()

        # creating config file
        config_file_path = dates_needed.all_parameters_needed(passed_as_of_date, folder_path_config, table_name, db_names, env, user_prefix, pod_name, extract_prep_flag=extract_prep_flag)
        cmn_vars.common_vars_add_to_config(config_file_path)

        tgt_table_cz_path = f'{cz_base_path}/{table_name}/'
        src_table_cz_path = f'{cz_base_path}/rna_qa/qa_{table_name}_final'
        dates_needed.add_aditional_parameters(config_file_path, tgt_table_cz_path=tgt_table_cz_path, src_table_cz_path=src_table_cz_path)

    # ----------

    conf = dict()
    with open(config_file_path, 'r') as config_file:
        for line in config_file:
            line = line[:-1].split('=')
            conf[line[0]] = line[1].replace('\r', '')

    print("Yaml content:{0}".format(conf))

    as_of_date = conf.get('as_of_date')
    process_date = conf.get('process_date')
    podium_delivery_date = conf.get('podium_delivery_date')
    non_nullable_col = conf.get('non_nullable_col','')
    pk_col = conf.get('pk_col','')
    user_prefix = conf.get('user_prefix')
    table_suffix = conf.get('table_suffix','')
    synapse_suffix = conf.get('synapse_suffix','')
    if script_type == 'sub_script':
        cz_base_path = conf.get('cz_base_path')

    tgt_table_cz_path = conf.get('tgt_table_cz_path')
    src_table_cz_path = conf.get('src_table_cz_path')

    # ----------

    # Src part
    if 'view' in file_view_flag:
        print('Getting Source data from View')
        spark.sql(f"select * from global_temp.temp_qa_{table_name}{table_suffix}{synapse_suffix}_final").createOrReplaceTempView('qa_src_final_table')
        # spark.sql(f"select * from global_temp.temp_dev_{table_name}{table_suffix}{synapse_suffix}_final").createOrReplaceTempView('dev_src_final_table')
    else:
        print('Getting Source data from File')
        spark.sql(f"select * from delta.`{src_table_cz_path}`").createOrReplaceTempView('qa_src_final_table')
        # spark.sql(f"select * from delta.`{tgt_table_cz_path}` WHERE Process_Date = '{process_date}'").createOrReplaceTempView('dev_src_final_table')

    # Tgt part
    tgt_df = spark.sql(f"select * from global_temp.temp_dev_{table_name}{table_suffix}{synapse_suffix}_final").createOrReplaceTempView('dev_src_final_table')

    # ----------

    count_query = """
                with SRC_COUNT AS
                (
                    SELECT COUNT(*) as SRC_Count
                    FROM qa_src_final_table
                )
                ,TGT_COUNT AS
                (
                    SELECT COUNT(*) as TGT_COUNT
                    FROM dev_src_final_table
                )
                SELECT 'SF_CNT_001' as testcase_id
                    ,'Record Count check for the table' as testcase_description
                    ,'Count' as type
                    ,'{tgt_table_cz_path}' as Database_name
                    ,'{table_name}{table_suffix}{synapse_suffix}' as Table_Name
                    ,'' as column_name
                    ,case when src_count=tgt_count then 'PASS' else 'FAIL' end as overall_Status
                    ,SRC_COUNT as SRC_Count
                    ,TGT_COUNT as TGT_Count
                    ,null as PASS_Count
                    ,null as FAIL_Count
                    ,null as NULL_Count
                    ,null AS SRC_PASS_Percentage
                    ,null AS TGT_PASS_Percentage
                    ,null AS TGT_NULL_Percentage
                    ,substring(cast(to_timestamp('{as_of_date}','yyyyMMdd') as string),1,10) as process_date
                    ,cast(now() as string) as last_insert_timestamp
                    ,'{as_of_date}' as as_of_date
                from src_count cross join tgt_count
                """.format(tgt_table_cz_path=tgt_table_cz_path, as_of_date=as_of_date, process_date=process_date,
                        table_name=table_name, table_suffix=table_suffix,synapse_suffix=synapse_suffix)
    # print("Query count_query:\n{0}".format(count_query))
    count_df = spark.sql(count_query)
    count_df.createOrReplaceTempView('temp_qa_count')
    print("count_df count: {0}".format(count_df.count()))

    # ----------

    if non_nullable_col != '':
        non_nullable_list = non_nullable_col.split(',')
        non_nullable_str = ''
        for col in non_nullable_list:
            non_nullable_str+= '{col} is null or '.format(col=col)
        non_nullable_str = non_nullable_str[:-4]
        print(non_nullable_str)

        null_query = """
                    SELECT 'SF_NUL_001' as testcase_id
                        ,'Null check for the table' as testcase_description
                        ,'Null' as type
                        ,'{tgt_table_cz_path}' as Database_name
                        ,'{table_name}{table_suffix}{synapse_suffix}' as Table_Name
                        ,'{non_nullable_col}' as column_name
                        ,case when count(*) = 0 then 'PASS' else 'FAIL' end as overall_Status
                        ,null as SRC_Count
                        ,null as TGT_Count
                        ,null as PASS_Count
                        ,null as FAIL_Count
                        ,count(*) as NULL_Count
                        ,null AS SRC_PASS_Percentage
                        ,null AS TGT_PASS_Percentage
                        ,null AS TGT_NULL_Percentage
                        ,substring(cast(to_timestamp('{as_of_date}','yyyyMMdd') as string),1,10) as process_date
                        ,cast(now() as string) as last_insert_timestamp
                        ,'{as_of_date}' as as_of_date
                    FROM dev_src_final_table
                    where ({non_nullable_str})
                    """.format(tgt_table_cz_path=tgt_table_cz_path, as_of_date=as_of_date, process_date=process_date, 
                            table_name=table_name, non_nullable_col=non_nullable_col, non_nullable_str=non_nullable_str,
                            table_suffix=table_suffix,synapse_suffix=synapse_suffix)
        # print("Query null_query:\n{0}".format(null_query))
        null_df = spark.sql(null_query)
        null_df.createOrReplaceTempView('temp_qa_null')
    else:
        print('No null check was done for this table as non_nullable_col is empty')
        null_df = count_df.filter('1=2')
    print("null_df count: {0}".format(null_df.count()))

    # ----------

    if pk_col != '' and conf.get('dupe_check_flag','1') == '1':
        dupe_query = """
                    SELECT 'SF_DUP_001' as testcase_id
                        ,'Duplicate check for the table' as testcase_description
                        ,'Duplicate' as type
                        ,'{tgt_table_cz_path}' as Database_name
                        ,'{table_name}{table_suffix}{synapse_suffix}' as Table_Name
                        ,'{pk_col}' as column_name
                        ,case when count(*) = 0 then 'PASS' else 'FAIL' end as overall_Status
                        ,null as SRC_Count
                        ,null as TGT_Count
                        ,null as PASS_Count
                        ,count(*) as FAIL_Count
                        ,null as NULL_Count
                        ,null AS SRC_PASS_Percentage
                        ,null AS TGT_PASS_Percentage
                        ,null AS TGT_NULL_Percentage
                        ,substring(cast(to_timestamp('{as_of_date}','yyyyMMdd') as string),1,10) as process_date
                        ,cast(now() as string) as last_insert_timestamp
                        ,'{as_of_date}' as as_of_date
                    From
                    (
                        select {pk_col}
                        FROM dev_src_final_table
                        GROUP BY {pk_col}
                        HAVING count(*) > 1
                    )A
                    """.format(tgt_table_cz_path=tgt_table_cz_path, as_of_date=as_of_date, process_date=process_date, 
                            table_name=table_name, pk_col=pk_col, table_suffix=table_suffix,synapse_suffix=synapse_suffix)
        # print("Query dupe_query:\n{0}".format(dupe_query))
        dupe_df = spark.sql(dupe_query)
        dupe_df.createOrReplaceTempView('temp_qa_dupe')
    else:
        print('No dupe check was done for this table as pk_col is empty or dupe_check_flag is set to 0')
        dupe_df = count_df.filter('1=2')
    print("dupe_df count: {0}".format(dupe_df.count()))

    # ----------

    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql.functions import col

    sanity_df = count_df.union(null_df).union(dupe_df)

    sanity_df = sanity_df.withColumn('testcase_id', col('testcase_id').cast(StringType()))\
                        .withColumn('testcase_description', col('testcase_description').cast(StringType()))\
                        .withColumn('type', col('type').cast(StringType()))\
                        .withColumn('Database_name', col('Database_name').cast(StringType()))\
                        .withColumn('Table_Name', col('Table_Name').cast(StringType()))\
                        .withColumn('column_name', col('column_name').cast(StringType()))\
                        .withColumn('overall_Status', col('overall_Status').cast(StringType()))\
                        .withColumn('SRC_Count', col('SRC_Count').cast(IntegerType()))\
                        .withColumn('TGT_Count', col('TGT_Count').cast(IntegerType()))\
                        .withColumn('PASS_Count', col('PASS_Count').cast(IntegerType()))\
                        .withColumn('FAIL_Count', col('FAIL_Count').cast(IntegerType()))\
                        .withColumn('NULL_Count', col('NULL_Count').cast(IntegerType()))\
                        .withColumn('src_pass_percentage', col('src_pass_percentage').cast(DoubleType()))\
                        .withColumn('tgt_pass_percentage', col('tgt_pass_percentage').cast(DoubleType()))\
                        .withColumn('tgt_null_percentage', col('tgt_null_percentage').cast(DoubleType()))\
                        .withColumn('process_date', col('process_date').cast(StringType()))\
                        .withColumn('last_insert_timestamp', col('last_insert_timestamp').cast(StringType()))\
                        .withColumn('as_of_date', col('as_of_date').cast(StringType()))

    sanity_df = sanity_df.select('testcase_id', 'testcase_description', 'type', 'database_name', 'table_name', 'column_name',
                            'overall_Status', 'src_count', 'tgt_count',
                            'pass_count', 'fail_count', 'null_count', 'src_pass_percentage', 'tgt_pass_percentage',
                            'tgt_null_percentage', 'process_date', 'last_insert_timestamp', 'as_of_date')
                        
    print("sanity_df count: {0}".format(sanity_df.count()))
    sanity_df.show()

    # ----------

    sanity_df.createOrReplaceGlobalTempView('temp_{0}_unittest_result_summary'.format(table_name))
    print('unittest_result_summary global temp view created: temp_{0}_unittest_result_summary'.format(table_name))
    if file_view_flag != 'view_all':
        sanity_df.write.mode("append").format('delta').save(f'{cz_base_path}/rna_qa/rna_regression_summary_{user_prefix}')
        print(f"Have appended the new records to rna_regression_summary_{user_prefix}.")
