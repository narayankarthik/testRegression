# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# COMMAND ----------

def execute(table_name='srd_tdw_iss', script_type='super_script', config_file_path='', pk_col='', env='dev', pod_name='ep1', file_view_flag='file', 
            dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/', passed_as_of_date='20230425', extract_prep_flag='prep', join_type='full'):
    """

    Args:
        table_name:
        script_type:
        config_file_path:
        pk_col:
        env:
        pod_name:
        file_view_flag: file/view/file-mount/view-mount
        dbfs_folder_base_path:
        passed_as_of_date:
        extract_prep_flag:
        join_type:

    Returns:

    """
    sys.path.insert(1, f'{dbfs_folder_base_path}/python'.replace('//','/'))
    sys.path.insert(1, f'{dbfs_folder_base_path}/spark/common'.replace('//','/'))
    import external_functions
    import dates_needed
    import common_variables
    from pyspark.sql.functions import col, coalesce, lit, round, when, current_timestamp, row_number
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql.window import Window

    # ----------

    # Uncoment below 2 lines for actual python scripts in dbfs
    app_name = "pass_fail_result_summary"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    dbutilspkg = DBUtils(spark)

    # ----------

    #variables that can be skipped based on weather script_type='sub_script'
    if script_type == 'super_script':
        # passed_as_of_date = '20230101'

        # running common variables and extracting the required variables
        cmn_vars = common_variables.All_Env_Specific_Variables(env, pod_name, dbutilspkg, dbfs_folder_base_path)
        dbfs_folder_base_path = cmn_vars.dbfs_folder_base_path
        if 'mount' in file_view_flag:
            pz_base_path = cmn_vars.pz_mount_path
        else:
            pz_base_path = cmn_vars.pz_base_path

        # calling required external functions
        external_functions.create_required_dbfs_folder_structure(cmn_vars)

        # creating config file
        config_file_path = dates_needed.all_parameters_needed(passed_as_of_date, cmn_vars.folder_path_config, table_name,
                                                              cmn_vars.db_names, env, cmn_vars.user_prefix, pod_name,
                                                              extract_prep_flag=extract_prep_flag)
        cmn_vars.common_vars_add_to_config(config_file_path)

        tgt_table_pz_path = f'{pz_base_path}/{table_name}/'
        src_table_pz_path = f'{pz_base_path}/rna_qa/qa_{table_name}_final'
        dates_needed.add_aditional_parameters(config_file_path, tgt_table_pz_path=tgt_table_pz_path, src_table_pz_path=src_table_pz_path)

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
    if script_type == 'sub_script':
        if 'mount' in file_view_flag:
            pz_base_path = conf.get('pz_mount_path')
        else:
            pz_base_path = conf.get('pz_base_path')
    user_prefix = conf.get('user_prefix')
    table_suffix = conf.get('table_suffix','')
    synapse_suffix = conf.get('synapse_suffix','')
    if pk_col == 'config_file':
        pk_col = conf.get('pk_col', '')
    important_col = conf.get('important_col', '')
    ignore_col = conf.get('ignore_col', '')
    non_nullable_col = conf.get('non_nullable_col', '')
    threshold_col = conf.get('threshold_col', '')
    threshold_val_perc = float(conf.get('threshold_val_perc', "0.002"))

    tgt_table_pz_path = conf.get('tgt_table_pz_path')
    src_table_pz_path = conf.get('src_table_pz_path')

    # ----------

    # Src part
    # src_df = spark.sql(f"select * from temp_qa_{table_name}_final")
    if 'view' in file_view_flag:
        print('Getting Source data from View')
        src_df = spark.sql(f"select * from global_temp.temp_qa_{table_name}{table_suffix}{synapse_suffix}_final")
    else:
        print('Getting Source data from File')
        src_df = spark.sql(f"select * from delta.`{src_table_pz_path}`")
    src_count = src_df.count()
    # print(src_df)
    print("src_df count: {0}".format(src_count))

    # Tgt part
    tgt_df = spark.sql(f"select * from global_temp.temp_dev_{table_name}{table_suffix}{synapse_suffix}_final")
    tgt_count = tgt_df.count()
    # print(tgt_df)
    print("tgt_df count: {0}".format(tgt_count))

    # ----------

    if (pk_col=='') or (pk_col is None):
        # convert nulls to blank('')
        # src_df
        for colmn in src_df.columns:
            src_df = src_df.withColumn(colmn, coalesce(f'{colmn}', lit('')))
        # tgt_df
        for colmn in tgt_df.columns:
            tgt_df = tgt_df.withColumn(colmn, coalesce(f'{colmn}', lit('')))
        # assigning a unique row_id to each record and using that as the primary key
        # if important_col is given then incorporating it to generate row_id
        if important_col != '':
            important_col_lst = important_col.split(',')
            src_df = src_df.withColumn('qa_row_id', row_number().over(Window.partitionBy([f'qa_{col}' for col in important_col_lst]).orderBy(src_df.columns)))
            tgt_df = tgt_df.withColumn('row_id', row_number().over(Window.partitionBy(important_col_lst).orderBy(tgt_df.columns)))
            pk_col = f'{important_col},row_id'
        else:
            src_df = src_df.withColumn('qa_row_id', row_number().over(Window.orderBy(src_df.columns)))
            tgt_df = tgt_df.withColumn('row_id', row_number().over(Window.orderBy(tgt_df.columns)))
            pk_col = 'row_id'

    join_criteria = ''
    for cols in pk_col.split(','):
        # coalesce(src_final_df.qa_account_number_kyndryl, lit('')) == coalesce(tgt_df.account_number_kyndryl, lit(''))
        join_criteria += f"(coalesce(src_df.qa_{cols}, lit('')) == coalesce(tgt_df.{cols}, lit(''))) & "
    join_criteria = join_criteria[:-3]
    print(f'join_criteria:{join_criteria}')
    pass_fail_df = src_df.join(tgt_df, eval(join_criteria), join_type)
    # print(pass_fail_df.columns)
    print("pass_fail_df count: {0}".format(pass_fail_df.count()))

    # ----------

    tgt_df_column = tgt_df.columns
    src_df_column = src_df.columns

    threshold_col = threshold_col.split(',')
    tgt_df_column_set = set(tgt_df_column)
    ignore_column_list = ignore_col.split(',')
    ignore_column_set = set(ignore_column_list)
    if not ignore_column_set.issubset(tgt_df_column_set) and ignore_col!='':
        # raise Exception('all elements of {0} are not present in {1}'.format(ignore_column_set, tgt_df_column_set))
        print('all elements of {0} are not present in {1}'.format(ignore_column_set, tgt_df_column_set))
    tgt_df_column = list(tgt_df_column_set - ignore_column_set)
    print("Columns removed from comparison: {0}".format(list(ignore_column_set)))

    missing_column = list(set(src_df_column) - set(['qa_' + i for i in tgt_df_column]))
    print("Columns in source missing in target: {0}".format(missing_column))
    print("ignore_column_list: {0}".format(ignore_column_list))
    if ignore_col=='':
        pass_fail_df = external_functions.create_pass_fail_df(pass_fail_df, tgt_df_column, missing_column, threshold_col=threshold_col, threshold_val_perc=threshold_val_perc)
    else:
        pass_fail_df = external_functions.create_pass_fail_df(pass_fail_df, tgt_df_column, missing_column, ignore_column_list, threshold_col=threshold_col, threshold_val_perc=threshold_val_perc)
    print("pass_fail_df count: {0}".format(pass_fail_df.count()))
    print(pass_fail_df)
    pass_fail_df.createOrReplaceGlobalTempView('temp_{0}_pass_fail'.format(table_name))
    print('pass_fail global temp view created: temp_{0}_pass_fail'.format(table_name))

    result_summary_df = external_functions.create_result_summary_df(tgt_df_column, non_nullable_col)
    print("result_summary_df count: {0}".format(result_summary_df.count()))
    result_summary_df.cache()
    result_summary_df.show()

    # ----------

    finla_base_df = result_summary_df.withColumn("src_count",lit(src_count))\
                                    .withColumn("tgt_count",lit(tgt_count))
    print("finla_base_df count: {0}".format(finla_base_df.count()))

    # ----------

    finla_df = finla_base_df.withColumn('testcase_id', lit('SF_REG_001')) \
                            .withColumn('testcase_description', lit('To validate all the columns')) \
                            .withColumn('type', lit('Regression')) \
                            .withColumn('database_name', lit(tgt_table_pz_path)) \
                            .withColumn('table_name', lit('{0}'.format(table_name))) \
                            .withColumn('src_pass_percentage', round((col('pass_count') / col('src_count') * 100), 2)) \
                            .withColumn('tgt_pass_percentage', round((col('pass_count') / col('tgt_count') * 100), 2)) \
                            .withColumn('tgt_null_percentage', round((col('null_count') / col('tgt_count') * 100), 2)) \
                            .withColumn('last_insert_timestamp', current_timestamp().cast(StringType())) \
                            .withColumn('process_date', lit(process_date)) \
                            .withColumn('as_of_date', lit(as_of_date)) \
                            .withColumn('overall_Status', when(col('src_count') != col('tgt_count'), 'FAIL(Count Check)')
                                        .when(((col('nullable') == 'N') & (col('null_count') != 0)), 'FAIL(Null Check)')
                                        .when(((col('src_count') == col('tgt_count')) & (col('src_count') != col('pass_count') + col('fail_count'))),'FAIL(Duplicate Check)')
                                        .when(col('fail_count') == 0, 'PASS')
                                        .otherwise('FAIL'))
    print("finla_df count: {0}".format(finla_df.count()))

    # ----------

    finla_df = finla_df.withColumn('testcase_id', col('testcase_id').cast(StringType()))\
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
    finla_df = finla_df.select('testcase_id', 'testcase_description', 'type', 'database_name', 'table_name', 'column_name',
                            'overall_Status', 'src_count', 'tgt_count',
                            'pass_count', 'fail_count', 'null_count', 'src_pass_percentage', 'tgt_pass_percentage',
                            'tgt_null_percentage', 'process_date', 'last_insert_timestamp', 'as_of_date')
    finla_df.show()

    # ----------

    # finla_df.write.mode("overwrite").format('delta').save(f'{pz_base_path}/rna_qa/rna_regression_summary_kar')
    finla_df.createOrReplaceGlobalTempView('temp_{0}_regression_result_summary'.format(table_name))
    print('regression_result_summary global temp view created: temp_{0}_regression_result_summary'.format(table_name))
    finla_df.write.mode("append").format('delta').save(f'{pz_base_path}/rna_qa/rna_regression_summary_{user_prefix}')
    print(f"Have appended the new records to rna_regression_summary_{user_prefix}.")
