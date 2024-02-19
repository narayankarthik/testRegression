# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# ----------

####### Update this cell #######

def execute(table_name='customer', script_type='super_script', config_file_path='', env='test', pod_name='dc',
            dbfs_folder_base_path='/dbfs/FileStore/testRepo/Mainkar/', passed_as_of_date='20230930', extract_prep_flag='prep'):

    # table_name='customer'
    # script_type='super_script'
    # config_file_path=''
    # env='test'
    # pod_name='dc'
    # dbfs_folder_base_path='/dbfs/FileStore/testRepo/Mainkar/'
    # extract_prep_flag='prep'
    # passed_as_of_date='20230930'

    print(f'table_name:{table_name}\nscript_type:{script_type}\nconfig_file_path:{config_file_path}')
    print(f'env:{env}\npod_name:{pod_name}\ndbfs_folder_base_path:{dbfs_folder_base_path}')
    print(f'passed_as_of_date:{passed_as_of_date}\nextract_prep_flag:{extract_prep_flag}')

    # ----------

    sys.path.insert(1, f'{dbfs_folder_base_path}/Scripts/python'.replace('//','/'))
    sys.path.insert(1, f'{dbfs_folder_base_path}/Scripts/spark/common'.replace('//','/'))
    import external_functions
    import dates_needed
    # import jtmf_update
    import common_variables
    import pass_fail_result_summary_part
    import target_table_unittest

    # ----------

    # Uncoment below 2 lines for actual python scripts in dbfs
    app_name = table_name
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    dbutilspkg = DBUtils(spark)

    # ----------

    #variables that can be skipped based on weather script_type='sub_script'
    if script_type == 'super_script':

        # running common variables and extracting the required variables
        cmn_vars = common_variables.All_Env_Specific_Variables(env, pod_name, dbfs_folder_base_path)
        dbfs_folder_base_path = cmn_vars.dbfs_folder_base_path
        db_names = cmn_vars.db_names
        cz_base_path = cmn_vars.cz_base_path

        # calling required external functions
        folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts = external_functions.create_required_dbfs_folder_structure(dbfs_folder_base_path)
        user_name, user_prefix = external_functions.current_user_info()

        # creating config file
        config_file_path = dates_needed.all_parameters_needed(passed_as_of_date, folder_path_config, table_name, db_names, env, user_prefix, pod_name, extract_prep_flag=extract_prep_flag)
        cmn_vars.common_vars_add_to_config(config_file_path)

        # tgt_extract_file_path=f'{folder_path_tgt_data[5:]}/{table_name}/{table_name}_{passed_as_of_date}_mod.psv'
        # dates_needed.add_aditional_parameters(config_file_path, tgt_extract_file_path=tgt_extract_file_path)

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
    pk_col = conf.get('pk_col','')
    # tgt_extract_file_path = conf.get('tgt_extract_file_path')
    if script_type == 'sub_script':
        cz_base_path = conf.get('cz_base_path')

    # ----------

    ####### Update this cell #######

    # Add testing specific path, you can hardcode it completely
    tgt_table_cz_path = f'{cz_base_path}/wealth_data_warehouse_db/{table_name}/'
    onprm_table_cz_path = f'{cz_base_path}/wealth_data_warehouse_db/{table_name}/'
    src_table_cz_path = f'{cz_base_path}/rna_qa/qa_{table_name}_final'
    conf = dates_needed.add_aditional_parameters(config_file_path, tgt_table_cz_path=tgt_table_cz_path, src_table_cz_path=src_table_cz_path, onprm_table_cz_path=onprm_table_cz_path)

    # 3 required variable, dont remove them but you can pass ""(blank) as the value
    non_nullable_col = ""
    pk_col = "c_custkey"
    ignore_col = ""
    important_col = ""
    conf = dates_needed.add_aditional_parameters(config_file_path, non_nullable_col=non_nullable_col, pk_col=pk_col, ignore_col=ignore_col, important_col=important_col)

    # ----------

    ####### Update this cell #######

    df = spark.sql(f"""
                        select *
                        from samples.tpch.customer 
                        --where process_date='{process_date}'
                        limit 100
                        """)
    src_df = df
    # adding qa_ prefix to column names and changing it to lower case
    for col_name in src_df.columns:
        if col_name[:3] != 'qa_':
            src_df = src_df.withColumnRenamed(col_name, f'qa_{col_name.lower()}')
        else:
            src_df = src_df.withColumnRenamed(col_name, col_name.lower())
    print("src_df count: {0}".format(src_df.count()))

    # ----------

    # creating a view
    src_df.createOrReplaceGlobalTempView('temp_qa_{0}_final'.format(table_name))
    print(f"Have overwriten the new records to global temp view temp_qa_{table_name}_final.")
    spark.sql(f"select count(*) from global_temp.temp_qa_{table_name}_final").show()

    # ----------

    ####### Update this cell #######

    # Tgt part
    tgt_df = df
    # changing columns to lower case
    for col_name in tgt_df.columns:
        tgt_df = tgt_df.withColumnRenamed(col_name, col_name.lower())
    print("tgt_df count: {0}".format(tgt_df.count()))
    tgt_df.createOrReplaceGlobalTempView('temp_dev_{0}_final'.format(table_name))

    # ----------

    pass_fail_result_summary_part.execute(table_name, 'sub_script', config_file_path, pk_col, env, pod_name, file_view_flag='view_all', dbfs_folder_base_path=dbfs_folder_base_path,
                                            passed_as_of_date=passed_as_of_date, extract_prep_flag=extract_prep_flag)

    # ----------

    target_table_unittest.execute(table_name, 'sub_script', config_file_path, env, pod_name, file_view_flag='view_all', dbfs_folder_base_path=dbfs_folder_base_path,
                                    passed_as_of_date=passed_as_of_date, extract_prep_flag=extract_prep_flag)

    # ----------

    print("Pass fail Result:")
    spark.sql(f"select * from global_temp.temp_{table_name}_pass_fail").show()
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

    # # ----------
    #
    # overall_status, overall_file_path = external_functions.get_final_overall_status(table_name, cz_base_path, user_prefix, dbfs_folder_base_path)
    # print(f"Overall Status: {overall_status}")
    # jtmf_params = [{'table_name':table_name, 'overall_status':overall_status, 'config_params':conf}]
    # # jtmf_update.jtmf_update_main('TDAASWLTH-2611', jtmf_params, dbfs_folder_base_path, overall_file_path, config_file_path)

# ----------

# running the python file
if __name__ == '__main__':
    import sys
    if len(sys.argv)==8:
        execute(table_name=sys.argv[1], script_type=sys.argv[2], config_file_path=sys.argv[3], env=sys.argv[4],
                pod_name=sys.argv[5], dbfs_folder_base_path=sys.argv[6], passed_as_of_date=sys.argv[7], extract_prep_flag=sys.argv[8])
    else:
        print("""This scrip needs 8 arguments to be passed you didnt pass all of them.
        So running with default arguments.
        Arguments needed: table_name, script_type, config_file_path, env, pod_name, 
                        dbfs_folder_base_path, passed_as_of_date, extract_prep_flag""")
        execute(table_name='table_name', script_type='super_script', config_file_path='', env='test', pod_name='dc',
                dbfs_folder_base_path='/dbfs/FileStore/testRepo/Mainkar/', passed_as_of_date='20230930', extract_prep_flag='prep')
