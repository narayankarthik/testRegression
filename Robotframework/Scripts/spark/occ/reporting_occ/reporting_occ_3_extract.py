# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# COMMAND ----------

def execute(table_name='reporting_occ', script_type='super_script', config_file_path='', env='cz_staging', pod_name='dc',
            dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Mainkar/', passed_as_of_date='20230425', extract_prep_flag='extract'):

    # table_name='reporting_occ'
    # script_type='super_script'
    # config_file_path=''
    # env='cz_staging'
    # pod_name='dc'
    # dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Mainkar/'
    # passed_as_of_date='20230927'
    # extract_prep_flag='extract'
    print(f'table_name:{table_name}\nscript_type:{script_type}\nconfig_file_path:{config_file_path}')
    print(f'env:{env}\npod_name:{pod_name}\ndbfs_folder_base_path:{dbfs_folder_base_path}\n')
    print(f'passed_as_of_date:{passed_as_of_date}\nextract_prep_flag:{extract_prep_flag}')

    sys.path.insert(1, f'{dbfs_folder_base_path}/python'.replace('//','/'))
    sys.path.insert(1, f'{dbfs_folder_base_path}/spark/common'.replace('//','/'))
    import external_functions
    import dates_needed
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
        table_suffix = '_3'

        # running common variables and extracting the required variables
        cmn_vars = common_variables.All_Env_Specific_Variables(env, pod_name, dbfs_folder_base_path)
        dbfs_folder_base_path = cmn_vars.dbfs_folder_base_path
        db_names = cmn_vars.db_names
        cz_base_path = cmn_vars.cz_base_path

        # calling required external functions
        folder_path_config, folder_path_logs, folder_path_src_data, folder_path_tgt_data, folder_path_results, folder_path_scripts = external_functions.create_required_dbfs_folder_structure(dbfs_folder_base_path)
        user_name, user_prefix = external_functions.current_user_info()

        # creating config file
        config_file_path = dates_needed.all_parameters_needed(passed_as_of_date, folder_path_config, table_name, db_names, env, user_prefix, pod_name, extract_prep_flag=extract_prep_flag, table_suffix=table_suffix)
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
    primary_key = conf.get('primary_key')
    if (extract_prep_flag == 'extract') and (primary_key is not None):
        pk_col = ','.join(eval(primary_key))
        non_nullable_col = ''
        ignore_col = ''
    else:
        pk_col = conf.get('pk_col')
    tgt_extract_file_path = conf.get('tgt_extract_file_path')
    table_suffix = conf.get('table_suffix','')
    if script_type == 'sub_script':
        cz_base_path = conf.get('cz_base_path')

    tgt_table_cz_path = f'{cz_base_path}/occ/{table_name}/'
    src_table_cz_path = f'{cz_base_path}/rna_qa/qa_{table_name}{table_suffix}_final'
    conf = dates_needed.add_aditional_parameters(config_file_path, tgt_table_cz_path=tgt_table_cz_path, src_table_cz_path=src_table_cz_path)

    # ----------

    from pyspark.sql.functions import lit, row_number, coalesce, col
    from pyspark.sql.window import Window
    from pyspark.sql.types import DecimalType

    # ----------

    #variables that can be skipped based on weather script_type='sub_script', also some of these variables are coming from the previous step
    if script_type == 'super_script':

        tgt_extract_file_path=f'{folder_path_tgt_data[5:]}/{table_name}/{table_name}{table_suffix}_{passed_as_of_date}_mod.psv'
        conf = dates_needed.add_aditional_parameters(config_file_path, non_nullable_col=non_nullable_col, pk_col=pk_col, ignore_col=ignore_col, tgt_extract_file_path=tgt_extract_file_path, table_suffix=table_suffix)

    # ----------

    # Right now this table dosent exist, uncomment it once we have proper data for this table
    rep_occ_table_name = 'reporting_occ'
    # reverse the comment statements later
    # rep_occ_df = spark.sql(f"select * from wdar_qa.qa_{table_name}_final_orig")
    # for col_name in rep_occ_df.columns:
    #     if col_name[:3] == 'qa_':
    #         rep_occ_df = rep_occ_df.withColumnRenamed(col_name, col_name[3:])
    rep_occ_df = spark.sql(f"select * from delta.`{tgt_table_cz_path}`")\
                            .filter('process_date="{0}"'.format(process_date))

    rep_occ_df.createOrReplaceTempView(f'temp_qa_{rep_occ_table_name}')
    print(f'View created:temp_qa_{rep_occ_table_name}')
    rep_occ_cnt = rep_occ_df.count()
    print(f"rep_occ_df count: {rep_occ_cnt}")

    # ----------

    # reverse the comment statements later
    src_final_df = spark.sql(f"""
            select '20030618' as qa_fixml_r,
                '20040109' as qa_fixml_s,
                '4.4' as qa_fixml_v,
                'FIA' as qa_fixml_xr,
                '1.1' as qa_fixml_xv,
                '' as qa_fixml_text,
                run_date as qa_batch_bizdt,
                {rep_occ_cnt} as qa_batch_totmsg,
                '' as qa_batch_text,
                '4' as qa_posmntreq_txntyp,
                '1' as qa_posmntreq_actn,
                run_date as qa_posmntreq_bizdt,
                'Spreads' as qa_posmntreq_txt,
                '' as qa_posmntreq_text,
                '00615' as qa_pty_id,
                '4' as qa_pty_r,
                '' as qa_pty_text,
                'C' as qa_sub_id,
                '26' as qa_sub_typ,
                '' as qa_sub_text,
                symbol as qa_instrmt_sym,
                cfi as qa_instrmt_cfi,
                mmy as qa_instrmt_mmy,
                cast(cast(occ_strkprice_report as decimal(30,2)) as string) as qa_instrmt_strkpx,
                '' as qa_instrmt_text,
                'IAS' as qa_qty_typ,
                occ_qty_reported as qa_qty_long,
                '' as qa_qty_text
            from temp_qa_{rep_occ_table_name}
    """)
    order_spec = Window.partitionBy('qa_instrmt_sym', 'qa_instrmt_mmy').orderBy(src_final_df.columns)
    src_final_df = src_final_df.withColumn('qa_rnm', row_number().over(order_spec))
    src_final_df.createOrReplaceTempView(f'temp_qa_{table_name}_final')
    print(f'View created: temp_qa_{table_name}_final')
    print("src_final_df count: {0}".format(src_final_df.count()))

    # ----------

    # creating a file/table
    try:
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}{table_suffix}_final")
        src_final_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}{table_suffix}_final', path=f'{src_table_cz_path}')
    except:
        dbutilspkg.fs.rm(f'{src_table_cz_path}',True)
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}{table_suffix}_final")
        src_final_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}{table_suffix}_final', path=f'{src_table_cz_path}')
    print(f"Have overwriten the new records to wdar_qa.qa_{table_name}_final.")
    # creating a view
    src_final_df.createOrReplaceGlobalTempView('temp_qa_{0}{1}_final'.format(table_name, table_suffix))
    print(f"Have overwriten the new records to global temp view temp_qa_{table_name}{table_suffix}_final.")
    spark.sql(f"select count(*) from global_temp.temp_qa_{table_name}{table_suffix}_final").show()

    # ----------

    # Tgt part
    tgt_df = spark.read.csv(tgt_extract_file_path.replace('\\','/'), header='True', sep='|')
    order_spec = Window.partitionBy('instrmt_sym', 'instrmt_mmy').orderBy(tgt_df.columns)
    tgt_df = tgt_df.withColumn('rnm', row_number().over(order_spec))
    print("tgt_df count: {0}".format(tgt_df.count()))
    tgt_df.createOrReplaceGlobalTempView('temp_dev_{0}{1}_final'.format(table_name, table_suffix))

    # ----------

    pass_fail_result_summary_part.execute(table_name, 'sub_script', config_file_path, pk_col, env, pod_name, dbfs_folder_base_path=dbfs_folder_base_path,
                                            passed_as_of_date=passed_as_of_date, extract_prep_flag=extract_prep_flag)

    # ----------

    target_table_unittest.execute(table_name, 'sub_script', config_file_path, env, pod_name, file_view_flag='view', dbfs_folder_base_path=dbfs_folder_base_path,
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

# running the python file
if __name__ == '__main__':
    import sys
    if len(sys.argv)==8:
        execute(table_name=sys.argv[1], script_type=sys.argv[2], config_file_path=sys.argv[3], env=sys.argv[4], pod_name=sys.argv[5], dbfs_folder_base_path=sys.argv[6], passed_as_of_date=sys.argv[7])
    else:
        print("""This scrip needs 6 arguments to be passed you didnt pass all of them.
        So running with default arguments.
        Arguments needed: table_name, script_type, config_file_path, env, pod_name, dbfs_folder_base_path""")
        execute(table_name='reporting_occ', script_type='super_script', config_file_path='', env='cz_staging', pod_name='dc',
                dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Mainkar/', passed_as_of_date='20230425', extract_prep_flag='extract')