# Databricks notebook source
# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

# COMMAND ----------

def execute(table_name='reporting_occ', script_type='super_script', config_file_path='', env='dev', pod_name='dc', 
            dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Mainkar/', passed_as_of_date='20240108', extract_prep_flag='prep'):

    # table_name='reporting_occ'
    # script_type='super_script'
    # config_file_path=''
    # env='dev'
    # pod_name='dc'
    # dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Mainkar/'
    # extract_prep_flag='prep'
    # # # actual
    # passed_as_of_date='20240108'
    # # # to_test
    # # dbutils.widgets.text('passed_as_of_date', '20220426')
    # # dbutils.widgets.text('order_type', 'ordered')
    # # passed_as_of_date=dbutils.widgets.get('passed_as_of_date')
    # # order_type=dbutils.widgets.get('order_type')
    print(f'table_name:{table_name}\nscript_type:{script_type}\nconfig_file_path:{config_file_path}')
    print(f'env:{env}\npod_name:{pod_name}\ndbfs_folder_base_path:{dbfs_folder_base_path}')
    print(f'passed_as_of_date:{passed_as_of_date}\nextract_prep_flag:{extract_prep_flag}')
    # # actual
    order_type='ordered'    #ordered is the actual value
    # # to_test
    # passed_as_of_date=dbutils.widgets.get('passed_as_of_date')
    # order_type=dbutils.widgets.get('order_type')

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
        # passed_as_of_date = '20230425'

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

        tgt_extract_file_path=f'{folder_path_tgt_data[5:]}/{table_name}/{table_name}_{passed_as_of_date}_mod.psv'
        dates_needed.add_aditional_parameters(config_file_path, tgt_extract_file_path=tgt_extract_file_path)


    # COMMAND ----------

    # ----------

    conf = dict()
    with open(config_file_path, 'r') as config_file:
        for line in config_file:
            # print(line[:-1])
            line = line[:-1].split('=')
            conf[line[0]] = line[1].replace('\r', '')

    print("Yaml content:{0}".format(conf))

    as_of_date = conf.get('as_of_date')
    as_of_date_wca = conf.get('as_of_date_wca')
    process_date = conf.get('process_date')
    podium_delivery_date = conf.get('podium_delivery_date')
    pk_col = conf.get('pk_col')
    tgt_extract_file_path = conf.get('tgt_extract_file_path')
    if script_type == 'sub_script':
        cz_base_path = conf.get('cz_base_path')

    tgt_table_cz_path = f'{cz_base_path}/occ/{table_name}/'
    src_table_cz_path = f'{cz_base_path}/rna_qa/qa_{table_name}_final'
    conf = dates_needed.add_aditional_parameters(config_file_path, tgt_table_cz_path=tgt_table_cz_path, src_table_cz_path=src_table_cz_path)


    # COMMAND ----------

    # ----------

    from pyspark.sql.functions import regexp_replace, split, trim, col, when, substring, length, countDistinct, coalesce, lit, count, min, sum, max, mean, row_number, lag, concat, current_date, date_format, asc, desc
    from pyspark.sql.types import DecimalType, IntegerType, StringType
    from pyspark.sql.window import Window

    # ----------

    # actual
    # sec_offset_df = spark.sql(f"select * from delta.`{cz_base_path}/occ/security_offset_occ` where process_date = '{process_date}'")
    # # to_test
    sec_offset_df = spark.read.csv(f'dbfs:/FileStore/tables/DaaSWealth_QA/Mainkar/Config/data/security_offset_occ_{as_of_date_wca}.csv', header='True')
    run_date_val = sec_offset_df.select('run_date').distinct().collect()[0].run_date
    print("sec_offset_df count: {0}".format(sec_offset_df.count()))
    # sec_offset_df.show()


    # COMMAND ----------

    # ----------

    # Step 1.1
    sec_occ_1_1_df = sec_offset_df.withColumn('underlying_sec_name', when((sec_offset_df.underlying_sec_name.isNull()) | (sec_offset_df.underlying_sec_name == ''), sec_offset_df.option_name)
                                        .otherwise(sec_offset_df.underlying_sec_name))\
                                    .withColumn('underlying_symbol', when(sec_offset_df.underlying_symbol.isNotNull(), sec_offset_df.underlying_symbol)
                                                .otherwise(trim(split(regexp_replace(regexp_replace('option_name', "[\d\$\'\@\+]", '|'),'\|+', '|'),'\|')[1])))
    sec_occ_1_1_df.createOrReplaceTempView(f'qa_{table_name}_1_1_full_vw')
    print(f'View created: qa_{table_name}_1_1_full_vw')
    print("sec_occ_1_1_df count: {0}".format(sec_occ_1_1_df.count()))

    # ----------

    # Step 1.2
    # select
    sec_occ_1_2_df = sec_occ_1_1_df.withColumn('option_type', trim(substring(col('option_name'), 1, 4)))\
                                    .withColumn('option_tradeqty', col('option_tradeqty').cast(DecimalType()))\
                                    .select('bor_client_id', 'option_type', 'underlying_symbol', 'underlying',
                                            'underlying_sec_name', 'option_maturitydate', 'short_or_long',
                                            'strikepriceamount', 'option_tradeqty')
    # groupBy
    sec_occ_1_2_df = sec_occ_1_2_df.groupBy('bor_client_id', 'option_type', 'underlying_symbol', 'underlying',
                                            'underlying_sec_name', 'option_maturitydate', 'short_or_long',
                                            'strikepriceamount')\
                                    .sum('option_tradeqty')\
                                    .withColumnRenamed('sum(option_tradeqty)', 'quantity')
    # having
    sec_occ_1_2_df = sec_occ_1_2_df.filter('quantity <> 0')
    sec_occ_1_2_df.createOrReplaceTempView(f'qa_{table_name}_1_2_full_vw')
    print(f'View created: qa_{table_name}_1_2_full_vw')
    print("sec_occ_1_2_df count: {0}".format(sec_occ_1_2_df.count()))
    sec_occ_1_2_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 2
    # groupBy
    sec_occ_2_df = sec_occ_1_2_df.select('bor_client_id', 'option_type', 'underlying_symbol', 'short_or_long')
    sec_occ_2_df = sec_occ_2_df.groupBy('bor_client_id', 'option_type', 'underlying_symbol')\
                                .agg(countDistinct('short_or_long').alias('short_or_long_count'))\
                                    .filter("short_or_long_count > 1")
    sec_occ_2_df.createOrReplaceTempView(f'qa_{table_name}_2_full_vw')
    print(f'View created: qa_{table_name}_2_full_vw')
    print("sec_occ_2_df count: {0}".format(sec_occ_2_df.count()))
    sec_occ_2_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 3 join: join step 1.2 and step 2 table
    # selece/rename
    so_1_2_df = sec_occ_1_2_df.select('bor_client_id', 'underlying', 'underlying_symbol',
                                        'underlying_sec_name', 'option_type', 'option_maturitydate',
                                        'short_or_long', 'strikepriceamount', 'quantity')
    so_2_df = sec_occ_2_df.select(sec_occ_2_df.bor_client_id.alias('so2_bor_client_id'),
                                    sec_occ_2_df.option_type.alias('so2_option_type'),
                                    sec_occ_2_df.underlying_symbol.alias('so2_underlying_symbol'))
    # join
    sec_occ_3_join_df = so_1_2_df.join(so_2_df, (coalesce(so_1_2_df.bor_client_id, lit('')) == coalesce(so_2_df.so2_bor_client_id, lit('')))
                                                & (coalesce(so_1_2_df.option_type, lit('')) == coalesce(so_2_df.so2_option_type, lit('')))
                                                & (coalesce(so_1_2_df.underlying_symbol, lit('')) == coalesce(so_2_df.so2_underlying_symbol, lit(''))))
    sec_occ_3_join_df = sec_occ_3_join_df.select('bor_client_id', 'underlying', 'underlying_symbol',
                                                'underlying_sec_name', 'option_type', 'option_maturitydate',
                                                'short_or_long', 'strikepriceamount', 'quantity')\
                                        .withColumn('strikepriceamount', sec_occ_3_join_df.strikepriceamount.cast(DecimalType(30, 6)).alias('strikepriceamount'))
    sec_occ_3_join_df.createOrReplaceTempView(f'qa_{table_name}_3_join_full_vw')
    print(f'View created: qa_{table_name}_3_join_full_vw')
    print("sec_occ_3_join_df count: {0}".format(sec_occ_3_join_df.count()))
    sec_occ_3_join_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 3.1
    from pyspark.sql.functions import row_number, rank, dense_rank
    from pyspark.sql.window import Window
    # filter
    sec_occ_3_1_df = sec_occ_3_join_df.filter("short_or_long = 'Long'")
    sec_occ_3_1_df = sec_occ_3_1_df.groupBy('bor_client_id', 'option_type', 'underlying_symbol', 'underlying',
                                            'underlying_sec_name', 'option_maturitydate', 'strikepriceamount', 'short_or_long')\
                                    .agg(sum('quantity').alias('quantity'))
    # Window
    # bit of an overkill but keeping it as it is, in case of more analysis is needed
    if order_type[:7] == 'ordered':
        # ordered
        order_spec = Window.orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'underlying',
                                    'underlying_sec_name', 'option_maturitydate', col('strikepriceamount').asc())
    elif order_type == 'unordered':
        # unordered
        order_spec = Window.orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'option_maturitydate')
    sec_occ_3_1_df = sec_occ_3_1_df.withColumn('row_id', row_number().over(order_spec))
    # writing it to a file
    try:
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}_step3_1")
        sec_occ_3_1_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}_step3_1', path=f'{cz_base_path}/rna_qa/qa_{table_name}_step3_1')
    except:
        dbutilspkg.fs.rm(f'{cz_base_path}/rna_qa/qa_{table_name}_step3_1',True)
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}_step3_1")
        sec_occ_3_1_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}_step3_1', path=f'{cz_base_path}/rna_qa/qa_{table_name}_step3_1')
    sec_occ_3_1_df = spark.sql(f'select * from wdar_qa.qa_{table_name}_step3_1')

    sec_occ_3_1_df.createOrReplaceTempView(f'qa_{table_name}_3_1_full_vw')
    print(f'View created: qa_{table_name}_3_1_full_vw')
    # getting max_count for step 8
    sec_occ_3_1_cnt = sec_occ_3_1_df.count()
    max_length = int('9'*(len(str(sec_occ_3_1_cnt))+1))
    print(f"sec_occ_3_1_df count: {sec_occ_3_1_cnt}\nmax_length:{max_length}")
    sec_occ_3_1_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # COMMAND ----------

    # ----------

    # Step 3.2
    # GroupBy is already done in the previous step so not doing the same thing here as well
    # Error: I think it should be sum but SR says count
    sec_occ_3_2_df = sec_occ_3_1_df.groupBy('bor_client_id', 'underlying_symbol', 'underlying', 'option_type',
                                            'short_or_long', 'option_maturitydate')\
                                    .agg(min('row_id').alias('row_id'), sum('quantity').alias('quantity'))
                                    # .agg(min('row_id').alias('row_id'), count('quantity').alias('quantity'))
    # sec_occ_3_1_df = sec_occ_3_2_df.select('bor_client_id', 'underlying', 'underlying_symbol', 'option_type',
    #                                         'option_maturitydate', 'quantity', 'short_or_long')
    sec_occ_3_2_df.createOrReplaceTempView(f'qa_{table_name}_3_2_full_vw')
    print(f'View created: qa_{table_name}_3_2_full_vw')
    print("sec_occ_3_2_df count: {0}".format(sec_occ_3_2_df.count()))
    sec_occ_3_2_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 3.3
    # filter
    sec_occ_3_3_df = sec_occ_3_join_df.filter("short_or_long = 'Short'")
    sec_occ_3_3_df.createOrReplaceTempView(f'qa_{table_name}_3_3_full_vw')
    print(f'View created: qa_{table_name}_3_3_full_vw')
    print("sec_occ_3_3_df count: {0}".format(sec_occ_3_3_df.count()))
    sec_occ_3_3_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 3.4
    # filter
    sec_occ_3_4_df = sec_occ_3_3_df.groupBy('bor_client_id', 'underlying_symbol', 'underlying', 'option_type',
                                            'short_or_long', 'option_maturitydate', 'underlying_sec_name')\
                                    .agg(sum('quantity').alias('quantity'))
    sec_occ_3_4_df.createOrReplaceTempView(f'qa_{table_name}_3_4_full_vw')
    print(f'View created: qa_{table_name}_3_4_full_vw')
    print("sec_occ_3_4_df count: {0}".format(sec_occ_3_4_df.count()))
    sec_occ_3_4_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 4
    sec_occ_4_df = spark.sql(f"""
                                select a.row_id,
                                    case when a.bor_client_id is null then b.bor_client_id else a.bor_client_id end as bor_client_id,
                                    case when a.bor_client_id is null then b.option_type else a.option_type end as option_type,
                                    case when a.bor_client_id is null then b.underlying_symbol else a.underlying_symbol end as underlying_symbol,
                                    case when a.bor_client_id is null then b.underlying else a.underlying end as underlying,
                                    case when a.bor_client_id is null then b.option_maturitydate else a.option_maturitydate end as long_option_maturitydate,
                                    b.option_maturitydate as short_option_maturitydate, 
                                    coalesce(b.quantity, 0) as short_quantity,
                                    b.underlying_sec_name
                                from qa_{table_name}_3_2_full_vw a
                                full join qa_{table_name}_3_4_full_vw b
                                on a.bor_client_id <=> b.bor_client_id
                                    and a.option_type <=> b.option_type
                                    and a.underlying_symbol <=> b.underlying_symbol
                                    and a.underlying <=> b.underlying
                                    and a.option_maturitydate <=> b.option_maturitydate
                            """)
    sec_occ_4_df.createOrReplaceTempView(f'qa_{table_name}_4_full_vw')
    print(f'View created: qa_{table_name}_4_full_vw')
    print("sec_occ_4_df count: {0}".format(sec_occ_4_df.count()))
    sec_occ_4_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 5
    so_3_1_df = sec_occ_3_1_df.select(sec_occ_3_1_df.row_id.alias('os31_row_id'),
                                        sec_occ_3_1_df.bor_client_id.alias('os31_bor_client_id'),
                                        sec_occ_3_1_df.option_type.alias('os31_option_type'),
                                        sec_occ_3_1_df.underlying.alias('os31_underlying'),
                                        sec_occ_3_1_df.underlying_symbol.alias('os31_underlying_symbol'),
                                        sec_occ_3_1_df.underlying_sec_name.alias('os31_underlying_sec_name'),
                                        sec_occ_3_1_df.option_maturitydate.alias('os31_option_maturitydate'),
                                        'strikepriceamount',
                                        sec_occ_3_1_df.quantity.cast(DecimalType(precision=30, scale=0)).alias('long_quantity'))
    so_3_1_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'row_id').show()
    so_4_df = sec_occ_4_df.select('row_id',
                                    sec_occ_4_df.long_option_maturitydate.alias('os4_long_option_maturitydate'),
                                    sec_occ_4_df.bor_client_id.alias('os4_bor_client_id'),
                                    sec_occ_4_df.option_type.alias('os4_option_type'),
                                    sec_occ_4_df.underlying.alias('os4_underlying'),
                                    sec_occ_4_df.underlying_symbol.alias('os4_underlying_symbol'),
                                    sec_occ_4_df.underlying_sec_name.alias('os4_underlying_sec_name'),
                                    'short_option_maturitydate', 'short_quantity')
    so_4_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'row_id').show()
    # sec_occ_5_df_tmp = so_4_df.join(so_3_1_df, (coalesce(so_4_df.os4_bor_client_id, lit('')) == coalesce(so_3_1_df.os31_bor_client_id, lit('')))
    #                                         & (coalesce(so_4_df.os4_option_type, lit('')) == coalesce(so_3_1_df.os31_option_type, lit('')))
    #                                         & (coalesce(so_4_df.os4_underlying_symbol, lit('')) == coalesce(so_3_1_df.os31_underlying_symbol, lit('')))
    #                                         & (coalesce(so_4_df.os4_underlying, lit('')) == coalesce(so_3_1_df.os31_underlying, lit('')))
    #                                         & (coalesce(so_4_df.os4_long_option_maturitydate, lit('')) == coalesce(so_3_1_df.os31_option_maturitydate, lit(''))),
    #                                     'full')\
    sec_occ_5_df_tmp = so_4_df.join(so_3_1_df, (coalesce(so_4_df.row_id, lit('')) == coalesce(so_3_1_df.os31_row_id, lit(''))), 'full')\
                            .withColumn('long_option_maturitydate', when(so_4_df.os4_bor_client_id.isNull(), so_3_1_df.os31_option_maturitydate).otherwise(so_4_df.os4_long_option_maturitydate))\
                            .withColumn('option_type', when(so_4_df.os4_bor_client_id.isNull(), so_3_1_df.os31_option_type).otherwise(so_4_df.os4_option_type))\
                            .withColumn('underlying', when(so_4_df.os4_bor_client_id.isNull(), so_3_1_df.os31_underlying).otherwise(so_4_df.os4_underlying))\
                            .withColumn('underlying_symbol', when(so_4_df.os4_bor_client_id.isNull(), so_3_1_df.os31_underlying_symbol).otherwise(so_4_df.os4_underlying_symbol))\
                            .withColumn('underlying_sec_name', when(so_4_df.os4_bor_client_id.isNull(), so_3_1_df.os31_underlying_sec_name).otherwise(so_4_df.os4_underlying_sec_name))\
                            .withColumn('source', when(so_4_df.os4_bor_client_id.isNull(), lit('step3.1')).otherwise(lit('step4')))\
                            .withColumn('bor_client_id', when(so_4_df.os4_bor_client_id.isNull(), so_3_1_df.os31_bor_client_id).otherwise(so_4_df.os4_bor_client_id))
    sec_occ_5_df = sec_occ_5_df_tmp.select('row_id', 'bor_client_id', 'underlying', 'underlying_symbol', 'underlying_sec_name', 'option_type',
                                            'long_option_maturitydate', 'short_option_maturitydate', 'short_quantity',
                                            coalesce(sec_occ_5_df_tmp.long_quantity, lit(0)).alias('long_quantity'),
                                            coalesce(sec_occ_5_df_tmp.strikepriceamount.cast(DecimalType(30, 6)), lit(0)).alias('strikepriceamount'),
                                            'source')  #'underlying_sec_name'
    sec_occ_5_df.createOrReplaceTempView(f'qa_{table_name}_5_full_vw')
    print(f'View created: qa_{table_name}_5_full_vw')
    print("sec_occ_5_df count: {0}".format(sec_occ_5_df.count()))
    sec_occ_5_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 6
    # filter
    sec_occ_6_df = sec_occ_5_df.withColumn('occ_qty_reported', when((sec_occ_5_df.short_quantity + sec_occ_5_df.long_quantity) == 0, sec_occ_5_df.long_quantity).otherwise(lit('0')))\
                                .withColumn('occ_strkprice_report', when((sec_occ_5_df.short_quantity + sec_occ_5_df.long_quantity) == 0, sec_occ_5_df.strikepriceamount).otherwise(lit(0)))\
                                .withColumn('row_processed', when((sec_occ_5_df.short_quantity + sec_occ_5_df.long_quantity) == 0, lit('Yes')).otherwise(lit('No')))
    sec_occ_6_df = sec_occ_6_df.withColumn('occ_strkprice_report', sec_occ_6_df.occ_strkprice_report.cast(DecimalType(30, 6)).alias('occ_strkprice_report'))
    sec_occ_6_df.createOrReplaceTempView(f'qa_{table_name}_6_full_vw')
    print(f'View created: qa_{table_name}_6_full_vw')
    print("sec_occ_6_df count: {0}".format(sec_occ_6_df.count()))
    sec_occ_6_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 6.1
    sec_occ_6_1_df = sec_occ_6_df.filter("row_processed == 'Yes'")
    sec_occ_6_1_df.createOrReplaceTempView(f'qa_{table_name}_6_1_full_vw')
    print(f'View created: qa_{table_name}_6_1_full_vw')
    print("sec_occ_6_1_df count: {0}".format(sec_occ_6_1_df.count()))
    sec_occ_6_1_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # Step 6.2
    sec_occ_6_2_df = sec_occ_6_df.filter("row_processed == 'No'")
    sec_occ_6_2_df.createOrReplaceTempView(f'qa_{table_name}_6_2_full_vw')
    print(f'View created: qa_{table_name}_6_2_full_vw')
    print("sec_occ_6_2_df count: {0}".format(sec_occ_6_2_df.count()))
    sec_occ_6_2_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 7.1 & 7.2
    sec_occ_7_1_df = sec_occ_6_2_df.selectExpr('bor_client_id', 'underlying', 'underlying_symbol', 'option_type',
                                                'long_option_maturitydate', 'short_option_maturitydate', 'strikepriceamount', 'occ_qty_reported',
                                                'occ_strkprice_report', 'row_processed', 'source', 'underlying_sec_name',
                                                "stack(2, 'Short', short_quantity, 'Long', long_quantity) as (short_or_long, quantity)")
    sec_occ_7_1_df = sec_occ_7_1_df.withColumn('quantity', coalesce(sec_occ_7_1_df.quantity, lit(0)))\
                                    .withColumn('occ_strkprice_report', sec_occ_7_1_df.occ_strkprice_report.cast(DecimalType(30, 6)))
    sec_occ_7_2_df = sec_occ_7_1_df.filter("quantity <> 0")    # underlying_sec_name
    sec_occ_7_2_df.createOrReplaceTempView(f'qa_{table_name}_7_2_full_vw')
    print(f'View created: qa_{table_name}_7_2_full_vw')
    print("sec_occ_7_2_df count: {0}".format(sec_occ_7_2_df.count()))
    sec_occ_7_1_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 7.3
    sec_occ_7_3_df = sec_occ_7_2_df.filter("occ_qty_reported == 0")\
                                    .select('bor_client_id', 'option_type', 'underlying_symbol', 'short_or_long')
    sec_occ_7_3_df = sec_occ_7_3_df.groupBy('bor_client_id', 'option_type', 'underlying_symbol')\
                                    .agg(countDistinct('short_or_long').alias('short_or_long_count'))\
                                    .filter('short_or_long_count > 1')
    sec_occ_7_3_df = sec_occ_7_3_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol')
    sec_occ_7_3_df.createOrReplaceTempView(f'qa_{table_name}_7_3_full_vw')
    print(f'View created: qa_{table_name}_7_3_full_vw')
    print("sec_occ_7_3_df count: {0}".format(sec_occ_7_3_df.count()))
    sec_occ_7_3_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()

    # ----------

    # Step 7.4
    so_7_3_df = sec_occ_7_3_df.select(sec_occ_7_3_df.bor_client_id.alias('so73_bor_client_id'),
                                        sec_occ_7_3_df.option_type.alias('so73_option_type'),
                                        sec_occ_7_3_df.underlying_symbol.alias('so73_underlying_symbol'),
                                        sec_occ_7_3_df.short_or_long_count.alias('so73_short_or_long_count'))
    sec_occ_7_4_df = so_7_3_df.join(sec_occ_6_2_df, (coalesce(sec_occ_6_2_df.bor_client_id, lit('')) == coalesce(so_7_3_df.so73_bor_client_id, lit('')))
                                                    & (coalesce(sec_occ_6_2_df.option_type, lit('')) == coalesce(so_7_3_df.so73_option_type, lit('')))
                                                    & (coalesce(sec_occ_6_2_df.underlying_symbol, lit('')) == coalesce(so_7_3_df.so73_underlying_symbol, lit(''))),
                                            'inner')
    sec_occ_7_4_df = sec_occ_7_4_df.select('row_id', 'bor_client_id', 'option_type', 'underlying_symbol', 'underlying', 
                                            'long_option_maturitydate', 'short_option_maturitydate',
                                            coalesce(sec_occ_7_4_df.short_quantity, lit(0)).alias('short_quantity'),
                                            coalesce(sec_occ_7_4_df.long_quantity, lit(0)).alias('long_quantity'),
                                            'strikepriceamount', 'occ_qty_reported', 'occ_strkprice_report', 'underlying_sec_name')
    sec_occ_7_4_df.createOrReplaceTempView(f'qa_{table_name}_7_4_full_vw')
    print(f'View created: qa_{table_name}_7_4_full_vw')
    print("sec_occ_7_4_df count: {0}".format(sec_occ_7_4_df.count()))
    sec_occ_7_4_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 8
    # bit of an overkill but keeping it as it is, in case of more analysis is needed
    if order_type == 'ordered':
        # ordered
        windowSpec_rnm = Window.partitionBy('bor_client_id', 'option_type', 'underlying_symbol')\
                            .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'long_option_maturitydate', 'strikepriceamount')
    elif order_type == 'ordered_alterix':
        # ordered alterix
        windowSpec_rnm = Window.partitionBy('bor_client_id', 'option_type', 'underlying_symbol')\
                            .orderBy('bor_client_id', 'option_type', 'underlying_symbol', col('long_option_maturitydate').desc(), coalesce(col('row_id'), lit(max_length)))
    elif order_type == 'unordered':
        # unordered
        windowSpec_rnm = Window.partitionBy('bor_client_id', 'option_type', 'underlying_symbol')\
                            .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'long_option_maturitydate')
    # actual calculation
    sec_occ_8_df = sec_occ_7_4_df.withColumn('row_seqno', row_number().over(windowSpec_rnm))

    windowSpec= Window.partitionBy('bor_client_id', 'option_type', 'underlying_symbol')\
                        .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'long_option_maturitydate', 'row_seqno')
    # actual calculation
    sec_occ_8_df = sec_occ_8_df.withColumn('short_balance_init', sum(coalesce(sec_occ_7_4_df.long_quantity, lit(0)) + coalesce(sec_occ_7_4_df.short_quantity, lit(0))).over(windowSpec))
    sec_occ_8_df = sec_occ_8_df.withColumn('short_balance_max', max(sec_occ_8_df.short_balance_init).over(windowSpec))
    sec_occ_8_df = sec_occ_8_df.withColumn('short_balance', sec_occ_8_df.short_balance_init - when(sec_occ_8_df.short_balance_max > 0, sec_occ_8_df.short_balance_max).otherwise(0))\
                                .withColumn('short_balance_prev', coalesce(lag('short_balance', 1).over(windowSpec), lit(0)))

    # writing it to a file
    try:
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}_step8")
        sec_occ_8_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}_step8', path=f'{cz_base_path}/rna_qa/qa_{table_name}_step8')
    except:
        dbutilspkg.fs.rm(f'{cz_base_path}/rna_qa/qa_{table_name}_step8',True)
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}_step8")
        sec_occ_8_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}_step8', path=f'{cz_base_path}/rna_qa/qa_{table_name}_step8')
    sec_occ_8_df = spark.sql(f'select * from wdar_qa.qa_{table_name}_step8')

    sec_occ_8_df.createOrReplaceTempView(f'qa_{table_name}_8_full_vw')
    print(f'View created: qa_{table_name}_8_full_vw')
    print("sec_occ_8_df count: {0}".format(sec_occ_8_df.count()))
    sec_occ_8_df.select('bor_client_id', 'option_type', 'underlying_symbol', 'long_option_maturitydate', 
                        'short_quantity', 'long_quantity', 'short_balance', 'short_balance_prev')\
                .orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 9
    windowSpec = Window.partitionBy('bor_client_id', 'option_type', 'underlying_symbol')\
                        .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'long_option_maturitydate', 'strikepriceamount')
    # occ_qty_reported calculation
    sec_occ_9_df = sec_occ_8_df.withColumn('short_balance_calc', sec_occ_8_df.short_balance_prev+sec_occ_8_df.short_quantity+sec_occ_8_df.long_quantity)
    sec_occ_9_df = sec_occ_9_df.withColumn('occ_qty_reported', when((sec_occ_9_df.short_balance_calc<0) & (sec_occ_9_df.long_quantity>0), sec_occ_9_df.long_quantity)
                                                                .when((sec_occ_9_df.short_balance_calc<0) & (sec_occ_9_df.long_quantity<=0), lit(0))
                                                                .when((sec_occ_9_df.short_balance_calc>0) & (sec_occ_9_df.long_quantity>0),
                                                                        when(sec_occ_9_df.short_balance_prev==0, sec_occ_9_df.short_quantity*-1)
                                                                                .otherwise(sec_occ_9_df.short_balance_prev*-1))
                                                                .when((sec_occ_9_df.short_balance_calc>0) & (sec_occ_9_df.long_quantity<=0), 0)
                                                                .otherwise(sec_occ_9_df.short_balance_prev*-1))
    # strikepriceamount calculation
    sec_occ_9_df = sec_occ_9_df.withColumn('occ_strkprice_report', when(sec_occ_9_df.occ_qty_reported>0, sec_occ_9_df.strikepriceamount).otherwise(lit('')))\
                                .filter('occ_qty_reported>0')

    sec_occ_9_df.createOrReplaceTempView(f'qa_{table_name}_9_full_vw')
    print(f'View created: qa_{table_name}_9_full_vw')
    print("sec_occ_9_df count: {0}".format(sec_occ_9_df.count()))
    sec_occ_9_df.select('bor_client_id', 'option_type', 'underlying_symbol', 'long_option_maturitydate', 
                        'short_quantity', 'long_quantity', 'short_balance', 'short_balance_calc',
                        'short_balance_prev', 'occ_qty_reported', 'occ_strkprice_report',
                        'strikepriceamount', 'underlying_sec_name')\
                .orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    # Step 10
    so_6_1_df = sec_occ_6_1_df.select('bor_client_id', 'option_type', 'underlying_symbol', 'underlying', 'long_option_maturitydate', 
                                        'short_option_maturitydate', 'long_quantity', 'short_quantity', 'strikepriceamount',
                                        'occ_qty_reported', 'occ_strkprice_report')\
                                .withColumn('source', lit('step6.1'))

    so_9_df = sec_occ_9_df.select('bor_client_id', 'option_type', 'underlying_symbol', 'underlying', 'long_option_maturitydate', 
                                    'short_option_maturitydate', 'long_quantity', 'short_quantity', 'strikepriceamount',
                                    'occ_qty_reported', 'occ_strkprice_report')\
                                .withColumn('source', lit('step9'))
    sec_occ_10_df = so_6_1_df.union(so_9_df)
    # mapping
    sec_occ_10_df = sec_occ_10_df.withColumnRenamed('underlying_symbol', 'symbol')\
                                .withColumn('underlying_sec_name', lit(''))\
                                .withColumn('expiry_maturity_date', sec_occ_10_df.long_option_maturitydate)\
                                .withColumn('mmy', regexp_replace(sec_occ_10_df.long_option_maturitydate,'-', ''))\
                                .withColumn('series_contract_date', date_format(sec_occ_10_df.long_option_maturitydate, 'MMddyyyy'))\
                                .withColumn('p_c', substring(col('option_type'), 1, 1))\
                                .withColumn('txntyp', lit('4'))\
                                .withColumn('actn', lit('1'))\
                                .withColumn('id_clr', lit('00615'))\
                                .withColumn('id_acct', lit('C'))\
                                .withColumn('typ', lit('26'))\
                                .withColumn('txt', lit('Spreads'))\
                                .withColumn('cfi', concat(lit('O'), substring(col('option_type'), 1, 1), lit('XXXX')))\
                                .withColumn('typ_ias', lit('IAS'))\
                                .withColumn('process_date', lit(f'{process_date}'))\
                                .withColumn('run_date', lit(run_date_val))\
                                .withColumn('occ_strkprice_report', sec_occ_10_df.occ_strkprice_report.cast(DecimalType(30, 6)).alias('occ_strkprice_report'))\
                                .select('bor_client_id', 'underlying', 'symbol', 'underlying_sec_name', 'option_type', 
                                        'expiry_maturity_date', 'occ_strkprice_report', 'occ_qty_reported', 'mmy',
                                        'series_contract_date', 'p_c', 'txntyp', 'actn', 'id_clr', 'id_acct', 'typ',
                                        'txt', 'cfi', 'typ_ias', 'process_date', 'run_date')
    sec_occ_10_df.createOrReplaceTempView(f'qa_{table_name}_10_full_vw')
    print(f'View created: qa_{table_name}_10_full_vw')
    print("sec_occ_10_df count: {0}".format(sec_occ_10_df.count()))
    sec_occ_10_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()


    # COMMAND ----------

    # ----------

    src_df = sec_occ_10_df # sec_occ_3_2_df, sec_occ_3_4_df, sec_occ_4_df, sec_occ_5_df, sec_occ_6_1_df, sec_occ_6_2_df, sec_occ_7_2_df, sec_occ_7_4_df, sec_occ_8_df, sec_occ_9_df
    src_df = src_df.withColumn('occ_strkprice_report', col('occ_strkprice_report').cast(StringType()))
    order_spec = Window.partitionBy('bor_client_id', 'option_type', 'symbol')\
                        .orderBy('bor_client_id', 'option_type', 'symbol', 'underlying', 'expiry_maturity_date', 'occ_qty_reported', 'occ_strkprice_report')    #10
    # order_spec = Window.partitionBy('bor_client_id', 'option_type', 'underlying_symbol')\
                        # .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'underlying', 'long_option_maturitydate', 'short_option_maturitydate')    #4, 5, 6.1, 6.2, 7.4, 8, 9
                        # .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'underlying', 'long_option_maturitydate', 'short_option_maturitydate', 'quantity')    #7.2
                        # .orderBy('bor_client_id', 'option_type', 'underlying_symbol', 'underlying', 'option_maturitydate')  #3.2, 3.4
    src_df = src_df.withColumn('rnm', row_number().over(order_spec))
    # adding qa_ prefix to column names
    for col_name in src_df.columns:
        src_df = src_df.withColumnRenamed(col_name, f'qa_{col_name}')

    # ----------

    # creating a file/table
    try:
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}_final")
        src_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}_final', path=f'{src_table_cz_path}')
        # only used for analysis
        # spark.sql(f"drop table if exists wdar_qa.qa_{table_name}{as_of_date[4:]}{order_type}_final")
        # src_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}{as_of_date[4:]}{order_type}_final', path=f'{cz_base_path}/rna_qa/qa_{table_name}{as_of_date[4:]}{order_type}_final')
    except:
        dbutilspkg.fs.rm(f'{src_table_cz_path}',True)
        spark.sql(f"drop table if exists wdar_qa.qa_{table_name}_final")
        src_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}_final', path=f'{src_table_cz_path}')
        # only used for analysis
        # dbutilspkg.fs.rm(f'{cz_base_path}/rna_qa/qa_{table_name}{as_of_date[4:]}{order_type}_final',True)
        # spark.sql(f"drop table if exists wdar_qa.qa_{table_name}{as_of_date[4:]}{order_type}_final")
        # src_df.write.format('delta').saveAsTable(f'wdar_qa.qa_{table_name}{as_of_date[4:]}{order_type}_final', path=f'{cz_base_path}/rna_qa/qa_{table_name}{as_of_date[4:]}{order_type}_final')
    print(f"Have overwriten the new records to wdar_qa.qa_{table_name}_final.")
    # creating a view
    src_df.createOrReplaceGlobalTempView('temp_qa_{0}_final'.format(table_name))
    print(f"Have overwriten the new records to global temp view temp_qa_{table_name}_final.")
    spark.sql(f"select count(*) from wdar_qa.qa_{table_name}_final").show()


    # COMMAND ----------

    # ----------

    # Tgt part
    tgt_df = spark.read.csv(f"dbfs:/FileStore/tables/DaaSWealth_QA/Mainkar/Config/data/reporting_occ_cloud_{as_of_date_wca}.csv", header='True')
    tgt_df = tgt_df.withColumnRenamed('Strkpx', 'occ_strkprice_report')

    order_spec = Window.partitionBy('bor_client_id', 'option_type', 'symbol')\
                        .orderBy('bor_client_id', 'option_type', 'symbol', 'expiry_maturity_date', 'occ_qty_reported', 'occ_strkprice_report')    #10
    tgt_df = tgt_df.withColumn('rnm', row_number().over(order_spec))

    # changing columns to lower case
    for col_name in tgt_df.columns:
        tgt_df = tgt_df.withColumnRenamed(col_name, col_name.lower())
        
    print("tgt_df count: {0}".format(tgt_df.count()))
    tgt_df.createOrReplaceGlobalTempView('temp_dev_{0}_final'.format(table_name))
    tgt_df.orderBy('bor_client_id', 'option_type', 'symbol').show()
    # tgt_df.orderBy('bor_client_id', 'option_type', 'underlying_symbol').show()    #1-9
    # pk_col = 'bor_client_id,option_type,underlying_symbol,rnm'    #1-9
    # conf = dates_needed.add_aditional_parameters(config_file_path, pk_col=pk_col)    #1-9


    # COMMAND ----------

    # ----------

    pass_fail_result_summary_part.execute(table_name, 'sub_script', config_file_path, pk_col, env, pod_name, file_view_flag='view', dbfs_folder_base_path=dbfs_folder_base_path, 
                                            passed_as_of_date=passed_as_of_date, extract_prep_flag=extract_prep_flag)

    # ----------

    target_table_unittest.execute(table_name, 'sub_script', config_file_path, env, pod_name, file_view_flag='view', dbfs_folder_base_path=dbfs_folder_base_path, 
                                    passed_as_of_date=passed_as_of_date, extract_prep_flag=extract_prep_flag)


    # COMMAND ----------

    # ----------

    print("Pass fail Result:")
    spark.sql(f"select * from global_temp.temp_{table_name}_pass_fail").display()
    print('Regression result:')
    spark.sql(f'''select type,table_name,column_name,overall_Status,
                    src_count,tgt_count,pass_count,fail_count,null_count,
                    src_pass_percentage,tgt_pass_percentage,
                    tgt_null_percentage,process_date,last_insert_timestamp 
                from global_temp.temp_{table_name}_regression_result_summary''').display()
    print('Unittest result:')
    spark.sql(f'''select type,table_name,column_name,overall_Status,
                    src_count,tgt_count,pass_count,fail_count,null_count,
                    src_pass_percentage,tgt_pass_percentage,
                    tgt_null_percentage,process_date,last_insert_timestamp
                from global_temp.temp_{table_name}_unittest_result_summary''').display()


# COMMAND ----------

# running the python file
if __name__ == '__main__':
    import sys
    if len(sys.argv)==8:
        execute(table_name=sys.argv[1], script_type=sys.argv[2], config_file_path=sys.argv[3], env=sys.argv[4], pod_name=sys.argv[5], dbfs_folder_base_path=sys.argv[6], passed_as_of_date=sys.argv[7])
    else:
        print("""This scrip needs 6 arguments to be passed you didnt pass all of them. 
        So running with default arguments.
        Arguments needed: table_name, script_type, config_file_path, env, pod_name, dbfs_folder_base_path""")
        execute(table_name='reporting_occ', script_type='super_script', config_file_path='', env='dev', pod_name='dc',
                dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Mainkar/', passed_as_of_date='20240108',
                extract_prep_flag='prep')
