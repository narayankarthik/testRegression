# Databricks notebook source
import os
import re
import csv
import json
from datetime import datetime,timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from dateutil.relativedelta import relativedelta

# COMMAND ----------
app_name = "dates_needed"
spark = SparkSession.builder.appName(app_name).getOrCreate()

def create_config_file(passed_as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix,
                       pod_name='', process_flag='reg', synapse_suffix='', extract_prep_flag='prep', table_suffix=''):
    print('\nAll parameters passed to create_config_file')
    print(f'passed_as_of_date:{passed_as_of_date}\nconfig_base_path:{config_base_path}\ntable_name:{table_name}')
    print(f'db_names:{db_names}\nenv:{env}\nprevdate_value:{prevdate_value}\npod_name{pod_name}')
    print(f'process_flag{process_flag}\nuser_prefix:{user_prefix}')
    print(f'synapse_suffix:{synapse_suffix}')

    as_of_date = passed_as_of_date
    file_name = f'config_file_{user_prefix}.yaml'
    config_path = config_base_path+'/'+file_name

    parameter_dict = {'as_of_date':as_of_date}

    create_config_file_inner(as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix, pod_name, synapse_suffix, config_path, parameter_dict, extract_prep_flag, table_suffix)
    files_created = config_path

    print(f'Config file(s) created: {files_created}')
    return files_created

# COMMAND ----------

def create_config_file_inner(as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix,
                             pod_name, synapse_suffix, config_path, parameter_dict, extract_prep_flag, table_suffix):
    print('\nAll parameters passed to create_config_file_inner')
    print(f'passed_as_of_date:{as_of_date}\nconfig_base_path:{config_base_path}\ntable_name:{table_name}')
    print(f'db_names:{db_names}\nenv:{env}\nprevdate_value:{prevdate_value}\nsynapse_suffix:{synapse_suffix}')

    # prep_table_info_file_suffix=f'_{pod_name}' if pod_name != '' else ''
    # prep_table_info_file_name = f'{extract_prep_flag}_table_info{prep_table_info_file_suffix}.json'
    # prep_table_info_file = config_base_path + '\\' + prep_table_info_file_name
    # prep_config_key_flag = 0

    config_file = open(config_path, 'w')

    print(f'env={env}\nuser_prefix={user_prefix}')
    config_file.write(f'env={env}\nuser_prefix={user_prefix}\nsynapse_suffix={synapse_suffix}\n')
    if table_suffix != '':
        config_file.write(f'table_suffix={table_suffix}\n')

    # Adding all the parameter we got from create_config_file_inner.parameter_dict
    for k,v in parameter_dict.items():
        print(f'{k}={v}')
        config_file.write(f'{k}={v}\n')

    process_date = datetime.strptime(as_of_date, "%Y%m%d").strftime("%Y-%m-%d")
    podium_delivery_date = as_of_date+'000000'
    as_of_date_wca = datetime.strptime(as_of_date, "%Y%m%d") + timedelta(days=1)
    as_of_date_wca = str(datetime.strptime(str(as_of_date_wca)[:10], "%Y-%m-%d").strftime("%Y%m%d"))
    process_date_wca = str(datetime.strptime(str(as_of_date_wca)[:10], "%Y%m%d").strftime("%Y-%m-%d"))
    podium_delivery_date_wca = as_of_date_wca + '000000'
    as_of_date_currmonth_firstday = str(datetime.strptime(as_of_date, "%Y%m%d").replace(day=1).strftime("%Y%m%d"))
    process_date_currmonth_firstday = datetime.strptime(as_of_date_currmonth_firstday[:10], "%Y%m%d").strftime("%Y-%m-%d")
    podium_delivery_date_currmonth_firstday = as_of_date_currmonth_firstday + '000000'
    as_of_date_prevmonth_lastday = str((datetime.strptime(as_of_date_currmonth_firstday[:10], "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d"))
    process_date_prevmonth_lastday = datetime.strptime(as_of_date_prevmonth_lastday[:10], "%Y%m%d").strftime("%Y-%m-%d")
    podium_delivery_date_prevmonth_lastday = as_of_date_prevmonth_lastday + '000000'
    as_of_date_twoprevmonth_firstday = str((datetime.strptime(as_of_date, "%Y%m%d") - relativedelta(months=2)).replace(day=1).strftime("%Y%m%d"))
    process_date_twoprevmonth_firstday = datetime.strptime(as_of_date_twoprevmonth_firstday[:10], "%Y%m%d").strftime("%Y-%m-%d")
    podium_delivery_date_twoprevmonth_firstday = as_of_date_twoprevmonth_firstday + '000000'

    print(f'process_date={process_date}\npodium_delivery_date={podium_delivery_date}\nas_of_date_wca={as_of_date_wca}')
    print(f'process_date_wca={process_date_wca}\npodium_delivery_date_wca={podium_delivery_date_wca}')
    print(f'as_of_date_currmonth_firstday={as_of_date_currmonth_firstday}\nprocess_date_currmonth_firstday={process_date_currmonth_firstday}')
    print(f'podium_delivery_date_currmonth_firstday={podium_delivery_date_currmonth_firstday}\nas_of_date_prevmonth_lastday={as_of_date_prevmonth_lastday}')
    print(f'process_date_prevmonth_lastday={process_date_prevmonth_lastday}\npodium_delivery_date_prevmonth_lastday={podium_delivery_date_prevmonth_lastday}')
    print(f'as_of_date_twoprevmonth_firstday={as_of_date_twoprevmonth_firstday}\nprocess_date_twoprevmonth_firstday={process_date_twoprevmonth_firstday}')
    print(f'podium_delivery_date_twoprevmonth_firstday={podium_delivery_date_twoprevmonth_firstday}')
    config_file.write(f'process_date={process_date}\npodium_delivery_date={podium_delivery_date}\nas_of_date_wca={as_of_date_wca}\n')
    config_file.write(f'process_date_wca={process_date_wca}\npodium_delivery_date_wca={podium_delivery_date_wca}\n')
    config_file.write(f'as_of_date_currmonth_firstday={as_of_date_currmonth_firstday}\nprocess_date_currmonth_firstday={process_date_currmonth_firstday}\n')
    config_file.write(f'podium_delivery_date_currmonth_firstday={podium_delivery_date_currmonth_firstday}\nas_of_date_prevmonth_lastday={as_of_date_prevmonth_lastday}\n')
    config_file.write(f'process_date_prevmonth_lastday={process_date_prevmonth_lastday}\npodium_delivery_date_prevmonth_lastday={podium_delivery_date_prevmonth_lastday}\n')
    config_file.write(f'as_of_date_twoprevmonth_firstday={as_of_date_twoprevmonth_firstday}\nprocess_date_twoprevmonth_firstday={process_date_twoprevmonth_firstday}\n')
    config_file.write(f'podium_delivery_date_twoprevmonth_firstday={podium_delivery_date_twoprevmonth_firstday}\n')

    #Get Previous date
    prev_dates(config_file, prevdate_value, as_of_date)

    # Adding parameters from prep_table_info.json if it exists
    extract_config_data(config_file, config_base_path, table_name, pod_name, extract_prep_flag, table_suffix, synapse_suffix)

    config_file.close()

# COMMAND ----------

def extract_config_data(config_file, config_base_path, table_name, pod_name, extract_prep_flag, table_suffix='', synapse_suffix=''):
    prep_table_info_file_suffix = f'_{pod_name}' if pod_name != '' else ''
    prep_table_info_file_name = f'{extract_prep_flag}_table_info{prep_table_info_file_suffix}.json'
    prep_table_info_file = config_base_path + '\\' + prep_table_info_file_name
    prep_config_key_flag = 0
    config_value_str = ''
    config_value_dict = dict()

    # Adding parameters from prep_table_info.json if it exists
    prep_info_params = dict()
    if (os.path.exists(prep_table_info_file)):
        with open(prep_table_info_file, 'r') as prep_config_file:
            for prep_table_info in json.load(prep_config_file):
                if prep_table_info['table_name'] == f'{table_name}{table_suffix}{synapse_suffix}':
                    prep_info_params = prep_table_info
                    prep_config_key_flag = 1
                    break
            if prep_config_key_flag == 0:
                print(f'No record for {table_name}{synapse_suffix} present in the preparator config file: {prep_table_info_file}')
        for cols_key, cols_value in prep_info_params.items():
            if cols_key == 'table_name':
                continue
            config_value_str = config_value_str+'{}={}\n'.format(cols_key, cols_value)
            config_value_dict[cols_key] = cols_value
            # print('{}={}'.format(cols_key, cols_value))
            # config_file.write('{}={}\n'.format(cols_key, cols_value))
    else:
        print(f'Preparator config file not found:{prep_table_info_file}')

    if hasattr(config_file,'write'):
        config_file.write(config_value_str)
    else:
        print(config_value_dict)
        add_aditional_parameters(config_file, **config_value_dict)
    print()

# COMMAND ----------

def prev_dates(config_file, prevdate_value, as_of_date):
    print('\nAll parameters passed to prev_dates')
    print(f'config_file:{config_file}\nprevdate_value:{prevdate_value}\nas_of_date:{as_of_date}')

    day_of_the_week = datetime.strptime(as_of_date, "%Y%m%d").strftime("%A")

    # When previous date(process_date or as_of_date format) value is passed
    if (prevdate_value is not None) and (prevdate_value not in ["weekdays", "alldays"]):
        if prevdate_value != '':
            try:    #if date has process_date format
                previous_process_date = str(datetime.strptime(prevdate_value,"%Y-%m-%d").strftime("%Y-%m-%d"))
            except:    #if date has as_of_date format
                previous_process_date =  str(datetime.strptime(prevdate_value, "%Y%m%d").strftime("%Y-%m-%d"))
            previous_as_of_date = str(datetime.strptime(previous_process_date, "%Y-%m-%d").strftime("%Y%m%d"))
            previous_podium_delivery_date = previous_as_of_date + '000000'
            previous_as_of_date_wca = datetime.strptime(previous_as_of_date, "%Y%m%d") + timedelta(days=1)
            previous_as_of_date_wca = str(datetime.strptime(str(previous_as_of_date_wca)[:10], "%Y-%m-%d").strftime("%Y%m%d"))
            previous_process_date_wca = str(datetime.strptime(str(previous_as_of_date_wca)[:10], "%Y%m%d").strftime("%Y-%m-%d"))
            previous_podium_delivery_date_wca = previous_as_of_date_wca + '000000'
        elif prevdate_value == '':
            print(f"As prevdate_value is '{prevdate_value}', so setting ''(empty) as the default value for previous dates")
            previous_process_date = ''
            previous_as_of_date = ''
            previous_podium_delivery_date = ''
            previous_as_of_date_wca = ''
            previous_process_date_wca = ''
            previous_podium_delivery_date_wca = ''
    # When previous date value is NOT passed or is "weekdays"/"alldays"
    else:
        prevdate_value = (prevdate_value or '') #Converting None to ''(empty)
        # consider only weekdays
        if prevdate_value.lower() == "weekdays":
            # print('day_of_the_week:{}'.format(day_of_the_week))
            if day_of_the_week != 'Monday':
                previous_process_date = str(datetime.strptime(as_of_date, "%Y%m%d") + timedelta(days=-1))[:10]
            else:
                previous_process_date = str(datetime.strptime(as_of_date, "%Y%m%d") + timedelta(days=-3))[:10]
        # consider all the days
        else:
            previous_process_date = str(datetime.strptime(as_of_date, "%Y%m%d") + timedelta(days=-1))[:10]
        previous_as_of_date = str(datetime.strptime(previous_process_date, "%Y-%m-%d").strftime("%Y%m%d"))
        previous_podium_delivery_date = previous_as_of_date + '000000'
        previous_as_of_date_wca = datetime.strptime(previous_as_of_date, "%Y%m%d") + timedelta(days=1)
        previous_as_of_date_wca = str(datetime.strptime(str(previous_as_of_date_wca)[:10], "%Y-%m-%d").strftime("%Y%m%d"))
        previous_process_date_wca = str(datetime.strptime(str(previous_as_of_date_wca)[:10], "%Y%m%d").strftime("%Y-%m-%d"))
        previous_podium_delivery_date_wca = previous_as_of_date_wca + '000000'
    print(f'previous_process_date={previous_process_date}\nprevious_as_of_date={previous_as_of_date}')
    print(f'previous_podium_delivery_date={previous_podium_delivery_date}\nprevious_process_date_wca={previous_process_date_wca}')
    print(f'previous_as_of_date_wca={previous_as_of_date_wca}\nprevious_podium_delivery_date_wca={previous_podium_delivery_date_wca}')

    if hasattr(config_file,'write'):
        config_file.write(f'previous_process_date={previous_process_date}\nprevious_as_of_date={previous_as_of_date}\n')
        config_file.write(f'previous_podium_delivery_date={previous_podium_delivery_date}\nprevious_process_date_wca={previous_process_date_wca}\n')
        config_file.write(f'previous_as_of_date_wca={previous_as_of_date_wca}\nprevious_podium_delivery_date_wca={previous_podium_delivery_date_wca}\n')
    else:
        add_aditional_parameters(config_file, previous_process_date=previous_process_date,previous_as_of_date=previous_as_of_date, previous_podium_delivery_date=previous_podium_delivery_date, previous_process_date_wca=previous_process_date_wca, previous_as_of_date_wca=previous_as_of_date_wca, previous_podium_delivery_date_wca=previous_podium_delivery_date_wca)
    print()

# COMMAND ----------

def all_parameters_needed(passed_as_of_date, config_base_path, table_name, db_names, env, user_prefix, pod_name='',
                            prevdate_value=None, process_flag='reg', synapse_suffix='', extract_prep_flag='prep', table_suffix=''):
    """

    Args:
        passed_as_of_date:
        config_base_path:
        table_name:
        db_names:
        env:
        user_prefix:
        pod_name:
        prevdate_value: None(default)/date(process_date or as_of_date)/"weekdays"/"alldays"
        process_flag:
        determination_date_df:
        determination_date_day0_df:
        synapse_suffix:
        extract_prep_flag:
        table_suffix:
    Returns:

    """
    config_base_path = os.path.abspath(config_base_path)
    print('\nAll parameters passed to all_parameters_needed')
    print('passed_as_of_date:{}\nconfig_base_path:{}\ntable_name:{}'.format(passed_as_of_date, config_base_path, table_name))
    print('db_names:{}\nenv:{}\nuser_prefix:{}\npod_name:{}'.format(db_names, env, user_prefix, pod_name))
    print('prevdate_value:{}\nprocess_flag:{}\n'.format(prevdate_value, process_flag))
    parameters_path = config_base_path+'/parameters.txt'
    files_created = create_config_file(passed_as_of_date, config_base_path, table_name, db_names, env, prevdate_value,
                                       user_prefix, pod_name, process_flag, synapse_suffix, extract_prep_flag, table_suffix)
    return files_created

# COMMAND ----------

def add_aditional_parameters(config_path, **kwargs):
    print('\nAll parameters passed to add_aditional_parameters')
    print(f'config_path:{config_path}\nkwargs:{kwargs}')

    # read existing content
    conf_dict = dict()
    with open(config_path, 'r') as file_in:
        for line in file_in:
            line = line[:-1].split('=')
            conf_dict[line[0]] = line[1].replace('\r', '')

    #Add new parameters to the existing once
    for name, value in kwargs.items():
        conf_dict[name] = value
    print(f"Content of the new file will be:\n{conf_dict}")

    #Write all the parameters back to the file
    with open(config_path, 'w') as file_out:
        for name, value in conf_dict.items():
            file_out.write(f'{name}={value}\n')
    print('Config file modified: {config_path}'.format(config_path=config_path))
    return conf_dict

# COMMAND ----------

# determination_date_df = spark.sql(f"""select * 
#                                         from delta.`abfss://wcz0003@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0003/test_src/cdic_determination_date/current/`
#                                         where ifw_effective_date = '20201130000000'""")
# determination_date_df.show()

# determination_date_day0_df = spark.sql(f"""select * 
#                                             from delta.`abfss://wcz0003@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0003/test_src/cdic_determination_date/current/`
#                                             where ifw_effective_date < '20201130000000'
#                                                 and run_day_indicator='0' 
#                                             order by ifw_effective_date desc 
#                                             limit 1""")
# determination_date_day0_df.show()

# passed_as_of_date = '20201130'
# config_base_path = '/dbfs/FileStore/tables/DaaSWealth_QA/Main/Config'
# table_name = 'cdic_fid_t0110'
# db_names = {'cdic': ['wcz0003', 'curated/wcz0003/test_src', 'edaaawcze1devcz', 'cdic'], 'cif': ['cif', 'CIF', 'edaaaedle1devsrz', 'cif'], 'odr': ['wcz0003', 'curated/wcz0003/test_src', 'edaaawcze1devcz', 'odr'], 'ossbr': ['ossbr', 'OSSBR', 'edaaaedle1devsrz', 'ossbr'], 'ptsql': ['ptsql', 'PTSQL', 'edaaaedle1devsrz', 'ptsql'], 'upg': ['upg', 'UPG', 'edaaaedle1devsrz', 'upg'], 'wca': ['wca', 'WCA', 'edaaaedle1devsrz', 'wca'], 'wdlob': ['wcz0003', 'curated/wcz0003/test_src', 'edaaawcze1devcz', 'wdlob'], 'wdprm': ['wdprm', 'WDPRM', 'edaaaedle1devsrz', 'wdprm'], 'wds': ['wds', 'WDS', 'edaaaedle1devsrz', 'wds'], 'wdsm': ['wdsm', 'WDSM', 'edaaaedle1devsrz', 'wdsm'], 'w360': ['w360', 'W360', 'edaaaedle1devsrz', 'w360']}
# env = 'dev'
# prevdate_value = ''
# user_prefix = 'kar'
# pod_name = 'ep1'
# process_flag = 'hod'

# all_parameters_needed(passed_as_of_date, config_base_path, table_name, db_names, env, user_prefix, pod_name, prevdate_value, process_flag, determination_date_df, determination_date_day0_df)