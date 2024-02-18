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

def create_config_file(passed_as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix, pod_name='', process_flag='reg', parameters_path='', 
                        determination_date_day0_df=spark.createDataFrame([], StructType([])), synapse_suffix='', extract_prep_flag='prep', table_suffix=''):
    print('\nAll parameters passed to create_config_file')
    print(f'passed_as_of_date:{passed_as_of_date}\nconfig_base_path:{config_base_path}\ntable_name:{table_name}')
    print(f'db_names:{db_names}\nenv:{env}\nprevdate_value:{prevdate_value}\npod_name{pod_name}')
    print(f'process_flag{process_flag}\nparameters_path:{parameters_path}\nuser_prefix:{user_prefix}')
    print(f'synapse_suffix:{synapse_suffix}')

    files_created = dict()

    parameter_dict = {}
    if process_flag == 'hod':
        with open(parameters_path, 'r') as parameters_file:
            for parameters_records in parameters_file:
                parameters_records = parameters_records[:-1].split('|')
                print(parameters_records)
                as_of_date = parameters_records[0]
                process_indicator = parameters_records[1]
                run_day = parameters_records[2]
                target_process_date = parameters_records[3]

                passed_process_date = datetime.strptime(passed_as_of_date, "%Y%m%d").strftime("%Y-%m-%d")
                passed_podium_delivery_date = passed_as_of_date + '000000'

                file_name = f'config_file_process_{process_indicator}_{user_prefix}.yaml'
                config_path = config_base_path+'/'+file_name

                #get coresponding day 0 date(used in cdic_fid_t0800)
                if (determination_date_day0_df.head() is None):
                    print('Either cdic_determination_date table dosent have any record or it is not a holds table, so running daily BAU process')
                    day0_determination_date = ''
                    day0_passed_podium_delivery_date = ''
                    day0_passed_as_of_date = ''
                    day0_passed_process_date = ''
                else:
                    print('cdic_determination_date table has record for this date so calculating all the other parameters')
                    determination_date_day0_collection = determination_date_day0_df.collect()[0]
                    day0_determination_date = determination_date_day0_collection.determination_date
                    day0_passed_podium_delivery_date=determination_date_day0_collection.ifw_effective_date
                    day0_passed_as_of_date=day0_passed_podium_delivery_date[:8]
                    day0_passed_process_date = str(datetime.strptime(str(day0_passed_as_of_date)[:10], "%Y%m%d").strftime("%Y-%m-%d"))

                if ((process_indicator == 'T') or (process_indicator == 'P')) and (run_day == '1'):
                    day0_target_process_date = day0_passed_process_date + day0_determination_date[4:]
                else:
                    day0_target_process_date = ''

                parameter_dict = {'as_of_date':as_of_date, 'process_indicator':process_indicator, 'run_day':run_day, 'target_process_date':target_process_date, 
                                    'passed_as_of_date':passed_as_of_date, 'passed_process_date':passed_process_date, 'passed_podium_delivery_date':passed_podium_delivery_date,
                                    'day0_determination_date':day0_determination_date, 'day0_passed_podium_delivery_date':day0_passed_podium_delivery_date,
                                    'day0_passed_as_of_date':day0_passed_as_of_date, 'day0_passed_process_date':day0_passed_process_date,
                                    'day0_target_process_date':day0_target_process_date}

                create_config_file_inner(as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix, pod_name, synapse_suffix, config_path, parameter_dict, extract_prep_flag, table_suffix)
                files_created[process_indicator]=config_path
    else:
        as_of_date = passed_as_of_date
        file_name = f'config_file_{user_prefix}.yaml'
        config_path = config_base_path+'/'+file_name

        parameter_dict = {'as_of_date':as_of_date}

        create_config_file_inner(as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix, pod_name, synapse_suffix, config_path, parameter_dict, extract_prep_flag, table_suffix)
        files_created = config_path

    print(f'Config file(s) created: {files_created}')
    return files_created

# COMMAND ----------

def create_config_file_inner(as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix, pod_name, synapse_suffix, config_path, parameter_dict, extract_prep_flag, table_suffix):
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
    # prep_info_params = dict()
    # if (os.path.exists(prep_table_info_file)):
    #     with open(prep_table_info_file, 'r') as prep_config_file:
    #         for prep_table_info in json.load(prep_config_file):
    #             if prep_table_info['table_name'] == f'{table_name}{table_suffix}{synapse_suffix}':
    #                 prep_info_params = prep_table_info
    #                 prep_config_key_flag = 1
    #                 break
    #         if prep_config_key_flag == 0:
    #             print(f'No record for {table_name}{synapse_suffix} present in the preparator config file: {prep_table_info_file}')
    #     for cols_key, cols_value in prep_info_params.items():
    #         if cols_key == 'table_name':
    #             continue
    #         print('{}={}'.format(cols_key,cols_value))
    #         config_file.write('{}={}\n'.format(cols_key,cols_value))
    # else:
    #     print(f'Preparator config file not found:{prep_table_info_file}')

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

    if prevdate_value is not None:
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
    else:
        # print('day_of_the_week:{}'.format(day_of_the_week))
        if day_of_the_week != 'Monday':
            previous_process_date = str(datetime.strptime(as_of_date, "%Y%m%d") + timedelta(days=-1))[:10]
        else:
            previous_process_date = str(datetime.strptime(as_of_date, "%Y%m%d") + timedelta(days=-3))[:10]
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

def holds_process_parameters(passed_as_of_date, determination_date_df, process_flag_path, parameters_path, table_name=''):
    non_day1_tables = ['cdic_fid_t0100', 'cdic_fid_t0110', 'cdic_fid_t0120', 'cdic_fid_t0130', 'cdic_fid_t0152', 'cdic_fid_t0153', 'cdic_fid_t0400',\
                      'cdic_fid_t0500', 'cdic_fid_t0900', 'cdic_fid_agg_summary']
    non_hold_tables = ['cdic_fid_rpts_summary', 'cdic_nbdr_arrangements', 'cdic_nbdr_clients', 'cdic_nfu_gls_astrade', 'cdic_rid_trust_balance',\
                      'cdic_sei_accint_nfr', 'cdic_uci_generator', 'cdic_sei_uci', 'cdic_resp_rdsp_beneficiaries','cdic_nfu_bns','cdic_nfu_cannex',\
                       'cdic_nbdr_extract','cdic_tdw_missing_beneficiaries','cdic_ext_fi_missing_beneficiaries','cdic_uci_ops_extract','cdic_t0700']

    print('\nAll parameters passed to holds_process_parameters')
    print('passed_as_of_date:{}\ndetermination_date_df:{}\nprocess_flag_path:{}\nparameters_path:{}'.format(passed_as_of_date,determination_date_df,process_flag_path,parameters_path))

    day_of_the_week = datetime.strptime(passed_as_of_date, "%Y%m%d").strftime("%A")

    if (determination_date_df.head() is None) or (table_name in non_hold_tables):
        print('Either cdic_determination_date table dosent have any record or it is not a holds table, so running daily BAU process')
        determination_date = passed_as_of_date
        process_indicator = 'N'
        run_day = '0'
    else:
        print('cdic_determination_date table has record for this date so calculating all the other parameters')
        determination_date_collection = determination_date_df.collect()[0]
        determination_date = determination_date_collection.determination_date
        process_indicator = determination_date_collection.process_indicator
        run_day = determination_date_collection.run_day_indicator

    # this is a flag to stop process P and T to run their respective day 1 process after executing day 9 process
    if (not os.path.exists(process_flag_path)):
        PT_process_flag = 1
    elif (os.path.getsize(process_flag_path) == 0):
        PT_process_flag = 1
    else:
        process_flag_file = open(process_flag_path, 'r')
        PT_process_flag = process_flag_file.readline()
        process_flag_file.close()

    #getting all the remaining parameters based on process_indicator
    if process_indicator == 'N':
        as_of_date = passed_as_of_date
        target_process_date = datetime.strptime(passed_as_of_date,"%Y%m%d").strftime("%Y-%m-%d")
    elif (process_indicator == 'T') or (process_indicator == 'P'):
        as_of_date = datetime.strptime(determination_date,"%Y-%m-%d").strftime("%Y%m%d")
        target_process_date = datetime.strptime(passed_as_of_date,"%Y%m%d").strftime("%Y-%m-%d")+determination_date[4:]
        if (run_day == '9'):
            PT_process_flag = 0
        elif (run_day == '0'):
            PT_process_flag = 1
        else:
            pass
    else:
        pass

    print('All the parameters found in holds_process_parameters')
    print('table_name:' + table_name)
    print('determination_date:' + determination_date)
    print('process_indicator:' + process_indicator)
    print('run_day:' + run_day)
    print('as_of_date:' + as_of_date)
    print('target_process_date:' + target_process_date)
    print('PT_process_flag:{}'.format(str(PT_process_flag)))

    required_parameters = [as_of_date,process_indicator,run_day,target_process_date]

    process_flag_file = open(process_flag_path, 'w')
    process_flag_file.write(str(PT_process_flag))
    process_flag_file.close()

    with open(parameters_path, 'w', encoding='utf-8', newline='') as parameters_file:
        writer = csv.writer(parameters_file, delimiter='|')
        if (table_name in non_day1_tables) and (run_day == '1'):
            print('ignoring {} day {} process'.format(table_name, run_day))
            required_parameters.clear()
        elif ((process_indicator == 'T') or (process_indicator == 'P')) and (str(PT_process_flag) == '0'):
            print('ignoring T/P process going forward as day indicator has become 9 so waiting for T/P day 0 to restart the T/P process')
            required_parameters.clear()
        else:
            if table_name == 'cdic_t0700':
                required_parameters = [as_of_date, 'T', run_day, target_process_date]
            writer.writerow(required_parameters)
            print(required_parameters)

        #below statement is to add a record to triger N process as well if T process is trigerd
        #and also to triger N process if P process is run for day 9
        if (process_indicator == 'T' or (process_indicator == 'P' and run_day == '9')):
            target_process_date = datetime.strptime(passed_as_of_date,"%Y%m%d").strftime("%Y-%m-%d")
            required_parameters = [passed_as_of_date,'N','0',target_process_date]
            if (process_indicator == 'T' and day_of_the_week == 'Monday'):
                print('Not Adding a record to triger N along T as its a Monday')
            else:
                print(required_parameters)
                writer.writerow(required_parameters)

# COMMAND ----------

def all_parameters_needed(passed_as_of_date, config_base_path, table_name, db_names, env, user_prefix, pod_name='', prevdate_value=None, process_flag='reg', 
                            determination_date_df=spark.createDataFrame([],StructType([])), determination_date_day0_df=spark.createDataFrame([], StructType([])), 
                            synapse_suffix='', extract_prep_flag='prep', table_suffix=''):
    config_base_path = os.path.abspath(config_base_path)
    print('\nAll parameters passed to all_parameters_needed')
    print('passed_as_of_date:{}\nconfig_base_path:{}\ntable_name:{}'.format(passed_as_of_date, config_base_path, table_name))
    print('db_names:{}\nenv:{}\nuser_prefix:{}\npod_name:{}'.format(db_names, env, user_prefix, pod_name))
    print('prevdate_value:{}\nprocess_flag:{}\n'.format(prevdate_value, process_flag))
    process_flag_path = config_base_path+'/PT_process_flag.txt'
    parameters_path = config_base_path+'/parameters.txt'
    holds_process_parameters(passed_as_of_date, determination_date_df, process_flag_path, parameters_path, table_name)
    files_created = create_config_file(passed_as_of_date, config_base_path, table_name, db_names, env, prevdate_value, user_prefix, pod_name, process_flag, parameters_path, determination_date_day0_df, synapse_suffix, extract_prep_flag, table_suffix)
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