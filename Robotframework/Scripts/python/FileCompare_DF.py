# Databricks notebook source
import os
import re
import sys
import ast
import csv
import json
import shutil
import subprocess
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime as dt


# COMMAND ----------

# Class1

class All_Attributes:

    def __init__(self, table_name, passed_as_of_date, extract_table_info_file, local_target_extract_folder_path,
                 timestamp_of_run, table_sufix=''):
        # Passed Variables
        self.table_name = table_name
        self.passed_as_of_date = passed_as_of_date
        self.extract_table_info_file = os.path.abspath(extract_table_info_file)
        self.local_target_extract_folder_path = os.path.abspath(local_target_extract_folder_path)
        self.timestamp_of_run = timestamp_of_run
        self.table_sufix = table_sufix

        # setting all table specific parameters from extract_table_info_file into info_params dectionary
        self.info_params = dict()
        with open(extract_table_info_file, 'r') as config_file:
            for table_info in json.load(config_file):
                if table_info['table_name'] == f'{table_name}{table_sufix}':
                    self.info_params = table_info
                elif table_info['table_name'] == table_name:
                    self.info_params = table_info
        # Checking if the dict is empty
        if not self.info_params:
            raise (f'The info_params dictionary is empty, there seems to be some issue with the extract_table_info_file:{extract_table_info_file}')
        self.extract_file_ext = self.info_params['extract_file_ext']
        self.delimiter = self.info_params['delimiter']
        self.primary_key = self.info_params['primary_key']
        self.file_format = self.info_params['file_format']  # new

        # Derived Variables
        self.local_config_folder_path = os.path.dirname(self.extract_table_info_file)
        self.passed_process_date = dt.strptime(passed_as_of_date, '%Y%m%d').strftime('%Y-%m-%d')
        self.base_tgt_file_name = f'{self.table_name}{self.table_sufix}_{self.passed_as_of_date}'
        self.tgt_file_name = f'{self.base_tgt_file_name}.{self.extract_file_ext}'
        # self.psv_file_name = f'{self.base_tgt_file_name}.psv'
        self.temp_file_name = f'{self.base_tgt_file_name}_temp.psv'  # changed .{self.extract_file_ext} to .psv
        self.modified_file_name = f'{self.base_tgt_file_name}_mod.psv'
        self.orig_file_name = f'{self.base_tgt_file_name}_orig.{self.extract_file_ext}'  # new
        self.tgt_file_location = f'{self.local_target_extract_folder_path}\\{self.tgt_file_name}'
        self.fwf_file_location = self.tgt_file_location  # didnt remove as it had lot of dependencies
        # self.psv_file_location = f'{self.local_target_extract_folder_path}\\{self.psv_file_name}'
        self.temp_file_path = f'{self.local_target_extract_folder_path}\\{self.temp_file_name}'  # renamed new_file_path to temp_file_path
        self.modified_tgt_file_location = f'{self.local_target_extract_folder_path}\\{self.modified_file_name}'
        self.orig_tgt_file_location = f'{self.local_target_extract_folder_path}\\{self.orig_file_name}'  # update
        self.log_file_path = f'{self.local_target_extract_folder_path}\\{self.base_tgt_file_name}_log_file.txt'
        # self.log_file = open(self.log_file_path, 'w', encoding="utf8")  #new

    def __str__(self):
        return """This class stores all the attributes needed, both passed and derived in one place. And it can be inherited by other classes to use the attributes.
            All the arguments passes to the class during declaration:
            table_name
            passed_as_of_date
            extract_table_info_file
            local_target_extract_folder_path
            timestamp_of_run
            [table_sufix]"""


# COMMAND ----------

# Class2

class Functional_Check(All_Attributes):

    # setting up required variables
    def __init__(self, table_name, passed_as_of_date, extract_table_info_file, local_target_extract_folder_path,
                 timestamp_of_run, table_sufix=''):
        All_Attributes.__init__(self, table_name, passed_as_of_date, extract_table_info_file,
                                local_target_extract_folder_path,
                                timestamp_of_run, table_sufix)
        self.all_checks = []
        self.error = 0

    def __str__(self):
        return """This class is used to perform Header, Trailer and other extract Validation required. It inherits all_attributes class to use all its attributes.
            All the arguments passes to the class during declaration:
            table_name
            passed_as_of_date
            extract_table_info_file
            local_target_extract_folder_path
            timestamp_of_run
            [table_sufix]"""

    # Header,  trailer comparison and creation of tmp file
    def header_and_trailer_validation(self):
        print('\nStarting header_and_trailer_validation method. All attributes/parameters used here.')
        print(f'orig_tgt_file_location:{self.orig_tgt_file_location}\ntimestamp_of_run:{self.timestamp_of_run}')
        print(f'info_params:{self.info_params}\ntemp_file_path:{self.temp_file_path}')
        print(f'passed_process_date:{self.passed_process_date}\npassed_as_of_date:{self.passed_as_of_date}')
        print(f'error:{self.error}\nlog_file:{self.log_file}\nall_checks:{self.all_checks}')
        print(self.table_name)
        print(self.orig_tgt_file_location)

        # Calculating required dates
        timestamp_yyyy_mm_dd = dt.strptime(self.timestamp_of_run, '%Y%m%d%H%M%S').strftime('%Y-%m-%d')
        timestamp_yyyymmdd = dt.strptime(self.timestamp_of_run, '%Y%m%d%H%M%S').strftime('%Y%m%d')

        # Getting all the required counts
        total_line_count = sum(1 for line in open(self.orig_tgt_file_location, 'r', encoding="utf8"))
        skip_header_rows = int(self.info_params['skip_header_rows'])
        skip_trailer_rows = int(self.info_params['skip_trailer_rows'])
        record_count = total_line_count - skip_header_rows - skip_trailer_rows
        print(f'total_line_count:{total_line_count}\nskip_header_rows:{skip_header_rows}')
        print(f'skip_trailer_rows:{skip_trailer_rows}\nrecord_count:{record_count}')

        # Getting QA header and trailer value from config file
        qa_header_trailer = [self.info_params['first_header'], self.info_params['second_header'],
                             self.info_params['first_trailer']]
        qa_header_trailer = [rec for rec in qa_header_trailer if rec != '']

        # calculating the row no. of header and trailer
        header_trailer_lines = list(range(1, skip_header_rows + 1)) + list(
            range(total_line_count + 1 - skip_trailer_rows, total_line_count + 1))
        print(f'header_trailer_lines{header_trailer_lines}')

        # Getting Actual Extract header and trailer value and populating a new file(temp_file_path) with all the records
        fout = open(self.temp_file_path, 'w', encoding="utf8")
        count = 1
        extract_header_trailer = []
        for line in open(self.orig_tgt_file_location, 'r', encoding="utf8"):
            extract_line = line[:-1]
            # Extracting just the header and trailer records
            if count in header_trailer_lines:
                extract_header_trailer.append(extract_line)
            # Populating a new file(temp_file_path) with all the records
            else:
                fout.write(extract_line + '\n')
            count += 1
        print(f'qa_header_trailer:{qa_header_trailer}\nextract_header_trailer{extract_header_trailer}')
        print(f'temp_file_path file populated: {self.temp_file_path}')
        fout.close()

        # Checking the QA and Extract value for equality
        for i, elem in enumerate(qa_header_trailer):
            if elem[1] == 'Y':
                qa_value = elem[0].replace('!', '\|').replace('//', '\\').format(
                    timestamp_yyyy_mm_dd=timestamp_yyyy_mm_dd,
                    timestamp_yyyymmdd=timestamp_yyyymmdd, count=record_count,
                    process_date=self.passed_process_date,
                    as_of_date=self.passed_as_of_date)
                # print(f'qa_value:{qa_value}\nelem:{elem}')
                if re.match(qa_value, extract_header_trailer[i]) is None:
                    self.error += 1
                    print(
                        f'Error{self.error}:The Column Header/Trailer value is different\nQA: {qa_value}\nExtract: {extract_header_trailer[i]}')
                    self.log_file.write(
                        f'Error{self.error}:The Column Header/Trailer value is different\nQA: {qa_value}\nExtract: {extract_header_trailer[i]}\n')
                self.all_checks.append(f'Header/Trailer value:\n\tQA:{qa_value}\n\tExtract:{extract_header_trailer[i]}')
            else:
                # print(f'qa_header_trailer[i]:{qa_header_trailer[i]}\nelem:{elem}')
                if elem[0] != extract_header_trailer[i]:
                    self.error += 1
                    print(
                        f'Error{self.error}:The Column Header/Trailer value is different\nQA: {elem[0]}\nExtract: {extract_header_trailer[i]}')
                    self.log_file.write(
                        f'Error{self.error}:The Column Header/Trailer value is different\nQA: {elem[0]}\nExtract: {extract_header_trailer[i]}\n')
                self.all_checks.append(f'Header/Trailer value:\n\tQA:{elem[0]}\n\tExtract:{extract_header_trailer[i]}')

        # Summarising the result
        if self.error == 0:
            print(
                '\nNo Errors found. Extract Header, Trailer and Record Length all looks good.\nMoving on to data comparison.')
            print(f'\nChecked:')
            self.log_file.write(
                '\nNo Errors found. Extract Header, Trailer and Record Length all looks good.\nMoving on to data comparison.\n')
            self.log_file.write(f'\nChecked:\n')
            for i, check in enumerate([*set(self.all_checks)], 1):
                print(f'{i}.{check}')
                self.log_file.write(f'{i}.{check}\n')
            print()
        else:
            print(
                f'\n{self.error} Errors found. There are some issues in Extract Header, Trailer and Record Length section.\nPlease refer to this file for more info:{self.log_file_path}\n')
            self.log_file.write(
                f'\n{self.error} Errors found. There are some issues in Extract Header, Trailer and Record Length section.\nPlease refer to this file for more info:{self.log_file_path}\n\n')

    # psv/csv convertion
    def convert_existing_psv_csv_to_req_psv(self):
        print('\nStarting convert_existing_psv_csv_to_req_psv method. All attributes/parameters used here.')
        print(f'modified_tgt_file_location:{self.modified_tgt_file_location}\ntemp_file_path:{self.temp_file_path}')
        print(f'info_params:{self.info_params}\nfile_format:{self.file_format}')

        # Moving data from temp_file_path to modified_tgt_file_location as the required psv
        record_count = 1
        pipe_records_csv = []
        mod_file = open(self.modified_tgt_file_location, 'w', encoding="utf8")
        with open(self.temp_file_path, 'r', encoding='utf8') as file:
            for record in file:
                if record_count == 1 and self.info_params['tbl_column_names'] != '':
                    mod_file.write('|'.join(self.info_params['tbl_column_names']) + '\n')
                record_count += 1
                # Also keeping record of pipes already present in csv
                if self.file_format == 'csv':
                    if record.find('|') != -1:
                        pipe_records_csv.append(record)
                    # Keeping the ',' inside ""(double quotes) as it is
                    if re.search('.*".*,.*".*', record):
                        new_rec_lst = record.replace('|', '~!~').replace(',', '|').split('"')
                        # removing " from the findall result eg ['"GARY L. LUND,C.A.PROF. CORP."'] becomes ['GARY L. LUND,C.A.PROF. CORP.']
                        quoted_rec = [elem[1:-1] for elem in re.findall('".*"', record)]
                        for i, elem in enumerate(new_rec_lst):
                            if elem.replace('|', ',') in quoted_rec:
                                new_rec_lst[i] = quoted_rec[quoted_rec.index(elem.replace('|', ','))]
                        record = '"'.join(new_rec_lst)
                        mod_file.write(record)
                        continue
                    mod_file.write(record.replace('|', '~!~').replace(',', '|'))
                else:
                    mod_file.write(record)
        mod_file.close()

        # if a record in the csv already has pipes displaying them
        if len(pipe_records_csv) != 0:
            print(
                f'Issue, {len(pipe_records_csv)} records in the csv have pre-existing pipes.\npipe_records_csv:{pipe_records_csv}')
            self.log_file.write(
                f'Issue, {len(pipe_records_csv)} records in the csv have pre-existing pipes.\npipe_records_csv:{pipe_records_csv}\n')

        return self.modified_tgt_file_location

    # fwf convertion
    def convert_existing_fwf_to_req_psv(self):
        print('\nStarting convert_existing_fwf_to_req_psv method. All attributes/parameters used here.')
        print(f'modified_tgt_file_location:{self.modified_tgt_file_location}\ntemp_file_path:{self.temp_file_path}')
        print(f'info_params:{self.info_params}')

        # Moving data from temp_file_path to modified_tgt_file_location as the required psv, common for fwf and fw_csv
        all_records_df = pd.read_fwf(self.temp_file_path, widths=self.info_params['fixed_widths'],
                                     names=self.info_params['tbl_column_names'], encoding='utf8')

        # Removing the space and coma(,) from the end of every column in case of fixed width csv
        if self.file_format == 'fw_csv':
            for col_name in all_records_df.columns:
                # print(col_name)
                all_records_df[col_name] = all_records_df[col_name].astype('str').str.replace(',', '')
                all_records_df[col_name] = all_records_df[col_name].str.strip()

        # Replace '|' to ',' as we are creating a psv file
        all_records_df = all_records_df.replace('\\|', ',', regex=True)
        # print(f'Fwf sample:\n{all_records_df.head().to_string()}')
        all_records_df.to_csv(self.modified_tgt_file_location, sep='|', index=False)

        return self.modified_tgt_file_location

    # # xsd validation

    def install(self, package):
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--index-url", "https://repo.td.com/repository/pypi-all/simple", "--trusted-host", "repo.td.com", package])

    def xml_xsd_validation(self):
        # Try to import the package if that dosent work then install it first then import it
        try:
            import xmlschema
        except:
            self.install('xmlschema')
            import xmlschema

        print('\nStarting xml_xsd_validation method. All attributes/parameters used here.')
        print(f'local_config_folder_path:{self.local_config_folder_path}\ntgt_file_location:{self.tgt_file_location}')
        print(f'log_file:{self.log_file}error:{self.error}\ninfo_params:{self.info_params}')

        # loading xsd document
        xsd_file = f"{self.local_config_folder_path}\\{self.info_params['validator_file_name']}"
        my_schema = xmlschema.XMLSchema(f'{xsd_file}')

        # Validating xml file
        if my_schema.is_valid(f'{self.tgt_file_location}') == False:
            self.error += 1
            print(my_schema.validate(f'{self.tgt_file_location}'))
            print(f"Error{self.error}:The XSD Validation failed for file: {self.tgt_file_location}\nError: {my_schema.validate(f'{self.tgt_file_location}')}")
            self.log_file.write(f"Error{self.error}:The XSD Validation failed for file: {self.tgt_file_location}\nError:\n{my_schema.validate(f'{self.tgt_file_location}')}\n")
        self.all_checks.append(f'XSD validation based on file: {xsd_file}')

        if self.error == 0:
            print('\nNo Errors found. XSD validation looks good.\nMoving on to data comparison.')
            print(f'\nChecked:')
            self.log_file.write('\nNo Errors found. XSD validation looks good.\nMoving on to data comparison.\n')
            self.log_file.write(f'\nChecked:\n')
            for i, check in enumerate([*set(self.all_checks)], 1):
                print(f'{i}.{check}')
                self.log_file.write(f'{i}.{check}\n')
            print()

    # xml conversion
    def extract_atribute_text(self, element):
        extract_dict = {}
        if type(element).__name__ == 'Element':
            element_tag = re.sub("\{.*?\}", "", element.tag)
            # print(f'\nelement.tag:{element.tag}\nelement.text:"{element.text}"\nelement.attrib:{element.attrib}')
            tag = element_tag.lower()
            text = element.text
            attrib = element.attrib
            if attrib is not None:
                extract_dict = {f'{tag}_{i.lower()}':j for i,j in attrib.items()}
            else:
                extract_dict[f'{tag}_attrib'] = ''
            if (text is not None) and isinstance(text, str):
                extract_dict[f'{tag}_text'] = element.text.strip().lower()
            else:
                extract_dict[f'{tag}_text'] = ''
            # print(f'tag:{element_tag}')
        # print(f'extract_dict:{extract_dict}')
        return extract_dict

    def iterate_sub_element(self, main_elem, tag_count, main_elem_dict, parent_tag = None):
        if type(main_elem).__name__ == 'Element':
            loop_flag = 0
            for i, sub_elem in enumerate(main_elem.find('.'), 1):
                loop_flag += 1
                sub_tag = re.sub("\{.*?\}", "", sub_elem.tag)
                sub_elem_dict = self.extract_atribute_text(sub_elem)
                if sub_tag in tag_count.keys():
                    if tag_count[sub_tag]+1 == i:
                        self.list_of_dict.append(self.cumulative_dict)
                        # print(f'start:cumulative_dict:{self.cumulative_dict}\tmain_elem_dict_parent:{main_elem_dict[parent_tag]}\tlist_of_dict:{self.list_of_dict}\tmain_elem_dict_act:{main_elem_dict[sub_tag]}')
                        self.cumulative_dict = {**main_elem_dict[parent_tag], **sub_elem_dict}
                    else:
                        self.cumulative_dict = {**self.cumulative_dict, **sub_elem_dict}
                    main_elem_dict[sub_tag] = self.cumulative_dict
                else:
                    self.cumulative_dict = {**self.cumulative_dict, **sub_elem_dict}
                main_elem_dict[sub_tag] = self.cumulative_dict
                # print(f'end:cumulative_dict:{self.cumulative_dict}\tmain_elem_dict_parent:{main_elem_dict.get(parent_tag,"")}\tmain_elem_dict_act:{main_elem_dict[sub_tag]}')
                tag_count[sub_tag] = i
                # print(i, sub_tag, sub_elem_dict, parent_tag)
                # print(f'tag_count:{tag_count}')
                self.iterate_sub_element(sub_elem, tag_count, main_elem_dict, sub_tag)
            if parent_tag is None:
                # print(f'start:cumulative_dict:{self.cumulative_dict}')
                # Adding the Last element value to list_of_dict
                self.list_of_dict.append(self.cumulative_dict)
                # print(f'None:list_of_dict:{self.list_of_dict}')
        return self.list_of_dict

    def convert_existing_xml_to_req_psv(self):
        print('\nStarting convert_existing_xml_to_req_psv method. All attributes/parameters used here.')
        print(f'tgt_file_location:{self.tgt_file_location}\nlist_of_dict:{self.list_of_dict}')
        print(f'modified_tgt_file_location:{self.modified_tgt_file_location}')
        main_elem_dict = {}
        tag_count = {}
        list_of_dict_final = []
        data_tree = ET.parse(self.tgt_file_location)
        data_root = data_tree.getroot()
        # actual
        root_tag = re.sub("\{.*?\}", "", data_root.tag)
        main_elem_dict[root_tag] = self.extract_atribute_text(data_root)
        tag_count[root_tag]=1
        # print(f'main_elem_dict root_tag: {main_elem_dict[root_tag]}')
        self.list_of_dict = self.iterate_sub_element(data_root, tag_count, main_elem_dict)
        # Adding the root element values to all the dictionary element
        for dict_val in self.list_of_dict:
            list_of_dict_final.append({**main_elem_dict[root_tag], **dict_val})
        # print(f'list_of_dict_final:{list_of_dict_final}')
        # Creating DF
        final_df = pd.DataFrame(list_of_dict_final)
        final_df.to_csv(f'{self.modified_tgt_file_location}', sep='|', index=False)
        print(f'final_df:{final_df.head()}')


# COMMAND ----------

# starting point of extractor validation
def extractor_validation_main(table_name, passed_as_of_date, extract_table_info_file, local_target_extract_folder_path,
                              timestamp_of_run, table_sufix=''):
    print('All the parameters passed to extractor_validation_main:')
    print(
        f'table_name:{table_name}\npassed_as_of_date:{passed_as_of_date}\nextract_table_info_file:{extract_table_info_file}')
    print(f'local_target_extract_folder_path:{local_target_extract_folder_path}\ntimestamp_of_run:{timestamp_of_run}')
    print(f'table_sufix:{table_sufix}')

    # Functional checks
    # setting up the class
    fnc_chk = Functional_Check(table_name, passed_as_of_date, extract_table_info_file, local_target_extract_folder_path,
                               timestamp_of_run, table_sufix)

    # See all the variables in the class passed
    fnc_chk.log_file = open(fnc_chk.log_file_path, 'w', encoding="utf8")
    print()
    for k, v in vars(fnc_chk).items():
        print(f'{k}:{v}')
        fnc_chk.log_file.write(f'{k}:{v}\n')

    # Header and trailer validation and moving records to temp file
    if fnc_chk.file_format == 'xml':
        fnc_chk.xml_xsd_validation()
    else:
        fnc_chk.header_and_trailer_validation()

    # Note:If any additional checks are needed please add it here

    # Converting the existing file to psv for data comparison
    if fnc_chk.file_format == 'psv' or fnc_chk.file_format == 'csv':
        fnc_chk.convert_existing_psv_csv_to_req_psv()
    elif fnc_chk.file_format == 'fwf':
        fnc_chk.convert_existing_fwf_to_req_psv()
    elif fnc_chk.file_format == 'fw_csv':
        fnc_chk.convert_existing_fwf_to_req_psv()
    elif fnc_chk.file_format == 'xml':
        fnc_chk.cumulative_dict = {}
        fnc_chk.list_of_dict = []
        fnc_chk.convert_existing_xml_to_req_psv()

    fnc_chk.log_file.close()
    return fnc_chk


# COMMAND ----------

if __name__ == '__main__':
    table_name = 'reporting_occ'
    passed_as_of_date = '20230310'
    # local_target_extract_folder_path = f'/dbfs/FileStore/tables/DaaSWealth_QA/Main/Data/Target/{table_name}'
    # extract_table_info_file = f'/dbfs/FileStore/tables/DaaSWealth_QA/Main/Config/extract_table_info_{file_format}.txt'
    local_target_extract_folder_path = f'C:\\Users\\narayk5\OneDrive - The Toronto-Dominion Bank\\Documents\\GitHub4\\daaswealth_qa_cloud\\Scripts_v2/Data/Target/{table_name}'
    extract_table_info_file = f'C:\\Users\\narayk5\OneDrive - The Toronto-Dominion Bank\\Documents\\GitHub4\\daaswealth_qa_cloud\\Scripts_v2\\Config\\extract_table_info_dc.json'
    timestamp_of_run = '20230329133258'
    table_sufix='_3'

    fnc_chk = extractor_validation_main(table_name, passed_as_of_date, extract_table_info_file, local_target_extract_folder_path, timestamp_of_run, table_sufix)
