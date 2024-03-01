# Databricks notebook source
import sys

# COMMAND ----------

# Class1

class All_Env_Specific_Variables:

    # Source raw zone
    test_rz_db = 'container,folder,storage_account,db_name'
    # Source processed zone
    test_pz_db = 'container,folder,storage_account,db_name'

    # Target processed zone
    test_pz_base_path = {'my': 'abfss://processed@karadlsdev.dfs.core.windows.net'}
    # Target mount
    test_pz_mount_path = {'my': 'dbfs:mnt/processed'}

    # Synapse raw zone
    test_pz_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
    # Synapse main(old)
    test_main_synapse_conn = {'my': {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}}

    def __init__(self, env, pod_name, dbutilspkg, dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/'):

        # Variables that are common across all environment
        self.dbfs_folder_base_path = dbfs_folder_base_path
        self.folder_path_config = f'{dbfs_folder_base_path}/Config'
        self.folder_path_logs = f'{dbfs_folder_base_path}/Logs'
        self.folder_path_data = f'{dbfs_folder_base_path}/Data'
        self.folder_path_src_data = f'{dbfs_folder_base_path}/Data/Source'
        self.folder_path_tgt_data = f'{dbfs_folder_base_path}/Data/Target'
        self.folder_path_results = f'{dbfs_folder_base_path}/Results'
        self.folder_path_scripts = f'{dbfs_folder_base_path}/Scripts'
        self.folder_paths = {'dbfs_folder_base_path':self.dbfs_folder_base_path, 'folder_path_config':self.folder_path_config,
                             'folder_path_logs':self.folder_path_logs, 'folder_path_data':self.folder_path_data,
                             'folder_path_src_data':self.folder_path_src_data, 'folder_path_tgt_data':self.folder_path_tgt_data,
                             'folder_path_results':self.folder_path_results, 'folder_path_scripts':self.folder_path_scripts,}

        self.current_user_info(dbutilspkg)

        # Variables that change across different env
        exec(f"self.rz_db=self.{env}_rz_db.split(',')")
        exec(f"self.pz_db=self.{env}_pz_db.split(',')")
        # Variables that change accross different env and pod_name
        exec(f"self.pz_base_path_dict=self.{env}_pz_base_path")
        exec(f"self.pz_mount_path_dict=self.{env}_pz_mount_path")
        exec(f"self.rz_synapse_conn=self.{env}_rz_synapse_conn")
        exec(f"self.main_synapse_conn_dict=self.{env}_main_synapse_conn")

        self.pz_base_path = self.pz_base_path_dict.get(pod_name,'')
        self.pz_mount_path = self.pz_mount_path_dict.get(pod_name,'')
        self.main_synapse_conn = self.main_synapse_conn_dict.get(pod_name,'')

        self.db_names = {'rz':self.rz_db, 'pz':self.pz_db}
        self.synapse_conn = {'main_synapse_conn':self.main_synapse_conn}

        print(f'db_names: {self.db_names}\nmain_synapse_conn: {self.main_synapse_conn}')
        print(f'pz_base_path: {self.pz_base_path}\npz_mount_path: {self.pz_mount_path}')
        print(f"folder_paths: {self.folder_paths}")

    def __str__(self):
        return """This class stores all the variables that change accross environment, based on env and pod_name.
            All the arguments passes to the class during declaration:
            env
            pod_name"""

    def common_vars_add_to_config(self, config_file_path):

        sys.path.insert(1, f'{self.dbfs_folder_base_path}/Scripts/python'.replace('//','/'))
        import dates_needed

        # self.comman_variables()

        # Adding DB variables
        # print(type(self.db_names))
        malcode_dict = dict()
        for db_key, db_value in self.db_names.items():
            #print(db_key, db_value)
            
            malcode_dict[f'container_{db_key}'] = db_value[0]
            malcode_dict[f'folder_path_{db_key}'] = db_value[1]
            malcode_dict[f'storage_acc_{db_key}'] = db_value[2]
            malcode_dict[f'database_{db_key}'] = db_value[3]
            if len(db_value) == 5:
                malcode_dict[f'synapse_conn_{db_key}'] = db_value[4]

        conf = dates_needed.add_aditional_parameters(config_file_path, dbfs_folder_base_path=self.dbfs_folder_base_path,
                                                     **malcode_dict, **self.synapse_conn, cz_base_path=self.pz_base_path,
                                                     pz_mount_path=self.pz_mount_path)

    def current_user_info(self, dbutilspkg):
        name_mapping = {'karthik': 'kar', 'narayankarthik19': 'kar', 'jeremy': 'jer'}
        self.user_name = dbutilspkg.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
        self.user_prefix = name_mapping.get(self.user_name.split('.')[0])
        print(f'\nuser_name:{self.user_name}, user_prefix:{self.user_prefix}')
        return self.user_name, self.user_prefix

# COMMAND ----------

# # # Uncomment if you want to run this file alone, for testing
# if __name__ == '__main__':
#     env = 'az_restrict'
#     pod_name = 'ep1'
#     config_file_path = 'C:/Users/narayk5/OneDrive - The Toronto-Dominion Bank/Documents/GitHub4/daaswealth_qa_cloud/Scripts_v2/Config/config_file_kar.yaml'
#     dbfs_folder_base_path = 'C:/Users/narayk5/OneDrive - The Toronto-Dominion Bank/Documents/GitHub4/daaswealth_qa_cloud/Scripts_v2'
#     cmn_vars = All_Env_Specific_Variables(env, pod_name, dbfs_folder_base_path)
#     cmn_vars.common_vars_add_to_config(config_file_path)
