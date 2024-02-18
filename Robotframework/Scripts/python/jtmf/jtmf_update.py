from requests.auth import HTTPBasicAuth
from jira import JIRA
import requests
import urllib3
import codecs
import time
import sys
import os

urllib3.disable_warnings()

# Connect to Jira
def establish_connection(host, user_name, password):
    """
        Connect to Jira
        Params: obscure_user_name, obscure_password
        Returns:
    """
    global jira

    # host = 'https://jtmf.td.com'
    print('Connecting to server:', host)
    try:
        # user_name = obscure_credentials.unobscure(obscure_user_name)
        # password = obscure_credentials.unobscure(obscure_password)
        options = {'server': host, 'verify': False}
        jira = JIRA(options=options, basic_auth=(user_name, password))
        print("CONNECTION OK")
        return True
    except Exception as e:
        print("CONNECTION ERROR", str(e))
        return False

# create the Test Execution(TExec)
def create_issue(project_key, pod, issue_type, table_name='', t_plan_key='', issue_sub_type='Regression'):
    """
        Creates Test Execution
        Params: project_key-project inside which the Execution is to be created
                pod, issue_type, table_name, t_plan_key
                issue_sub_type used to create issue with custome title by default its 'Regression'
        Returns: created issue key
    """
    global jira
    unix_ts = int(time.time())
    if issue_type.lower() == 'test execution':
        issue_dict = {
                        'project': {'key': f'{project_key}'},
                        'summary': f'Execution {issue_sub_type} results - [{unix_ts}]',
                        'description': f'Execution of {issue_sub_type} Test case',
                        'issuetype': {'name': 'Test Execution'},
                        "customfield_10219": {"value": pod}
                    }
        # Adding TExec to TP only if TP is provided
        if t_plan_key != '':
            issue_dict['customfield_10027'] = [t_plan_key]
        print("Created Test Execution.")
    elif issue_type.lower() == 'test':
        issue_dict = {
                        'project': {'key': f'{project_key}'},
                        'summary': f'{issue_sub_type} TC {table_name}',
                        'description': f'Created Automated {issue_sub_type} Test Case for {table_name}',
                        'issuetype': {'name': 'Test'},
                        "customfield_10219": {"value": pod},
                    }
        print("Created Test.")
    issue = jira.create_issue(fields=issue_dict)
    print(f'issue:{issue.key}')
    return issue.key

# Link the Test Execution(TExec) to Test Plan(TP)
def link_inward_n_outward_issue(inward_key, outward_key, link_type='Relates'):
    """
        Links outward issue(eg. Test Case) to inward issue(eg. Test Exec)
        Params: inward_key-issue to have the link,
                outward_key-issue to be linked
        Returns:
    """
    global jira
    print('Linking outward issue to inward issue')
    try:
        inward_issue = jira.issue(f'{inward_key}')  #eg. Test Exec
        outward_issue = jira.issue(f'{outward_key}')    #eg. Test Case
        print(f'inward_issue:{inward_issue.fields.issuetype.name}:{inward_issue}')
        print(f'outward_issue:{outward_issue.fields.issuetype.name}:{outward_issue}')
    except Exception as e:
        print(f'Issue not fount: {e}')
    response = jira.create_issue_link(
        type=link_type,
        inwardIssue=f"{inward_key}",
        outwardIssue=f"{outward_key}",
        comment={'body': f"Linked {outward_key} to {inward_key}"}
    )

# Adding Test cases to Test Plan
def add_test_cases_to_plan(t_plan_key, t_case_key_lst):
    """
        Adds all the Test cases generated to the Test plan
        Params: t_plan_key: Test plan Key
                t_case_key_lst: list of test case to be added to the TP
        Returns:
    """
    issue_tp = jira.issue(t_plan_key)
    test_case_arr=issue_tp.raw["fields"]["customfield_10026"]
    # test_case_arr.append("TDAASWLTH-5296")
    test_case_arr.extend(t_case_key_lst)
    test_case_arr = list(set(test_case_arr))
    fields= {
                "customfield_10026":test_case_arr
            }
    issue_tp.update(fields=fields)
    print('Test Cases added to Test Plan')

# Update attributes of the TC
def update_test_case_attrib(issue_key, user_name, fix_version=''):
    """
        Update Test Case attributes
        Params: issue_key: Test Case Key
                user_name: User to be added as assignee
        Returns:
    """
    issue_tc = jira.issue(f'{issue_key}')
    fields= {
                "description": "This is an automated test case, executed with python. Please refer to the"
                               "Files attached to the Test Execution JIRA to get the final result",
                "customfield_10201": {"id": "11306"},
                "customfield_10304": [{"id": "11206"}, {"id": "11205"}],
                "customfield_10309": [{"id": "11217"}, {"id": "11218"}, {"id": "11219"}, {"id": "11226"}],
                "assignee": {"name": user_name}
                #"components": [{"name": self.component}]
            }
    if fix_version != '':
        fields['fixVersions'] = [{"name": fix_version}]
    issue_tc.update(fields=fields)
    print('Test Case Updated')

# Update attributes of TExec and linking it to Test Case(TC)
def update_test_exec_attrib(issue_key, user_name, t_case_key='', fix_version=''):
    """
        Update attributes of TExec and linking it to Test Case(TC)
        Params: issue_key: Test Execution Key
                user_name: User to be added as assignee
                t_case_key: Test Case Key, to be linked to the TExec
        Returns:
    """
    issue_te = jira.issue(f'{issue_key}')
    fields = {"customfield_10228": {"value": "Yes"},
                "assignee": {"name": user_name}
                #"components": [{"name": self.component}]
                }
    # Linking TP to TExec only if TP is provided
    if t_case_key != '':
        fields['customfield_10015']= [t_case_key]
    if fix_version != '':
        fields['fixVersions'] = [{"name": fix_version}]
    issue_te.update(fields=fields)
    print('Test Exec Updated')

# Update attributes of the Story
def update_story_attrib(story_key, user_name, fix_version=''):
    """
        Update attributes of Story
        Params: story_key: Story Key
                user_name: User to be added as assignee
                fix_version
        Returns:
    """
    issue_te = jira.issue(story_key)
    fields = {"assignee": {"name": user_name}
              }
    if fix_version != '':
        fields['fixVersions'] = [{"name": fix_version}]
    issue_te.update(fields=fields)
    print('Test Exec Updated')

# Transition TC
def transition_issue(issue_key, transition_id):
    issue = jira.issue(f'{issue_key}')
    # transitions = jira.transitions(issue)
    try:
        jira.transition_issue(issue, transition_id)
    except Exception as e:
        print(f'Transition not fount: {e}')
    print('Completed Transition')

# Add all files to TExec
def add_all_attachments_issue(issue_key, *file_paths):
    issue = jira.issue(f'{issue_key}')
    for file_path in file_paths:
        filename = os.path.basename(file_path)
        jira.add_attachment(issue=issue, attachment=file_path, filename=filename)
    print('Completed Addition of file to the Issue')

# Transition TC attached to TExec to pass/fail
def update_test_exec_tc_status(t_exec_key, t_case_key_dict, host, user_name, password):
    """
        Update TC added to TExec to show passed/failed status
        Params: t_exec_key: Test Execution Key
                t_case_key_dict: Test case Key with coresponding PASS/FAIL status
                host, user_name, password
        Returns:
    """
    auth = HTTPBasicAuth(user_name, password)
    session = requests.Session()
    issue_te = jira.issue(t_exec_key)
    test_ids_in_test_execution = issue_te.raw["fields"]["customfield_10015"]
    test_id_dict = dict()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    for test_id_in_test_execution in test_ids_in_test_execution:
        # if t_case_key_dict[test_id_in_test_execution['b']] == 'PASS':
        pass_input_url = host + "/rest/raven/1.0/api/testrun/" + str(test_id_in_test_execution['c']) \
                         + "/status?status=" + f"{t_case_key_dict[test_id_in_test_execution['b']]} "
        update_test_to_pass_response = session.put(pass_input_url,
                                                   headers=headers,
                                                   auth=auth,
                                                   verify=False,
                                                   timeout=10)

# Add comments to the TExec
def add_comment_to_issue(issue_key, comment_message=''):
    issue = jira.issue(f'{issue_key}')
    if comment_message == '':
        comment_message = "Changed Automated='Yes'. Python framework [^output.xml] has been attached for this test execution." \
                        " Marking the test execution as DONE. Additionally certain fields in the tests also have been modified"
    jira.add_comment(issue, comment_message)
    print('Completed Addition of comment to the Issue')

def jtmf_update_main(ticket_key_dict, jtmf_params, dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/', *output_file):

    sys.path.insert(1, f'{dbfs_folder_base_path}/python'.replace('//', '/'))
    import obscure_credentials

    print('\nStarting main method. All attributes/parameters used here.')
    print(f'ticket_key_dict:{ticket_key_dict}\njtmf_params:{jtmf_params}')
    print(f'output_file:{output_file}\ndbfs_folder_base_path:{dbfs_folder_base_path}')

    t_case_key_dict = dict()
    t_plan_key = ticket_key_dict.get('test_plan', '')
    story_key = ticket_key_dict.get('story', '')

    user_cred_file_path = f'{dbfs_folder_base_path}\\Config\\user_cred.txt'
    print(f'user_cred_file_path: {user_cred_file_path}')
    user_cred = dict()
    with open(user_cred_file_path, 'r') as user_cred_file:
        for line in user_cred_file:
            # print(line[:-1])
            line = line.replace('\n', '').split(': ')
            if len(line) == 2:
                user_cred[line[0]] = line[1].replace('\r', '')
    print("user_cred content:{0}".format(user_cred))

    try:
        obscure_user_name = user_cred['obscure_user_name']
        obscure_password = user_cred['obscure_password']
        project_key = user_cred['project_key']
        pod = user_cred['pod']
        jtmf_host = user_cred['jtmf_host']
        fix_version = user_cred.get('fix_version', '')
        sprint = user_cred.get('sprint', '')
        print(f'project_key:{project_key}\npod:{pod}\njtmf_host:{jtmf_host}')
    except Exception as e:
        print(f"""One of the required parameter is missing
        we need obscure_user_name, obscure_password, project_key and pod
        Error message:{e}""")

    global jira
    # Connect to Jira
    user_name = obscure_credentials.unobscure(obscure_user_name)
    password = obscure_credentials.unobscure(obscure_password)
    establish_connection(jtmf_host, user_name, password)
    # create the Test Execution(TExec) & Link the Test Execution(TExec) to Test Plan(TP)    #changed
    t_exec_key = create_issue(project_key, pod, issue_type='Test Execution', t_plan_key=t_plan_key)
    # t_exec_key = 'TDAASWLTH-5495'   # test
    # loop through all the elements of the list
    for param in jtmf_params:
        table_name = param.get('table_name')
        overall_status = param.get('overall_status')
        # Search if Test Case(TC) exists, if not then create the TC
        tc_issues_search = jira.search_issues(f'summary ~ "Regression TC {table_name}"')  # and issue in TestRepositoryFolderTests(TDAASWLTH,"Avengers/CDIC","true")

        if tc_issues_search:
            print('Test Case already Exists.')
            for tc_issue_search in tc_issues_search:
                t_case_key = tc_issue_search.key
                print(t_case_key)
        else:
            print('No Test Case found. So creating the Test Case.')
            t_case_key = create_issue(project_key, pod, issue_type='Test', table_name=table_name)
        if overall_status == 'PASS':
            t_case_key_dict[t_case_key] = 'PASS'
        else:
            t_case_key_dict[t_case_key] = 'FAIL'

        if t_plan_key != '':
            # Add the Test Case(TC) to Test Plan(TP)    #Changes
            add_test_cases_to_plan(t_plan_key, t_case_key_dict.keys())

        ticket_user_name = user_name    #'tag1535'    #This is just in case you want to run for someone else
        # Update attributes of TExec and linking Test Case(TC) to it
        update_test_exec_attrib(t_exec_key, ticket_user_name, t_case_key, fix_version=fix_version)
        # Update attributes of the TC
        update_test_case_attrib(t_case_key, ticket_user_name, fix_version=fix_version)
        # Transition TC
        transition_issue(t_case_key, '91')
        # # Add file to TExec
        # add_all_attachments_issue(t_exec_key, output_file)
    # Transition TC attached to TExec to pass/fail
    update_test_exec_tc_status(t_exec_key, t_case_key_dict, jtmf_host, user_name, password)
    # Transition TExec(I think it'll be out of the loop)
    transition_issue(t_exec_key, '11')
    # Add comments to the TExec
    add_comment_to_issue(t_exec_key)

    # Adding TC and TExec to Story, if its provided
    if story_key != '':
        for t_case_key in t_case_key_dict.keys():
            # Update attributes of the Story
            update_story_attrib(story_key, ticket_user_name, fix_version=fix_version)
            # Transition Story
            transition_issue(story_key, '31')
            # # Link the Test Case(TC) to Story
            # link_inward_n_outward_issue(t_case_key, story_key, 'Tests')
            # # Link the Test Exec(TExec) to Story
            # link_inward_n_outward_issue(story_key, t_exec_key)


if __name__ == '__main__':
    # NO loops
    ticket_key_dict = {'test_plan':'TDAASWLTH-5535', 'story': 'TDAASWLTH-5501'} #'test_plan':'TDAASWLTH-2611'
    dbfs_folder_base_path = 'C:\\Users\\narayk5\OneDrive - The Toronto-Dominion Bank\\Documents\\GitHub4\\daaswealth_qa_cloud\\Scripts_v2'
    jtmf_params = [{'table_name': 'mstar_international_bonds', 'overall_status': 'PASS'}]
    jtmf_update_main(ticket_key_dict, jtmf_params, dbfs_folder_base_path)
    # # With loops
    # ticket_key_dicts = [{'test_plan': 'TDAASWLTH-5535', 'story': 'TDAASWLTH-5496'},
    #                    {'test_plan': 'TDAASWLTH-5535', 'story': 'TDAASWLTH-5497'},
    #                    {'test_plan': 'TDAASWLTH-5535', 'story': 'TDAASWLTH-5498'},
    #                    {'test_plan': 'TDAASWLTH-5535', 'story': 'TDAASWLTH-5500'},
    #                    {'test_plan': 'TDAASWLTH-5535', 'story': 'TDAASWLTH-5501'}]  # 'test_plan':'TDAASWLTH-2611'
    # dbfs_folder_base_path = 'C:\\Users\\narayk5\OneDrive - The Toronto-Dominion Bank\\Documents\\GitHub4\\daaswealth_qa_cloud\\Scripts_v2'
    # jtmf_params_all = [[{'table_name': 'bank_account_odp', 'overall_status': 'PASS'}],
    #                [{'table_name': 'sf_pb_products_mhaccount', 'overall_status': 'PASS'}],
    #                [{'table_name': 'sf_pb_products_via', 'overall_status': 'PASS'}],
    #                [{'table_name': 'mstar_private_placement_transaction_bus', 'overall_status': 'PASS'}],
    #                [{'table_name': 'mstar_international_bonds', 'overall_status': 'PASS'}]]
    # for i, jtmf_params in enumerate(jtmf_params_all):
    #     jtmf_update_main(ticket_key_dicts[i], jtmf_params, dbfs_folder_base_path)
    # output_file = 'C:\\Users\\narayk5\\OneDrive - The Toronto-Dominion Bank\\Desktop\\documents\\Sprint\\Sprint_169\\JTMF\\output.xml'
    # jtmf_params = [{'table_name': 'iiroc_6_month_supervision_report', 'overall_status': 'PASS',
    #                 'config_params': {'env': 'dev', 'user_prefix': 'kar', 'synapse_suffix': '',
    #                                   'as_of_date': '20201130',
    #                                   'process_date': '2020-11-30', 'podium_delivery_date': '20201130000000',
    #                                   'as_of_date_wca': '20201201', 'process_date_wca': '2020-12-01',
    #                                   'podium_delivery_date_wca': '20201201000000',
    #                                   'as_of_date_currmonth_firstday': '20201101',
    #                                   'process_date_currmonth_firstday': '2020-11-01',
    #                                   'podium_delivery_date_currmonth_firstday': '20201101000000',
    #                                   'as_of_date_prevmonth_lastday': '20201031',
    #                                   'process_date_prevmonth_lastday': '2020-10-31',
    #                                   'podium_delivery_date_prevmonth_lastday': '20201031000000',
    #                                   'as_of_date_twoprevmonth_firstday': '20200901',
    #                                   'process_date_twoprevmonth_firstday': '2020-09-01',
    #                                   'podium_delivery_date_twoprevmonth_firstday': '20200901000000',
    #                                   'previous_process_date': '2020-11-27', 'previous_as_of_date': '20201127',
    #                                   'previous_podium_delivery_date': '20201127000000',
    #                                   'previous_process_date_wca': '2020-11-28',
    #                                   'previous_as_of_date_wca': '20201128',
    #                                   'previous_podium_delivery_date_wca': '20201128000000',
    #                                   'non_nullable_col': 'depositor_id,depositor_unique_id,personal_id_count,personal_id_type_code,last_insert_timestamp,process_date',
    #                                   'pk_col': 'depositor_id', 'ignore_col': 'last_insert_timestamp',
    #                                   'dbfs_folder_base_path': '/dbfs/FileStore/tables/DaaSWealth_QA/',
    #                                   'container_cdic': 'wcz0003',
    #                                   'folder_path_cdic': 'curated/wcz0003/test_src', 'database_cdic': 'cdic',
    #                                   'storage_acc_cdic': 'edaaawcze1devcz', 'container_cif': 'cif',
    #                                   'folder_path_cif': 'CIF',
    #                                   'database_cif': 'cif', 'storage_acc_cif': 'edaaaedle1devsrz',
    #                                   'container_odr': 'wcz0003',
    #                                   'folder_path_odr': 'curated/wcz0003/test_src', 'database_odr': 'odr',
    #                                   'storage_acc_odr': 'edaaawcze1devcz', 'container_ossbr': 'ossbr',
    #                                   'folder_path_ossbr': 'OSSBR',
    #                                   'database_ossbr': 'ossbr', 'storage_acc_ossbr': 'edaaaedle1devsrz',
    #                                   'container_ptsql': 'ptsql',
    #                                   'folder_path_ptsql': 'PTSQL', 'database_ptsql': 'ptsql',
    #                                   'storage_acc_ptsql': 'edaaaedle1devsrz', 'container_upg': 'upg',
    #                                   'folder_path_upg': 'UPG',
    #                                   'database_upg': 'upg', 'storage_acc_upg': 'edaaaedle1devsrz',
    #                                   'container_wca': 'wca',
    #                                   'folder_path_wca': 'WCA', 'database_wca': 'wca',
    #                                   'storage_acc_wca': 'edaaaedle1devsrz',
    #                                   'container_wdlob': 'wcz0003', 'folder_path_wdlob': 'curated/wcz0003/test_src',
    #                                   'database_wdlob': 'wdlob', 'storage_acc_wdlob': 'edaaawcze1devcz',
    #                                   'container_wdprm': 'wdprm',
    #                                   'folder_path_wdprm': 'WDPRM', 'database_wdprm': 'wdprm',
    #                                   'storage_acc_wdprm': 'edaaaedle1devsrz', 'container_wds': 'wds',
    #                                   'folder_path_wds': 'WDS',
    #                                   'database_wds': 'wds', 'storage_acc_wds': 'edaaaedle1devsrz',
    #                                   'container_wdsm': 'wdsm',
    #                                   'folder_path_wdsm': 'WDSM', 'database_wdsm': 'wdsm',
    #                                   'storage_acc_wdsm': 'edaaaedle1devsrz',
    #                                   'container_w360': 'w360', 'folder_path_w360': 'W360', 'database_w360': 'w360',
    #                                   'storage_acc_w360': 'edaaaedle1devsrz',
    #                                   'synapse_conn': "{'server': 'd3004-eastus2-asql-42.database.windows.net', 'database': 'eda-akora-aawcz-wcz0005pooldev', 'scope': 'aawcz', 'username': 'SP_ADB_AAWCZ_WCZ0005_DEV-AppID', 'password': 'SP_ADB_AAWCZ_WCZ0005_DEV-PWD'}",
    #                                   'cz_base_path': 'abfss://wcz0003@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0003'}}]
    # jtmf_update_main(t_plan_key, jtmf_params, dbfs_folder_base_path, output_file)