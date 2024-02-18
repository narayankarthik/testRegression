# from azure.mgmt.datafactory import DataFactoryManagementClient
# from azure.common.credentials import UserPassCredentials #To login with user and pass, use this.
# from azure.common.credentials import ServicePrincipalCredentials #To login with service principal (appid and client secret) use this
#
# subscription_id = "7a7b245b-aa14-4df6-95ce-658835762b6a"
#
# #Use only one of these, depending on how you want to login.
# credentials = UserPassCredentials(username="narayankarthik19@gmail.com", password="Hrv_Mik2012") #To login with user and pass
# # credentials = ServicePrincipalCredentials(client_id='002c181e-0f72-4b9b-8499-a11d5bde3093',
# #                                           secret='client secret',
# #                                           tenant='tenantid') #To login with serv ppal
#
# adf_client = DataFactoryManagementClient(credentials, subscription_id)
#
#
# # rg_name = "kar_etl_test_project"
# # df_name = "dkaradfdev"
# # p_name = "pl_test_wait"
# # params = {}
# #
# # adf_client.pipelines.create_run(rg_name, df_name, p_name, params)

# client id: cfa048f1-3199-4183-88ec-22c39f0869b1
# object id: 72f45d5d-2306-4bfb-8ed6-18c74020984f
# tenant id: 21159e3f-d353-413e-ae2a-132742d43740
# secret val: Ce-8Q~vTneKGr_nMBcWjO5~k0a4WzDxkJdPyEabO
# subscriptions/7a7b245b-aa14-4df6-95ce-658835762b6a/resourceGroups/kar_etl_test_project/providers/Microsoft.DataFactory/factories/karadfdev

import requests
import json
# Set variables
tenant_id = '21159e3f-d353-413e-ae2a-132742d43740' # Done Get from SPN
client_id = 'cfa048f1-3199-4183-88ec-22c39f0869b1' # Done Get from SPN
client_secret = 'Ce-8Q~vTneKGr_nMBcWjO5~k0a4WzDxkJdPyEabO' # Done Get from SPN

# get access token
resource = 'https://management.core.windows.net/'
# Get access token
token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
data = {
  'grant_type': 'client_credentials',
  'client_id': client_id,
  'client_secret': client_secret,
  'resource': resource
}
response = requests.post(token_url, headers=headers, data=data)
access_token = response.json()['access_token']
print(f"access_token:{access_token}")

# trigger pipeline
PipelineName = "pl_test_wait" #Done
pipelinePayload = {}
subscriptionId = "7a7b245b-aa14-4df6-95ce-658835762b6a" #Done
resourceGroupName = "kar_etl_test_project" #Done
factoryName = "karadfdev" #Done

runId = None

callPipelinePostUrl = f"https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{PipelineName}/createRun?api-version=2018-06-01"
print("triggerPipelinePostUrl: " + callPipelinePostUrl)

headers = {"Authorization": f"Bearer {access_token}"}
response = requests.post(callPipelinePostUrl, data=pipelinePayload, headers=headers)

if (response.status_code == 200):
    response_dict = response.json()
    pipelineRunId = response_dict.get("runId")
    print("Run Id: " + pipelineRunId)
else:
    print("ERROR: ", response)
    # Assert.fail("ERROR : API Call error " + response.getStatusLine());
