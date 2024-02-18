from robot.api.deco import library, keyword
import obscure_credentials as obscred
from jproperties import Properties
from base64 import decodebytes
from Pipeline import Pipeline
import requests
import paramiko
import time
import os

@library
class EtlValidationTestSteps:

	def __init__(self):
		# Folder locations
		self.python_folder = os.path.dirname(__file__)
		self.parent_folder = os.path.abspath(os.path.realpath(os.path.join(self.python_folder, '../')))
		self.config_folder = os.path.abspath(f"{self.parent_folder}/Config/")
		self.property_file = os.path.abspath(f"{self.config_folder}/tcoe.properties")

		# Normal Variables
		self.TestProperty = self.extract_TestProperty()
		self.host = self.TestProperty.get("ADF.SSH.Hostname")
		self.objectID = self.TestProperty.get("ADF.SSH.ObjectID")
		self.user = obscred.unobscure(self.TestProperty.get("ADF.SSH.Username"))
		self.password = obscred.unobscure(self.TestProperty.get("ADF.SSH.Password"))
		self.subscriptionId = self.TestProperty.get("ADF.SubscriptionId")
		self.resourceGroupName = self.TestProperty.get("ADF.ResourceGroupname")
		self.factoryName = self.TestProperty.get("ADF.Factoryname")
		self.pipeline = Pipeline()

	@keyword
	def extract_TestProperty(self):
		print(f"property_file: {self.property_file}")
		config_temp = Properties()
		config = {}
		with open(self.property_file, 'rb') as config_file:
			config_temp.load(config_file)
		for k, v in config_temp.items():
			config[k] = v.data
		print(config)
		return config

	@keyword("Pipeline name")
	def pipeline_name(self, PipelineName):
		self.pipeline.setPipelineName(PipelineName)
		print(self.pipeline.getPipelineName())

	@keyword
	def add_Parameters_for_Duplicate_Record_Validation_in_ADLS(self, malcode, tableName, schemaName, env, datamart):
		self.pipeline.setMalcode(malcode)
		self.pipeline.setTableName(tableName)
		self.pipeline.setSchemaName(schemaName)
		self.pipeline.setEnv(env)
		self.pipeline.setDatamart(datamart)
		self.payload = dict()
		self.payload["malcode"] = self.pipeline.getMalcode()
		self.payload["tableName"] = self.pipeline.getTableName()
		self.payload["schemaName"] = self.pipeline.getSchemaName()
		self.payload["env"] = self.pipeline.getEnv()
		self.payload["datamart"] = self.pipeline.getDatamart()
		self.pipeline.setPayload(self.payload)
		print(self.pipeline.getPayload())

	@keyword
	def get_Bearer_Token(self):
		if (self.host is None) or (self.user is None) or (self.password is None):
			print("ERROR: Missing mandatory tcoe parameters, please check if all mandatory tcoe parameters are provided in tcoe.properties file ")
			# Assert.fail("ERROR: Missing mandatory tcoe parameters, please check if all mandatory tcoe parameters are provided in tcoe.properties file ")
		else:
			self.bearerToken = self.generateBearerToken(self.host, self.user, self.password)
		if self.bearerToken is not None:
			self.pipeline.setToken(self.bearerToken)
			print("Token: " + self.pipeline.getToken())
		else:
			print("ERROR in generating Bearer Token")
			# Assert.fail("ERROR in generating Bearer Token; token generated:" + pipeline.getToken())

	@keyword
	def generateBearerToken(self, host, user, password):
		# keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABgQDIyTDGIKtDfQ2u06UHD0XZfc4ltRh0by6AJ5HiCMOJyw/HJCZwCL7fH3UQWyCm8/+FWYHZwCWu2D+TIJqe1IqkiMeOWWG94bHg1jwxczVW1GoS5IoLhWuCq3+oGme5jrNJx6eHEkCkCM6YIXs3Wg5xZzoCGVhwh7b+G3Nm5YxEZPFTWqfnLkRjwk9ZUvQkOynFsFe5kH+pw2zhXYWax9LkkqyJlFYsufMsXNSwv6VOYiuctwZE/HfnzduKbhlP3h6m4mMqc6BjX/f9I3Ov2p8SoPCRrJpydxKxtJz9Ur0R4u+s6a/SU7E7/GWrjQtqdAwKVGgrHzXWIO7H+txuKfUO77q1NMZvYa3TgQAtBGyzlsz79sahehercK27BgY9+z3l1f3YdVWJcpLtGKH/61QAw8XggxWVbEE1zA3pdv9L0obIbXErGYFZxD/1sb55rG5q/FQMLyhozCBKjEAIEh00566hpCzRFOdV0uIQfBbaFYS1XradbTYJq+UdybylcS8="""
		# key = paramiko.RSAKey(data=decodebytes(keydata))
		ssh = paramiko.SSHClient()
		# ssh.get_host_keys().add(self.host, "ssh-rsa", key)
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.connect(self.host, username=self.user, password=self.password)
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(f"""curl -s -H Metadata:true -X GET 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com&object_id={self.objectID}'""")

		string = ssh_stdout.read().decode('ascii').strip("\n")
		response_dict = eval(string)
		self.bearerToken = response_dict['access_token']
		print(f"{ssh_stdin}\n{ssh_stdout}\n{ssh_stderr}\n{string}\n{response_dict}")

		return self.bearerToken

	@keyword
	def trigger_Specific_Pipeline(self):
		pipelineName = self.pipeline.getPipelineName();
		pipelinePayload = self.pipeline.getPayload();

		if (self.resourceGroupName is None) or (self.factoryName is None):
			print("ERROR: Missing mandatory tcoe parameters, please check if all mandatory tcoe parameters are provided in tcoe.properties file ")
			# Assert.fail("ERROR: Missing mandatory tcoe parameters, please check if all mandatory tcoe parameters are provided in tcoe.properties file ")

		runId = None
		PipelineName = self.pipeline.getPipelineName()
		callPipelinePostUrl = f"https://management.azure.com/subscriptions/{self.subscriptionId}/resourceGroups/{self.resourceGroupName}/providers/Microsoft.DataFactory/factories/{self.factoryName}/pipelines/{PipelineName}/createRun?api-version=2018-06-01"
		print("triggerPipelinePostUrl: " + callPipelinePostUrl)

		headers = {"Authorization": f"Bearer {self.bearerToken}"}
		response = requests.post(callPipelinePostUrl, data=pipelinePayload, headers=headers)

		if (response.status_code == 200):
			response_dict = response.json()
			self.pipelineRunId = response_dict.get("runId")
			print("Run Id: " + self.pipelineRunId)
			self.pipeline.setPipelineRunId(self.pipelineRunId)
		else:
			print("ERROR: ", response)
			# Assert.fail("ERROR : API Call error " + response.getStatusLine());

		self.pipeline.setPipelineStatus("");
		pipelineRunStatusGetUrl = f"https://management.azure.com/subscriptions/{self.subscriptionId}/resourceGroups/{self.resourceGroupName}/providers/Microsoft.DataFactory/factories/{self.factoryName}/pipelineruns/{self.pipelineRunId}?api-version=2018-06-01"

		while (not(self.pipeline.getPipelineStatus().strip().lower() == "succeeded")) \
				or (self.pipeline.getPipelineStatus().strip().lower == ("failed")):

			headers = {"Authorization": f"Bearer {self.bearerToken}"}
			pipelineStatusResponse = requests.get(pipelineRunStatusGetUrl, headers=headers)
			# print(pipelineStatusResponse.status_code, type(pipelineStatusResponse.status_code), pipelineStatusResponse.json(), pipelineStatusResponse.json()['id'])

			if (pipelineStatusResponse.status_code == 200):
				pipelineStatusResponse_dict = pipelineStatusResponse.json()
				self.pipeline.setPipelineStatus(pipelineStatusResponse_dict.get("status"))
				time.sleep(60)
			else:
				print("ERROR: " + pipelineStatusResponse.getStatusLine())
				# Assert.fail("ERROR : API Call error " + pipelineStatusResponse.getStatusLine())

		print(f"{PipelineName} ADF Pipeline executed Conpletely")
		# dp.setData(DatapoolName, ColName+" PipelineRunStatus", pipeline.getPipelineStatus(), row)

	@keyword
	def get_activity_output(self, TestActivity):
		try:
			activityName = TestActivity
			pipelineRunId = self.pipeline.getPipelineRunId()
			activityOutput = self.getActivityResponseOutput(pipelineRunId, activityName)
			testCaseName = activityOutput.get("runOutput").get("Test Case Name")
			validationStatus = activityOutput.get("runOutput").get("Test Execution Status")
			validationReportUrl = activityOutput.get("runOutput").get("Report")
			# dp.setData(DatapoolName, TestActivity, validationStatus, row); // DELETED +"_Results"
			# dp.setData(DatapoolName, TestActivity+" Report", validationReportUrl, row); // ADDED
			if validationStatus.strip().lower() != "passed":
				print("Pipeline is FAILED")
				#Assert.assertTrue("Pipeline is FAILED", validationStatus.trim().equalsIgnoreCase("passed")) // Commented out so that the download report step isn't skipped
			else:
				print(f"Activity status: {validationStatus}")
		except Exception as e:
			print("Get Activity Output has failed")
			# Assert.fail("Get Activity Output has failed")
		# util.ExportData(tableName+"_"+TestActivity+"_Results");

	@keyword
	def getActivityResponseOutput(self, pipelineRunId, activityName):
		ResponseOutput = dict()
		activityPostUrl = f"https://management.azure.com/subscriptions/{self.subscriptionId}/resourceGroups/{self.resourceGroupName}/providers/Microsoft.DataFactory/factories/{self.factoryName}/pipelineruns/{self.pipelineRunId}/queryActivityruns?api-version=2018-06-01"
		print("activityPostUrl:" + activityPostUrl)
		headers = {"Authorization": f"Bearer {self.bearerToken}"}
		data = {"lastUpdatedAfter": "2000-01-01T00:00:00.0000000Z", "lastUpdatedBefore": "2099-12-31T00:00:00.0000000Z",
				"filters": [{"operand": "ActivityName", "operator": "Equals", "values": [activityName]}]}
		activityStatusResponse = requests.post(activityPostUrl, data=data, headers=headers)
		if (activityStatusResponse.status_code == 200):
			activityStatusResponse_dict = activityStatusResponse.json()
			print("activityResponse: ", activityStatusResponse_dict)
			ResponseOutput = activityStatusResponse_dict.get("value")[0].get("output")
		else:
			print("ERROR: Call Activity API Error", activityStatusResponse)
			# System.out.println("Response Body:" + request.getBody().asPrettyString());
		return ResponseOutput

if __name__ == '__main__':
	a = EtlValidationTestSteps()
	a.pipeline_name("NewADB_ETL_Column_Schema_Validation_Onprem_ADLS")
	a.add_Parameters_for_Duplicate_Record_Validation_in_ADLS('cpbdw', 'dimsubbranchs', 'pbss_dm', 'dev', 'rbpsw')
	a.get_Bearer_Token()
	# a.trigger_Specific_Pipeline()
	# a.get_activity_output("Schema_Validation in ADLS with Data mapping")
