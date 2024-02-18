class Pipeline:
    def __init__(self):
        self.token = ''
        self.pipelineStatus = ''
        self.pipelineName = ''
        self.malcode = ''
        self.entityName = ''
        self.sourceFileName = ''
        self.partition = ''
        self.env = ''
        self.pipelineRunId = ''
        self.datamart = ''
        self.OnPremColumnCount = 0
        self.job_id = ''
        self.custom_parameter = ''
        self.effective_start_date = ''
        self.effective_end_date = ''
        self.job_name = ''
        self.pod = ''
        self.expectedSchemaFilePath = ''
        self.isDataTypeValidation = ''
        self.container = ''
        self.DBname = ''
        self.servername = ''
        self.processing_pipeline_starttime = ''
        self.processing_pipeline_run_id = ''
        self.schemaName = ''
        self.tableName = ''
        self.payload = {}

    def getExpectedSchemaFilePath(self):
        return self.expectedSchemaFilePath

    def setExpectedSchemaFilePath(self, expectedSchemaFilePath):
        self.expectedSchemaFilePath = expectedSchemaFilePath

    def getIsDataTypeValidation(self):
        return self.isDataTypeValidation

    def setIsDataTypeValidation(self, isDataTypeValidation):
        self.isDataTypeValidation = isDataTypeValidation

    def getContainer(self):
        return self.container

    def setContainer(self, container):
        self.container = container

    def getDBname(self):
        return self.DBname

    def setDBname(self, DBname):
        self.DBname = DBname

    def getServername(self):
        return self.servername

    def setServername(self, servername):
        self.servername = servername

    def getJob_id(self):
        return self.job_id

    def setJob_id(self, job_id):
        self.job_id = job_id

    def getCustom_parameter(self):
        return self.custom_parameter

    def setCustom_parameter(self, custom_parameter):
        self.custom_parameter = custom_parameter

    def getEffective_start_date(self):
        return self.effective_start_date

    def setEffective_start_date(self, effective_start_date):
        self.effective_start_date = effective_start_date

    def getEffective_end_date(self):
        return self.effective_end_date

    def setEffective_end_date(self, effective_end_date):
        self.effective_end_date = effective_end_date

    def getJob_name(self):
        return self.job_name

    def setJob_name(self, job_name):
        self.job_name = job_name

    def getPod(self):
        return self.pod

    def setPod(self, pod):
        self.pod = pod

    def getSchemaName(self):
        return self.schemaName

    def setSchemaName(self, schemaName):
        self.schemaName = schemaName

    def getTableName(self):
        return self.tableName

    def setTableName(self, tableName):
        self.tableName = tableName

    def getPipelineRunId(self):
        return self.pipelineRunId

    def setPipelineRunId(self, pipelineRunId):
        self.pipelineRunId = pipelineRunId

    def getPayload(self):
        return self.payload

    def setPayload(self, payload):
        self.payload = payload

    def getMalcode(self):
        return self.malcode

    def setMalcode(self, malcode):
        self.malcode = malcode

    def getEntityName(self):
        return self.entityName

    def setEntityName(self, entityName):
        self.entityName = entityName

    def getSourceFileName(self):
        return self.sourceFileName

    def setSourceFileName(self, sourceFileName):
        self.sourceFileName = sourceFileName

    def getPartition(self):
        return self.partition

    def setPartition(self, partition):
        self.partition = partition

    def getEnv(self):
        return self.env

    def setEnv(self, env):
        self.env = env

    def getPipelineName(self):
        return self.pipelineName

    def setPipelineName(self, pipelineName):
        self.pipelineName = pipelineName

    def getPipelineStatus(self):
        return self.pipelineStatus

    def setPipelineStatus(self, pipelineStatus):
        self.pipelineStatus = pipelineStatus

    def getToken(self):
        return self.token

    def setToken(self, token):
        self.token = token

    def getOnPremColumnCount(self):
        return self.OnPremColumnCount

    def setOnPremColumnCount(self, OnPremColumnCount):
        self.OnPremColumnCount = OnPremColumnCount

    def setDatamart(self, datamart):
        self.datamart = datamart

    def getDatamart(self):
        return self.datamart

    def getProcessing_pipeline_starttime(self):
        return self.processing_pipeline_starttime

    def setProcessing_pipeline_starttime(self, processing_pipeline_starttime):
        self.processing_pipeline_starttime = processing_pipeline_starttime

    def getProcessing_pipeline_run_id(self):
        return self.processing_pipeline_run_id

    def setProcessing_pipeline_run_id(self, processing_pipeline_run_id):
        self.processing_pipeline_run_id = processing_pipeline_run_id

if __name__ == '__main__':
    abc = Pipeline()