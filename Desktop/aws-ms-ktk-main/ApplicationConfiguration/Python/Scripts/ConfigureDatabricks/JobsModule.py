import email
from email.mime import base
from enum import auto
from lib2to3.pgen2 import driver
from pydoc import doc
import time, json
import DatabricksAPIHelper as api
import pyspark

#region begin Usage 
#myjob = jobs.getDatabricksJob(db, "UAT_ADX")
#jobs.runDatabricksJob(db, myjob[0]["settings"]["name"])
#runs = jobs.getDatabricksJobRunsStatus(db, myjob[0]["settings"]["name"])
#db.jobs.cancel_run(run_id=runs["runs"][0]["run_id"])
#jobs.removeDatabricksJob(db, "UAT_ADX")
#emailNotifications = jobs.newEmailNotification(onFailure=["edward.edgeworth@koantek.com"])
#spark3sql = library.newMavenLibrary(coordinates="com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")
#frameworkWhl = library.newJAREggOrWhlLibrary(type="whl", uri="dbfs:/FileStore/jars/ktk-0.0.2-py3-none-any.whl")
#storageAccountHash = helper.setStorageAccountHash(bronzeStorageAccountName="ktkdmdpe16adlsb", silverStorageAccountName="ktkdmdpe16adlssg", sandboxStorageAccountName="ktkdmdpe16adlssb")
#sparkConf = helper.setSparkConf(storageAccountHash=storageAccountHash)
#parameters = {
#    "projectName": "UAT_ML",
#    "threadPool": 1,
#    "timeoutSeconds": 18000,
#    "whatIf": 0
#}
#newClusterSettings = jobs.newClusterSettings(nodeType="Standard_DS3_v2", sparkVersion="10.2.x-cpu-ml-scala2.12", storageAccountHash=storageAccountHash, maxWorkers=1, sparkConf=sparkConf, instancePoolId="0217-210730-kneed642-pool-vsj0jg6r", initScript="dbfs:/databricks/pyodbc.sh")
#notebookTaskSettings = jobs.newNotebookTaskSettings(notebookPath="/Framework/Orchestration/Orchestration", baseParameters=parameters)
#uat_ml = jobs.newTaskSettings(taskKey="UAT_ML", description="UAT_ML Project", EmailNotification=emailNotifications, newClusterSettings=newClusterSettings, notebookTaskSettings=notebookTaskSettings, libraries=[spark3sql, frameworkWhl])
#jobs.newDatabricksJob(db, "UAT_ML_test", [uat_ml])
#jobs.runUATJobs(db)
#endregion

# region begin Jobs (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksJobs.ps1)
# getDatabricksJob
def getDatabricksJob(db, jobName):
    jobs = db.jobs.list_jobs()
    return [j for j in jobs["jobs"] if jobName in j["settings"]["name"]]

# getDatabricksJobActiveRuns
def getDatabricksJobActiveRuns(db, jobName):
    job = getDatabricksJob(db, jobName)
    if len(job) > 0:
        return db.jobs.list_runs(job_id=job[0]["job_id"], active_only=True) 
    else:
        print("Job not found")

# getDatabricksJobRunsStatus
def getDatabricksJobRunsStatus(db, jobName, waitForCompletion=True):
    job = getDatabricksJob(db, jobName)
    if len(job) > 0:
        runs = db.jobs.list_runs(job_id=job[0]["job_id"])
        if waitForCompletion==True:
            latestRun = runs["runs"][0]
            while latestRun["state"]["life_cycle_state"] not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                time.sleep(60)
                runs = db.jobs.list_runs(job_id=job[0]["job_id"])
                latestRun = runs["runs"][0]
                print("{0} : Running job {1} - State: {2}".format(time.ctime(), jobName, latestRun["state"]["life_cycle_state"]))
        return runs
    else:
        print("Job not found")    

# removeDatabricksJob
def removeDatabricksJob(db, jobName):
    job = getDatabricksJob(db, jobName)
    if len(job) > 0:
        print("{0} : Removing job {1}".format(time.ctime(), jobName))
        db.jobs.delete_job(job_id=job[0]["job_id"])
    else:
        print("Job not found")

# runDatabricksJob
def runDatabricksJob(db, jobName, jar_params=None, notebook_params=None, python_params=None, spark_submit_params=None):
    job = getDatabricksJob(db, jobName)
    if len(job) > 0:
        print("{0} : Running job {1}".format(time.ctime(), jobName))
        db.jobs.run_now(job_id=job[0]["job_id"], jar_params=jar_params, notebook_params=notebook_params, python_params=python_params, spark_submit_params=spark_submit_params)
    else:
        print("Job not found")

# newNotebookTaskSettings
def newNotebookTaskSettings(notebookPath, revisionTimestamp=None, baseParameters=None):
    notebookTask = {}
    notebookTask["notebook_path"] = notebookPath
    if revisionTimestamp is not None:
        notebookTask["revision_timestamp"] = revisionTimestamp
    if baseParameters is not None:
        notebookTask["base_parameters"] = baseParameters
    return notebookTask

# newPipelineTaskSettings
def newPipelineTaskSettings(pipelineId):
    pipelineTask = {}
    pipelineTask["pipeline_id"] = pipelineId
    return pipelineTask 

# newEmailNotification
def newEmailNotification(onStart=None, onSuccess=None, onFailure=None, noAlertForSkippedRuns=False):
    if onStart is None and onSuccess is None and onFailure is None:
        return 
    else:
        emailNotification = {"no_alert_for_skipped_runs": noAlertForSkippedRuns}
        if onStart is not None:
            emailNotification["on_start"] = onStart
        if onSuccess is not None:
            emailNotification["on_success"] = onSuccess
        if onFailure is not None:
            emailNotification["on_failure"] = onFailure
        return emailNotification

# newScheduleSettings
def newScheduleSettings(cronSchedule, timezoneId, paused=False):
    schedule = {
        "quartz_cron_expression": cronSchedule,
        "timezone_id": timezoneId
    }
    if paused == True:
        schedule["pause_status"] = "PAUSED"
    return newScheduleSettings

# newTaskSettings
def newTaskSettings(taskKey, description=None, dependsOn=None, timeoutSeconds=0, maxRetries=0, minRetryIntervalMilliseconds=0, retryOnTimeout=False, EmailNotification=None, noAlertForSkippedRuns=False, existingClusterId=None, newClusterSettings=None, notebookTaskSettings=None, pipelineTaskSettings=None, libraries=None):
    if existingClusterId is None and newClusterSettings is None:
        print("Cluster Details must be supplied")
        return
    if notebookTaskSettings is None and pipelineTaskSettings is None:
        print("Notebook or Pipeline details must be supplied")
        return 
    taskSettings = {
        "task_key": taskKey,
        "retry_on_timeout": retryOnTimeout
        }
    if dependsOn is not None:
        taskSettings["depends_on"] = dependsOn
    if libraries is not None:
        taskSettings["libraries"] = libraries
    if timeoutSeconds != 0:
        taskSettings["timeout_seconds"] = timeoutSeconds
    if maxRetries != 0:
        taskSettings["max_retries"] = maxRetries
    if minRetryIntervalMilliseconds != 0:
        taskSettings["min_retry_interval_millis"] = minRetryIntervalMilliseconds
    if EmailNotification is not None:
        taskSettings["email_notifications"] = EmailNotification
    if existingClusterId is not None:
        taskSettings["existing_cluster_id"] = existingClusterId
    if newClusterSettings is not None:
        taskSettings["new_cluster"] = newClusterSettings
    if notebookTaskSettings is not None:
        taskSettings["notebook_task"] = notebookTaskSettings
    if pipelineTaskSettings is not None:
        taskSettings["pipeline_task"] = pipelineTaskSettings
    return taskSettings 

# newClusterSettings
def newClusterSettings(nodeType, sparkVersion, storageAccountHash, maxWorkers, minWorkers=0, sparkConf=None, driverNodeType=None, instancePoolId=None, driverInstancePoolId=None, customTags=None, clusterLogPath="dbfs:/clusterlogs", initScript=None, sparkEnvVars=None, enableElasticDisk=True, cloud="Azure"):
    autoscale, num_workers = api.setWorkers(minWorkers, maxWorkers)
    spark_conf = api.setSparkConf(storageAccountHash)
    spark_env_vars = api.setSparkEnvVars(sparkEnvVars)
    nodeType, driverNodeType, enableElasticDisk, instancePoolId, driverInstancePoolId = api.setSparkNodes(nodeType, driverNodeType, enableElasticDisk, instancePoolId, driverInstancePoolId)
    cloudSpecificAttributes = api.setCloudSpecificAttributes(cloud=cloud, first_on_demand=1, availability="SPOT_WITH_FALLBACK", zone_id="auto", instance_profile_arn=None, spot_bid_price_percent=100, spot_bid_max_price=-1, ebs_volume_type="GENERAL_PURPOSE_SSD", ebs_volume_count=None, ebs_volume_size=None, ebs_volume_iops=None, ebs_volume_throughput=None)
    cluster_log_conf = api.setClusterLogConf()
    
    newCluster = {}
    if autoscale is not None:
        newCluster["autoscale"] = autoscale
    else:
        newCluster["num_workers"] = num_workers
    newCluster["spark_version"] = sparkVersion
    newCluster["spark_conf"] = spark_conf
    newCluster["spark_env_vars"] = spark_env_vars
    if nodeType is not None:
        newCluster["node_type_id"] = nodeType
    if driverNodeType is not None:
        newCluster["driver_node_type_id"] = driverNodeType
    if driverInstancePoolId is not None:
        if cloud == "Azure":
            newCluster["driver_instance_pool_id"] = driverInstancePoolId
    if instancePoolId is not None:
        newCluster["instance_pool_id"] = instancePoolId
    if driverInstancePoolId is not None:
        newCluster["driver_instance_pool_id"] = driverInstancePoolId
    if customTags is not None:
        newCluster["custom_tags"] = customTags
    if cluster_log_conf is not None:
        newCluster["cluster_log_conf"] = cluster_log_conf
    if initScript is not None:
        init_scripts = api.setInitScriptPath(initScript)
        newCluster["init_scripts"] = init_scripts
    if enableElasticDisk is not None:
        if cloud == "AWS":
            newCluster["enable_elastic_disk"] = enableElasticDisk
    return newCluster

# newDatabricksJob
def newDatabricksJob(db, jobName, tasks, clusters=None, timeoutSeconds=6000, maxConcurrentRuns=1, schedule=None, emailNotofication=None):
    job = getDatabricksJob(db, jobName)
    if len(job) == 0:
        print("{0} : Creating job {1}".format(time.ctime(), jobName))
        db.jobs.create_job(name=jobName, tasks=tasks, timeout_seconds=timeoutSeconds, max_concurrent_runs=maxConcurrentRuns, schedule=schedule, email_notifications=emailNotofication, version="2.1")
    else:
        print("{0} : Updating job {1}".format(time.ctime(), jobName))
        new_settings = {
            "name": job[0]["settings"]["name"],
            "tasks": tasks,
            "timeout_seconds": timeoutSeconds,
            "max_concurrent_runs": maxConcurrentRuns,
            "format": "MULTI_TASK"
        }
        if clusters is not None:
            new_settings["job_clusters"] = clusters
        if schedule is not None:
            new_settings["schedule"] = schedule 
        if emailNotofication is not None:
            new_settings["email_notifications"] = emailNotofication
        db.jobs.reset_job(job_id=job[0]["job_id"], new_settings=new_settings)

# startDatabricksNotebook
def startDatabricksNotebook(db, existingClusterId, notebookPath, notebookParams, runName):
    notebookTask = newNotebookTaskSettings(notebookPath=notebookPath, baseParameters=notebookParams)
    db.jobs.submit_run(run_name=runName, existing_cluster_id=existingClusterId, notebook_task=notebookTask)
        
# runUATJobs
def runUATJobs(db):
    uat_jobs = getDatabricksJob(db, "UAT_")
    terminalStates = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
    for j in uat_jobs:
        print("{0} : Running {1}".format(time.ctime(), j["settings"]["name"]))
        db.jobs.run_now(job_id=j["job_id"])
        while True:
            time.sleep(60)
            print("{0} : Running {1}".format(time.ctime(), j["settings"]["name"]))
            run = db.jobs.list_runs(job_id=j["job_id"])
            if run["runs"][0]["state"]["life_cycle_state"] in terminalStates:
                break


def overwriteSettingsForAJob(db, job_id, new_settings, headers=None, version=None):
    new_settings = new_settings.replace('\\', '/')
    file_format = new_settings.split('.')[-1]
    if file_format == 'json':
        with open(new_settings) as data:
            settings = json.load(data)
        db.jobs.reset_job(job_id, settings, headers, version)
        print(f"Successfully overwrote settings for job {job_id}.")
    else:
        print(f"Failed to overwrite settings for job {job_id}. Provided settings file is not in json format.")
    # try:
    #     if file_format == 'json':
    #         settings = json.load(new_settings)
    #         print(type(settings))
    #         db.jobs.reset_job(job_id, new_settings, heading=None, version=None)
    #         print(f"Successfully overwrote settings for job {job_id}.")
    #     else:
    #         print(f"Failed to overwrite settings for job {job_id}. Provided settings are not in json format.")
    # except Exception as e:
    #     print(f"Failed to overwrite settings for job {job_id}. Exception thrown: {str(e)}")

# endregion

# test overwrite job
# First create a dummy job in databricks and copy its job_id
# 317867011167757

# from databricks_api import DatabricksAPI
# db = DatabricksAPI(
#     host="https://dbc-ac23aef8-2a83.cloud.databricks.com/",
#     token= ""
# )

# overwriteSettingsForAJob(db, 462420503673266, 'C:\\Users\\Admin\\Downloads\\Brightline\\upload\\9th-April-DO NOT MODIFY Load_DocDB_Import_tables_v2.json')