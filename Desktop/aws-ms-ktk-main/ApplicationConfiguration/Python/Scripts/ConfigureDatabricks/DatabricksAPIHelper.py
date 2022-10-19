import time 
import requests

# invokePOSTRESTRequest
def invokePOSTRESTRequest(uri, token, body, expectedStatusCode=200):
    bearerToken = "Bearer {0}".format(token)
    headers = {'Authorization': bearerToken}
    
    try:
        response = requests.post(url=uri, headers=headers, data=body)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print(errh)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
    except requests.exceptions.Timeout as errt:
        print(errt)
    except requests.exceptions.RequestException as err:
        print(err)

    if response.status_code != expectedStatusCode:
        print("InvokePOSTRESTRequest failed")
    return response

# setCloudSpecificAttributes 
def setCloudSpecificAttributes(cloud="Azure", first_on_demand=1, availability="SPOT_WITH_FALLBACK", zone_id="auto", instance_profile_arn=None, spot_bid_price_percent=100, spot_bid_max_price=-1, ebs_volume_type="GENERAL_PURPOSE_SSD", ebs_volume_count=None, ebs_volume_size=None, ebs_volume_iops=None, ebs_volume_throughput=None):
    if cloud == "Azure":
        if availability == "SPOT_WITH_FALLBACK":
            availability = "SPOT_WITH_FALLBACK_AZURE"
        azure_attributes = {
            "first_on_demand": first_on_demand,
            "availability": availability,
            "spot_bid_max_price": spot_bid_max_price
        }
        return {k: v for k, v in azure_attributes.items() if v is not None}
    elif cloud == "AWS":
        aws_attributes = {
            "first_on_demand": first_on_demand,
            "availability": availability,
            "zone_id": zone_id,
            "instance_profile_arn": instance_profile_arn,
            "spot_bid_price_percent": spot_bid_price_percent,
            "ebs_volume_type": ebs_volume_type,
            "ebs_volume_count": ebs_volume_count,
            "ebs_volume_size": ebs_volume_size,
            "ebs_volume_iops": ebs_volume_iops,
            "ebs_volume_throughput": ebs_volume_throughput
        }
        return {k: v for k, v in aws_attributes.items() if v is not None}

# setClusterLogConf
def setClusterLogConf(type="dbfs", location="dbfs:/home/cluster_log", region=None):
    cluster_log_conf = {}
    if type=="dbfs":
        dbfs = {
            "destination": location
        }
        cluster_log_conf["dbfs"] = dbfs
    else:
        s3 = {
                "destination": location,
                "region": region
        }
        cluster_log_conf["s3"] = s3
    return cluster_log_conf

# setInitScriptPath
def setInitScriptPath(initScript):
    dbfs = {"destination": initScript}
    initScripts = {}
    initScripts["dbfs"] = dbfs
    init_scripts = [initScripts]
    return init_scripts
    
# setSparkNodes
def setSparkNodes(nodeType, driverNodeType, enableElasticDisk, instancePoolId=None, driverInstancePoolId=None):
    if instancePoolId is None:
        return nodeType, driverNodeType, enableElasticDisk, None, None
    else:
        if driverInstancePoolId is None:
            driverInstancePoolId = instancePoolId
        return None, None, None, instancePoolId, driverInstancePoolId
        
# setSparkConf
def setSparkConf(storageAccountHash={}, clusterMode="Standard", enableTableAccessControl=False, enableADLSCredentialPassthrough=False):
    spark_conf = {}
    if clusterMode=="Standard":
        spark_conf["spark.scheduler.mode"]="FAIR"
        spark_conf["spark.sql.adaptive.enabled"] = "true"
    elif clusterMode=="HighConcurrency":
        spark_conf["spark.databricks.cluster.profile"] = "serverless"
        spark_conf["spark.databricks.pyspark.enableProcessIsolation"] = "true"
        spark_conf["spark.databricks.repl.allowedLanguages"] = "python,sql"
        spark_conf["spark.sql.adaptive.enabled"] = "true"
        if enableTableAccessControl==True:
            spark_conf["spark.databricks.acl.dfAclsEnabled"] = "true"
        if enableADLSCredentialPassthrough==True:
            spark_conf["spark.databricks.passthrough.enabled"] = "true"
    elif clusterMode=="SingleNode":   
        spark_conf["spark.databricks.cluster.profile"] = "singleNode"
        spark_conf["spark.master"] = "local[*]"
        spark_conf["spark.databricks.delta.preview.enabled"] = "true"
    
    for s in storageAccountHash:
        spark_conf[s]=storageAccountHash[s]

    return spark_conf

# setSparkEnvVars
def setSparkEnvVars(otherEnvVars={}, pyspark_python="/databricks/python3/bin/python3"):
    spark_env_vars = {
        "pyspark_python": pyspark_python
    }
    if otherEnvVars is not None:
        for ev in otherEnvVars:
            spark_env_vars[ev]=otherEnvVars[ev]
    return spark_env_vars

# setStorageAccountHash
def setStorageAccountHash(bronzeStorageAccountName, silverStorageAccountName, sandboxStorageAccountName):
    fs = "spark.hadoop.fs.azure.account.key.{0}.dfs.core.windows.net"
    bronzeStorageAccountFSName = fs.format(bronzeStorageAccountName)
    silverGoldStorageAccountFSName = fs.format(silverStorageAccountName)
    sandboxStorageAccountFSName = fs.format(sandboxStorageAccountName)
    storageAccountHash = {
        bronzeStorageAccountFSName: "{{secrets/internal/BronzeStorageAccountKey}}",
        silverGoldStorageAccountFSName: "{{secrets/internal/SilverGoldStorageAccountKey}}",
        sandboxStorageAccountFSName: "{{secrets/internal/SandboxStorageAccountKey}}"
    }
    return storageAccountHash

# setWorkers
def setWorkers(minWorkers, maxWorkers):
    if minWorkers == 0:
        autoscale=None
        num_workers = maxWorkers
    else:
        num_workers = None
        autoscale = {
            "min_workers": minWorkers,
            "max_workers": maxWorkers
        }
    return autoscale, num_workers

# region begin DBFS (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksDBFS.ps1)

# importLocalFileToDBFS

# endregion 

# region begin Delta Live Tables (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksDeltaLiveTables.ps1)

# newDeltaLiveTablePipeline
# removeDeltaLiveTablePipeline
# updateDeltaLiveTablePipeline
# startDeltaLiveTablePipeline
# getDeltaLiveTablePipelineUpdateDetails
# getDeltaLiveTablePipelineEvents
# stopDeltaLiveTablePipeline

# endregion 

# region begin Security (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksGroups.ps1)
# addDatabricksGroup
# getDatabricksGroupMembers
# addDatabricksGroupMembers
# addDatabricksGroupMember
# removeDatabricksGroupMember
# newSCIMUser

# endregion

# region begin Instance Pools (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksInstancePools.ps1)
# newInstancePool
# removeInstancePool
# getInstancePool

# endregion 

# region begin MLFlow (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksMLFlow.ps1)
# None
# endregion

# region begin Permissions (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksPermissions.ps1)
# None
# endregion

# region begin Secrets (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksSecrets.ps1)
# setDatabricksSecretACLForPrincipal
# newDatabricksSecretScope
# addDatabricksSecretScope
# removeDatabricksSecretScope
# removeDatabricksSecret
# removeDatabricksSecretACLForPrincipal

# endregion

# region begin SQL (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksSQL.ps1)
# getSQLEndpoint
# removeSQLEndpoint
# editGlobalSQLEndpointConfiguration
# newSQLEndpoint
# editSQLEndpoint
# deploySQLEndpoint

# query endpoints
# dashboard endpoints

# endregion

# region begin Tokens (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksTokens.ps1)
# getDatabricksToken
# newDatabricksToken
# revokeDatabricksToken

# endregion

# region begin Workspace (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksWorkspace.ps1)

# importNotebook
# importNotebookDirectory
# exportNotebook
# removeDatabricksWorkspaceFolder
# exportNotebookDirectory
# exportNotebookDirectoryRecursive

# endregion