from enum import auto
from lib2to3.pgen2 import driver
from pydoc import doc
import time, json
import DatabricksAPIHelper as api
import pyspark

#region begin Usage
#cluster.stopRunningDatabricksClusters(db)
#defaultCluster = getCluster(db, "Default")
#storageAccountHash = cluster.setStorageAccountHash(bronzeStorageAccountName="ktkdmdpe16adlsb", silverStorageAccountName="ktkdmdpe16adlssg", sandboxStorageAccountName="ktkdmdpe16adlssb")
#cluster.newDatabricksCluster(db, token=args.access_token, clusterName="test_standard", autoTerminationMinutes=10, sparkVersion="10.2.x-cpu-ml-scala2.12", nodeType="Standard_DS3_v2", driverNodeType="Standard_DS3_v2", maxWorkers=2, clusterMode="Standard", ssh_public_keys=None, custom_tags=None, spark_env_vars=None, enableElasticDisk=False, minWorkers=1, initScriptPath="dbfs:/databricks/pyodbc.sh", instancePoolId="0201-190217-croup838-pool-acuhu0tv", storageAccountHash=storageAccountHash, cloud="Azure", applyPolicyDefaultValues=True, enableLocalDiskEncryption=True)
#cluster.newDatabricksCluster(db, token=args.access_token, clusterName="test_highconcurrency_tableaccesscontrol", autoTerminationMinutes=10, sparkVersion="10.2.x-scala2.12", nodeType="Standard_DS3_v2", driverNodeType="Standard_DS3_v2", maxWorkers=2, clusterMode="HighConcurrency", ssh_public_keys=None, custom_tags=None, spark_env_vars=None, enableElasticDisk=False, minWorkers=1, initScriptPath="", instancePoolId="0201-190217-croup838-pool-acuhu0tv", storageAccountHash=storageAccountHash, cloud="Azure", applyPolicyDefaultValues=True, enableLocalDiskEncryption=True, enableTableAccessControl=True, enableADLSCredentialPassthrough=False)
#cluster.newDatabricksCluster(db, token=args.access_token, clusterName="test_highconcurrency_adlspassthrough", autoTerminationMinutes=10, sparkVersion="10.2.x-cpu-ml-scala2.12", nodeType="Standard_DS3_v2", driverNodeType="Standard_DS3_v2", maxWorkers=2, clusterMode="HighConcurrency", ssh_public_keys=None, custom_tags=None, spark_env_vars=None, enableElasticDisk=False, minWorkers=1, initScriptPath="", instancePoolId="0201-190217-croup838-pool-acuhu0tv", storageAccountHash=storageAccountHash, cloud="Azure", applyPolicyDefaultValues=True, enableLocalDiskEncryption=True, enableTableAccessControl=False, enableADLSCredentialPassthrough=True)
#cluster.newDatabricksCluster(db, token=args.access_token, clusterName="test_singlenode", autoTerminationMinutes=10, sparkVersion="10.2.x-cpu-ml-scala2.12", nodeType="Standard_DS3_v2", driverNodeType="Standard_DS3_v2", maxWorkers=1, clusterMode="SingleNode", ssh_public_keys=None, custom_tags=None, spark_env_vars=None, enableElasticDisk=False, minWorkers=0, initScriptPath="dbfs:/databricks/pyodbc.sh", instancePoolId="0201-190217-croup838-pool-acuhu0tv", storageAccountHash=storageAccountHash, cloud="Azure", applyPolicyDefaultValues=True, enableLocalDiskEncryption=True)
#cluster.stopRunningDatabricksClusters(db)
#cluster.removeCluster(db, clusterName="test_standard")
#cluster.removeCluster(db, clusterName="test_highconcurrency_tableaccesscontrol")
#cluster.removeCluster(db, clusterName="test_highconcurrency_adlspassthrough")
#cluster.removeCluster(db, clusterName="test_singlenode")
#endregion

# region begin Clusters (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksClusters.ps1)
def getCluster(db, clusterName):
    clusters = db.cluster.list_clusters(headers=None)
    return [c for c in clusters["clusters"] if c["cluster_name"] == clusterName]

# removeDatabricksCluster
def removeCluster(db, clusterName):
    cluster = getCluster(db, clusterName)
    if len(cluster) > 0:
        print("{0} : Removing cluster {1}".format(time.ctime(), clusterName))
        db.cluster.permanent_delete_cluster(cluster_id=cluster[0]["cluster_id"])
    else:
        print("cluster not found")

# newDatabricksCluster
def newDatabricksCluster(db, token, clusterName, autoTerminationMinutes, sparkVersion, nodeType, driverNodeType, maxWorkers, clusterMode="Standard", ssh_public_keys=None, custom_tags=None, spark_env_vars=None, enableElasticDisk=False, minWorkers=0, initScriptPath=None, dockerImage=None, instancePoolId=None, driverInstancePoolId=None, storageAccountHash = {}, cloud="Azure", applyPolicyDefaultValues=True, enableLocalDiskEncryption=True, enableTableAccessControl=False, enableADLSCredentialPassthrough=False):
    cluster = getCluster(db, clusterName)
    if len(cluster) == 0:
        autoscale, num_workers = api.setWorkers(minWorkers, maxWorkers)
        spark_conf = api.setSparkConf(storageAccountHash, clusterMode=clusterMode, enableTableAccessControl=enableTableAccessControl, enableADLSCredentialPassthrough=enableADLSCredentialPassthrough)
        spark_env_vars = api.setSparkEnvVars(spark_env_vars)
        nodeType, driverNodeType, enableElasticDisk, instancePoolId, driverInstancePoolId = api.setSparkNodes(nodeType, driverNodeType, enableElasticDisk, instancePoolId, driverInstancePoolId)
        cloudSpecificAttributes = api.setCloudSpecificAttributes(cloud=cloud, first_on_demand=1, availability="SPOT_WITH_FALLBACK", zone_id="auto", instance_profile_arn=None, spot_bid_price_percent=100, spot_bid_max_price=-1, ebs_volume_type="GENERAL_PURPOSE_SSD", ebs_volume_count=None, ebs_volume_size=None, ebs_volume_iops=None, ebs_volume_throughput=None)
        cluster_log_conf = api.setClusterLogConf()

        body = {}
        if autoscale is not None:
            body["autoscale"] = autoscale
        else:
            body["num_workers"] = num_workers
        body["cluster_name"] = clusterName
        body["spark_version"] = sparkVersion
        body["spark_conf"] = spark_conf
        if cloudSpecificAttributes is not None:
            if cloud == "Azure":
                body["azure_attributes"] = cloudSpecificAttributes
            elif cloud == "AWS":
                body["aws_attributes"] = cloudSpecificAttributes
        if nodeType is not None:
            body["node_type_id"] = nodeType
        if driverNodeType is not None:
            body["driver_node_type_id"] = driverNodeType
        if ssh_public_keys is not None:
            body["ssh_public_keys"] = ssh_public_keys
        if custom_tags is not None:
            body["custom_tags"] = custom_tags
        if cluster_log_conf is not None:
            body["cluster_log_conf"] = cluster_log_conf
        if initScriptPath is not None:
            init_scripts = api.setInitScriptPath(initScriptPath)
            body["init_scripts"] = init_scripts
        if dockerImage is not None:
            body["docker_image"] = dockerImage
        body["spark_env_vars"] = spark_env_vars
        body["autotermination_minutes"] = autoTerminationMinutes
        if enableElasticDisk is not None:
            if cloud == "AWS":
                body["enable_elastic_disk"] = enableElasticDisk
        if driverInstancePoolId is not None:
            body["driver_instance_pool_id"] = driverInstancePoolId
        if instancePoolId is not None:
            body["instance_pool_id"] = instancePoolId
        body["apply_policy_default_values"] = "true"
        body["enable_local_disk_encryption"] = "true"
        data = json.dumps(body)
        uri = "{0}{1}/clusters/create".format(db.client.url, db.client.api_version)
        response = api.invokePOSTRESTRequest(uri, token, data)
        return data, uri, token, response
    else:
        print("Cluster already exists")

# newDatabricksHighConcurrencyCluster
# newDatabricksSingleNodeCluster

# startDatabricksCluster
def startDatabricksCluster(db, clusterName):
    cluster = getCluster(db, clusterName)
    if len(cluster) > 0:
        print("{0} : Starting cluster {1}".format(time.ctime(), clusterName))
        db.cluster.start_cluster(cluster_id=cluster[0]["cluster_id"])
    else:
        print("cluster not found")

# startDatabricksClusterAwaitUntilStarted
def startDatabricksClusterAwaitUntilStarted(db, clusterName):
    cluster = getCluster(db, clusterName)
    if len(cluster) > 0:
        if cluster[0]["state"] == "TERMINATED":
            print("{0} : Starting cluster {1}".format(time.ctime(), clusterName))
            startDatabricksCluster(db, clusterName)
            time.sleep(5)
        while True:
            cluster = getCluster(db, clusterName)
            if cluster[0]["state"] not in ["PENDING"]:
                break
            else:
                print("{0} : Starting cluster {1} - State: {2}".format(time.ctime(), clusterName, cluster[0]["state"]))
            time.sleep(60)
    else:
        print("cluster not found")
        
# stopDatabricksCluster
def stopDatabricksCluster(db, clusterName):
    cluster = getCluster(db, clusterName)
    if len(cluster) > 0:
        print("{0} : Stopping cluster {1}".format(time.ctime(), clusterName))
        db.cluster.delete_cluster(cluster_id=cluster[0]["cluster_id"])
    else:
        print("cluster not found")

# stopRunningDatabricksClusters
def stopRunningDatabricksClusters(db, include_job_clusters=False):
    clusters = db.cluster.list_clusters()
    for cluster in clusters["clusters"]:
        if cluster["state"] != "TERMINATED" and (cluster["cluster_source"] != "JOB" or include_job_clusters==True):
            stopDatabricksCluster(db, cluster["cluster_name"])
    
# endregion