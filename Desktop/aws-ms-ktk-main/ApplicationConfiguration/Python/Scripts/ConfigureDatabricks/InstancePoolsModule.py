from cgitb import enable
from enum import auto
from lib2to3.pgen2 import driver
from pydoc import doc
import time, json
import DatabricksAPIHelper as api
import pyspark

#region begin Usage
#pools = db.instance_pool.list_instance_pools()
#defaultPool = pool.getInstancePool(db, "Default")
#pool.newInstancePool(db, instancePoolName="testPool2", nodeTypeId="Standard_DS3_v2", minIdleInstances=0, maxCapacity=3, idleInstanceAutoterminationMinutes=10, preloadedSparkVersions="10.2.x-cpu-ml-scala2.12")
#pool.removeInstancePool(db, "testPool")
#endregion

# region begin InstancePools (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksInstancePools.ps1)
def getInstancePool(db, instancePoolName):
    pools = db.instance_pool.list_instance_pools(headers=None)
    return [c for c in pools["instance_pools"] if c["instance_pool_name"] == instancePoolName]

# removeInstancePool
def removeInstancePool(db, instancePoolName):
    pool = getInstancePool(db, instancePoolName)
    if len(pool) > 0:
        print("{0} : Removing instance pool {1}".format(time.ctime(), instancePoolName))
        db.instance_pool.delete_instance_pool(instance_pool_id=pool[0]["instance_pool_id"])
    else:
        print("pool not found")

# newInstancePool
def newInstancePool(db, instancePoolName, nodeTypeId, minIdleInstances=None, maxCapacity=None, awsAttributes=None, customTags=None, idleInstanceAutoterminationMinutes=None, enableElasticDisk=None, diskSpec=None, preloadedSparkVersions=None, cloud="Azure"):
    pool = getInstancePool(db, instancePoolName)
    if len(pool) > 0:
        print("{0} : Editing instance pool {1}".format(time.ctime(), instancePoolName))
        db.instance_pool.edit_instance_pool(instance_pool_id=pool[0]["instance_pool_id"], instance_pool_name=instancePoolName, min_idle_instances=minIdleInstances, max_capacity=maxCapacity, aws_attributes=awsAttributes, node_type_id=nodeTypeId, custom_tags=customTags, idle_instance_autotermination_minutes=idleInstanceAutoterminationMinutes, enable_elastic_disk=enableElasticDisk, disk_spec=diskSpec, preloaded_spark_versions=preloadedSparkVersions)
    else:
        print("{0} : Creating instance pool {1}".format(time.ctime(), instancePoolName))
        db.instance_pool.create_instance_pool(instance_pool_name=instancePoolName, min_idle_instances=minIdleInstances, max_capacity=maxCapacity, aws_attributes=awsAttributes, node_type_id=nodeTypeId, custom_tags=customTags, idle_instance_autotermination_minutes=idleInstanceAutoterminationMinutes, enable_elastic_disk=enableElasticDisk, disk_spec=diskSpec, preloaded_spark_versions=preloadedSparkVersions)

# endregion