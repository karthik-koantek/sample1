import time 
from ClusterModule import getCluster

#region begin Usage
#temporaily manually assign instance_pool_id until instance pools module exists
#instance_pool_id ="0217-210730-kneed642-pool-vsj0jg6r"
#token = "dapib7d3b27d33536aa71dc63ceee778b67e"
#library_statuses = library.getClusterLibraryStatuses(db, cluster_id=None)
#defaultCluster = cluster.getCluster(db, clusterName="Default")
#library_statuses = library.getClusterLibraryStatuses(db, cluster_id=defaultCluster[0]["cluster_id"])
#libraries = library_statuses[0]["library_statuses"]
#libraries_by_type = library.getLibrariesByType(libraries)
#maven_xml = library.newMavenLibrary("com.databricks:spark-xml_2.12:0.10.0")
#pypi_ge = library.newPythonPyPiOrRCranLibrary("pypi", "great_expectations")
#ktk = library.newJAREggOrWhlLibrary("whl", "dbfs:/mnt/libraries/wheel-libraries.wheelhouse.zip")
#libraries = [maven_xml, pypi_ge]
#storageAccountHash = cluster.setStorageAccountHash(bronzeStorageAccountName="ktkdmdpe16adlsb", silverStorageAccountName="ktkdmdpe16adlssg", sandboxStorageAccountName="ktkdmdpe16adlssb")
#data, uri, token, response = cluster.newDatabricksCluster(db, token=token, clusterName="test_standard", autoTerminationMinutes=10, sparkVersion="10.2.x-cpu-ml-scala2.12", nodeType="Standard_DS3_v2", driverNodeType="Standard_DS3_v2", maxWorkers=2, clusterMode="Standard", ssh_public_keys=None, custom_tags=None, spark_env_vars=None, enableElasticDisk=False, minWorkers=1, initScriptPath="dbfs:/databricks/pyodbc.sh", instancePoolId=instance_pool_id, storageAccountHash=storageAccountHash, cloud="Azure", applyPolicyDefaultValues=True, enableLocalDiskEncryption=True)
#cluster.startDatabricksClusterAwaitUntilStarted(db, "test_standard")
#test_standard_cluster = cluster.getCluster(db, clusterName="test_standard")
#db.managed_library.install_libraries(cluster_id=test_standard_cluster[0]["cluster_id"], libraries=libraries)
#libraries = [ktk]
#db.managed_library.uninstall_libraries(cluster_id=test_standard_cluster[0]["cluster_id"], libraries=libraries)

#endregion

# region begin Libraries (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksLibraries.ps1)

def newJAREggOrWhlLibrary(type, uri):
    if type in ["jar", "egg", "whl"]:
        return {type: uri}
    else:
        return "Library must be of type jar, egg, or whl"

def newPythonPyPiOrRCranLibrary(type, package, repo=None):
    if type in ["pypi", "cran"]:
        lib = {}
        lib["package"] = package 
        if repo is not None:
            lib["repo"] = repo 
        return {type: lib}
    else:
        return "Library must be of type pypi or cran"

def newMavenLibrary(coordinates, repo=None, exclusions=None):
    maven = {}
    maven["coordinates"] = coordinates
    if repo is not None:
        maven["repo"] = repo 
    if exclusions is not None:
        maven["exclusions"] = exclusions
    return {"maven": maven}

def getLibrariesByType(libraries):
    def typeHelper(type, libraries):
        return [c["library"].get(type) for c in libraries if c["library"].get(type) is not None]
    types = ["maven", "pypi", "jar", "egg", "whl", "cran"]
    l = {}
    l["maven"] = typeHelper("maven", libraries)
    l["pypi"] = typeHelper("pypi", libraries)
    l["jar"] = typeHelper("jar", libraries)
    l["egg"] = typeHelper("egg", libraries)
    l["whl"] = typeHelper("whl", libraries) 
    l["cran"] = typeHelper("cran", libraries)
    return l

# getClusterLibraryStatuses
def getClusterLibraryStatuses(db, cluster_id=None, headers=None):
    status = db.managed_library.all_cluster_statuses(headers=headers)
    if cluster_id is None:
        return status.get("statuses")
    else:
        return [c for c in status.get("statuses") if c.get("cluster_id") == cluster_id]

# # endregion