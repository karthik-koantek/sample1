import argparse
from lib2to3.pgen2 import driver
from platform import node 
from databricks_api import DatabricksAPI
import InstancePoolsModule as pool
import ClusterModule as cluster
import ReposModule as repos
import LibraryModule as library
import JobsModule as jobs
import DatabricksAPIHelper as helper

#region begin parameters
parser = argparse.ArgumentParser(description='Databricks Information')
parser.add_argument('--host_name', dest='host_name', type=str, help='Databricks host')
parser.add_argument('--access_token', dest='access_token', type=str, help='Databricks access token')

args = parser.parse_args()
#endregion

db = DatabricksAPI(host=args.host_name, token=args.access_token)
#db = DatabricksAPI(host="https://westus2.azuredatabricks.net", token="dapib7d3b27d33536aa71dc63ceee778b67e")
#db = DatabricksAPI(host="https://adb-4447139959491338.18.azuredatabricks.net", token="dapib7d3b27d33536aa71dc63ceee778b67e")
# python3 ConfigureDatabricks.py --host_name "https://adb-4447139959491338.18.azuredatabricks.net" --access_token "dapi08c1f75027be5d96ee8b51f711bd7f00"
#region begin testing

#region begin instance pool testing 
#endregion

#region begin cluster testing
#endregion

#region begin job testing
jobs.runUATJobs(db)
#endregion

#region begin repo testing
#endregion

#region begin library testing
#endregion

#endregionyu9