#from enum import auto
#from lib2to3.pgen2 import driver
#from pydoc import doc
import time, json
#import DatabricksAPIHelper as api
#import pyspark

#region begin Usage
#print("creating pythonRepos")
#repos.newDatabricksRepo(db, url="https://dev.azure.com/koantek/_git/Azure%20Databricks", provider="azureDevOpsServices", path="/Repos/edward.edgeworth@koantek.com/pythonRepos")
#print("creating pythonRepos again (should fail gracefully)")
#repos.newDatabricksRepo(db, url="https://dev.azure.com/koantek/_git/Azure%20Databricks", provider="azureDevOpsServices", path="/Repos/edward.edgeworth@koantek.com/pythonRepos")
#myPythonRepo = repos.getDatabricksRepo(db, path_prefix="/Repos/edward.edgeworth@koantek.com/pythonRepos")
#print("updating pythonRepo to features/december")
#db.repos.update_repo(id=myPythonRepo[0]["id"], branch="features/december")
#print("deleting repo")
#repos.removeDatabricksRepo(db, repo_id=myPythonRepo[0]["id"])
#print("deleting repo again (should fail gracefully)")
#repos.removeDatabricksRepo(db, repo_id=myPythonRepo[0]["id"])

#endregion

# region begin Repos (../ApplicationConfiguration/Powershell/Modules/Databricks/DatabricksRepos.ps1)
# https://github.com/crflynn/databricks-api

# getDatabricksRepo
def getDatabricksRepo(db, path_prefix=None, branch=None):
    repos = db.repos.list_repos(path_prefix=path_prefix)
    if len(repos) > 0:
        if branch == None:
            return repos["repos"]
        else:
            return [r for r in repos["repos"] if r["branch"] == branch]
    else:
        return []

# newDatabricksRepo
def newDatabricksRepo(db, url, provider, path, headers=None):
    repos = getDatabricksRepo(db, path_prefix=path)
    if len(repos) == 0:
        print("{0} : Creating repo {1}, path: {2}".format(time.ctime(), url, path))
        return db.repos.create_repo(url=url, provider=provider, path=path, headers=headers)
    else:
        return "Repo already exists"

# removeDatabricksRepo
def removeDatabricksRepo(db, repo_id):
    repos = getDatabricksRepo(db)
    repo = [r for r in repos if r["id"] == repo_id]
    if len(repo) > 0:
        print("{0} : Removing repo {1}".format(time.ctime(), repo_id))
        return db.repos.delete_repo(id=repo[0]["id"])
    else:
        return "Repo does not exist"

# endregion