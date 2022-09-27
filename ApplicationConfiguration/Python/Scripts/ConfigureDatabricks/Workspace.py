import base64
import json
import os
from pkgutil import get_data
import time


def newDatabricksWorkspaceFolder(path, headers=None):
    '''
    Create the given directory and necessary parent directories if they do not exists. 
    If there exists an object (not a directory) at any prefix of the input path, 
    this call returns an error RESOURCE_ALREADY_EXISTS. If this operation fails 
    it may have succeeded in creating some of the necessary parent directories.
    '''
    try:
        db.workspace.mkdirs(path, headers=headers)
        print(f"Workspace folder successfully created: {path}")
    except Exception as e:
        if json.loads(e)['error'] == "RESOURCE_ALREADY_EXISTS":
            print(f"Directory '{path}' already exists")
        else:
            print(f"Error in creating workspace folder. \nException: {str(e)}\n\n")


def removeDatabricksWorkspaceFolder(path, recursive=False, headers=None):
    '''
    Delete an object or a directory (and optionally recursively deletes all objects in the directory). 
    If path does not exist, this call returns an error RESOURCE_DOES_NOT_EXIST. 
    If path is a non-empty directory and recursive is set to false, this call returns an error DIRECTORY_NOT_EMPTY. 
    Object deletion cannot be undone and deleting a directory recursively is not atomic.
    '''
    try:
        db.workspace.delete(path, recursive, headers)
        print(f"Workspace folder successfully deleted: {path}\n\n")
    except Exception as e:
        print(f"Error in deleting workspace folder {path}.\nException: {str(e)}\n\n")


def importNotebook(path, language, local_directory, format="SOURCE", overwrite=False, headers=None):
    '''
    Import a notebook or the contents of an entire directory. 
    If path already exists and overwrite is set to false, 
    this call returns an error RESOURCE_ALREADY_EXISTS. 
    You can use only DBC format to import a directory.
    https://docs.databricks.com/dev-tools/api/latest/workspace.html#import
    '''
    try:
        if os.path.isfile(local_directory):
            with open(local_directory, "rb") as file:
                print("{0} : Encoding {1} to base64".format(time.ctime(), local_directory))
                content = base64.b64encode(file.read())
                content = content.decode('utf-8')

        db.workspace.import_workspace(path, format, language, content, overwrite, headers)
        print(f"Notebook '{path.split('/')[-1]}' imported to Workspace folder {'/'.join(path.split('/')[:-1])}")

    except Exception as e:
        if str(e) == "MAX_NOTEBOOK_SIZE_EXCEEDED":
            print(f"Error in importing notebooks. 10 MB limit exceeded. Exception: {str(e)}")
        else:
            print(f"Error in importing notebooks. Exception: {str(e)}")

    
def importNotebookDirectory(local_path, workspace_path=None):
    '''
    Import a notebook or the contents of an entire directory. 
    If path already exists and overwrite is set to false, 
    this call returns an error RESOURCE_ALREADY_EXISTS. 
    You can use only DBC format to import a directory.
    https://docs.databricks.com/dev-tools/api/latest/workspace.html#import
    '''
    if workspace_path is None:
        workspace_path = ''
    
    notebooks = []
    for root, dirs, files in os.walk(local_path):
        for filename in files:
            notebooks.append(os.path.join(root, filename))
    
    # Create directory structure in workspace
    workspace_directories = []
    for notebook in notebooks:
        notebook_name = notebook.split('.')
        _notebook_destination_name = (''.join(notebook_name[0:-1])).replace(local_path, "")
        _notebook_destination_name = '/'.join(_notebook_destination_name.split('\\')[:-1])
        _notebook_destination_name = f"{workspace_path}{_notebook_destination_name}"
        workspace_directories.append(_notebook_destination_name)

    for workspace_dir in workspace_directories:
        newDatabricksWorkspaceFolder(workspace_dir)
    
    # Upload Notebooks
    for notebook in notebooks:
        notebook_name = notebook.split('.')
        if notebook_name[-1] == 'py':
            notebook_language = "PYTHON"
        elif notebook_name[-1] == "scala":
            notebook_language ="SCALA"
        elif notebook_name[-1] == "sql":
            notebook_language = "SQL"
        elif notebook_name[-1] == 'r':
            notebook_language = "R"

        _notebook_destination_name = ((''.join(notebook_name[0:-1])).replace(local_path, '')).replace('\\', '/')
        _notebook_destination_name = f"{workspace_path}{_notebook_destination_name}"
        importNotebook(_notebook_destination_name, notebook_language, notebook)


def getDirectoryStatus(path, headers=None):
    '''
    Gets the status of an object or a directory. If path does not exist, 
    this call returns an error RESOURCE_DOES_NOT_EXIST.
    '''
    return db.workspace.get_status(path, headers)


def getDirectoryList(path, headers=None):
    try:
        status = getDirectoryStatus(path, headers)
        if status['object_type'] == 'DIRECTORY':
            objects_list = db.workspace.list(path).get('objects')
            if objects_list is not None:
                for obj in objects_list:
                    if obj['object_type'] == 'DIRECTORY':
                        path = obj['path']
                        objects_list = objects_list + getDirectoryList(path)
                    print(f"Successfully fetched object list from directory {path}")
            else:
                return []
        return objects_list
    except Exception as e:
        print(f"Failed to fetch object lists from directory {path}")


def exportNotebookDirectory(path, output_path):
    try:
        objects_list = getDirectoryList(path)
        for obj in objects_list:
            if obj['object_type'] == 'DIRECTORY':
                output_path = output_path.replace('\\', '/')
                dir_path = output_path + obj['path']
                os.makedirs(dir_path, exist_ok=True)
                print(f"Created directory {dir_path}")
    except Exception as e:
        print(f"Failed to create notebook directory. Exception: {str(e)}")


def exportNotebook(path, format, output_path, direct_download=True, headers=None):
    '''Export a notebook or contents of an entire directory. 
    You can also export a Databricks Repo, or a notebook or directory from a Databricks Repo. 
    You cannot export non-notebook files from a Databricks Repo. 
    If path does not exist, this call returns an error RESOURCE_DOES_NOT_EXIST. 
    You can export a directory only in DBC format. 
    If the exported data exceeds the size limit, this call returns an error MAX_NOTEBOOK_SIZE_EXCEEDED. 
    This API does not support exporting a library.
    https://docs.databricks.com/dev-tools/api/latest/workspace.html#export
    '''
    try:
        status = getDirectoryStatus(path)
        if status['object_type'] == 'NOTEBOOK':
            file_content = db.workspace.export_workspace(path, format, direct_download, headers)
            print(f"Notebook content from {path} exported successfully.")
            file_extension = file_content['file_type']
            file_content = base64.b64decode(file_content['content'])

            output_path = output_path.replace('\\', '/')
            file_output_full_path = f"{output_path}{path}.{file_extension}"
            with open(file_output_full_path, 'w+') as f:
                f.write(file_content.decode("utf-8"))
                print(f"Successfully written file {file_output_full_path}")

    except Exception as e:
        print(f"Error in exporting notebook. Exception: {str(e)}")


def exportAllNotebooks(path, format, output_path, direct_download=False, headers=None):
    #create directory structure in local
    exportNotebookDirectory(path, output_path)

    # export notebooks one by one
    try:
        objects_list = getDirectoryList(path)
        for obj in objects_list:
            if obj['object_type'] == 'NOTEBOOK':
                notebook_path = obj['path']
                exportNotebook(notebook_path, format, output_path, direct_download, headers)
    except Exception as e:
        print(f"Failed to create notebook directory. Exception: {str(e)}")


# test
# from databricks_api import DatabricksAPI
# db = DatabricksAPI(
#     host="https://adb-4447139959491338.18.azuredatabricks.net/",
#     token= ""
# )

# newDatabricksWorkspaceFolder
# path = '/test/'
# newDatabricksWorkspaceFolder(path)
# path = '/test/function/'
# newDatabricksWorkspaceFolder(path)
# path = '/test/function/newDatabricksWorkspace/'
# newDatabricksWorkspaceFolder(path)
# path = '/test/function/'
# newDatabricksWorkspaceFolder(path)

# # removeDatabricksWorkspaceFolder
# path = '/test'
# removeDatabricksWorkspaceFolder(path, recursive=True)
# path = '/import_notebook_test'
# removeDatabricksWorkspaceFolder(path, recursive=True)

# # importNotebook
# path = '/source_directory/sub_directory/'
# newDatabricksWorkspaceFolder(path)
# path = '/source_directory/sub_directory/nameOfNotebook'
# local_directory = 'C:\\Users\\Admin\\Downloads\\temp\\import_notebook_test\\describe-my-ec2.py'
# language = 'PYTHON'
# importNotebook(path, language, local_directory, overwrite=True)

# # importNotebookDirectory
# workspace_path = "/test" #must not have '/' at the end
# path = "C:\\Users\\Admin\\Downloads\\temp"
# importNotebookDirectory(path, workspace_path)

# # getDirectoryList
# path = "/test"
# print(getDirectoryList(path))

# # exportNotebook
# path = "/test/import_notebook_test/describe-my-ec2"
# # # path = "/test"
# format = "SOURCE"
# output_path = "C:\\Users\\Admin\\Downloads\\temp\\exported"
# exportNotebook(path, format, output_path, direct_download=False)

# # exportNotebookDirectory
# path = "/test"
# output_path = "C:\\Users\\Admin\\Downloads\\temp\\exported"
# exportNotebookDirectory(path, output_path)

# # exportAllNotebooks
# path = "/test"
# output_path = "C:\\Users\\Admin\\Downloads\\temp\\exported"
# format = "SOURCE"
# exportAllNotebooks(path, format, output_path)

# from databricks_api import DatabricksAPI
# db = DatabricksAPI(
#     host="https://dbc-ac23aef8-2a83.cloud.databricks.com/",
#     token= ""
# )

# path = "/"
# # print(getDirectoryList('/Users/9c933057-3066-437b-a115-6503d7a0eed9/'))
# output_path = "C:\\Users\\Admin\\Downloads\\temp\\exported"
# format = "SOURCE"
# exportAllNotebooks(path, format, output_path)
# print(db.workspace.list('/Users/9c933057-3066-437b-a115-6503d7a0eed9').get('objects'))

# # importNotebookDirectory
# workspace_path = "/test" #must not have '/' at the end
# path = "C:\\Users\\Admin\\Downloads\\Brightline\\upload"
# importNotebookDirectory(path, workspace_path)