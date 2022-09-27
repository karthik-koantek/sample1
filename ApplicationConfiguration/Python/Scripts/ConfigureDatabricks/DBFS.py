import time, json
import base64
import os


def importLocalFileToDBFS(path, local_file_with_path=None, overwrite=False, headers=None):
    try:
        if os.path.isfile(local_file_with_path):
            with open(local_file_with_path, "rb") as file:
                print("{0} : Encoding {1} to base64".format(time.ctime(), local_file_with_path))
                encoded_content = base64.b64encode(file.read())
                encoded_content = encoded_content.decode('utf-8')
    except Exception as e:
        print(f"File does not exists\n{e}")

    if encoded_content:
        print("{0} : Moving {1} to path: {2}".format(time.ctime(), local_file_with_path, path))
        dbfs = db.dbfs.put(path=path, contents=encoded_content, overwrite=overwrite, headers=headers)


# # test
# from databricks_api import DatabricksAPI
# db = DatabricksAPI(
#     host="https://adb-4447139959491338.18.azuredatabricks.net/",
#     token= ""
# )

# # importLocalFileToDBFS
# path = '/FileStore/user/data.csv'
# local_file_with_path = 'C:\\Users\\Admin\\Downloads\\temp\\New_Query_2022_04_20 (1).csv'
# importLocalFileToDBFS(path, local_file_with_path)