def getDatabricksToken(db, headers=None):
    '''
    List all the valid tokens for a user-workspace pair.
    https://docs.databricks.com/dev-tools/api/latest/tokens.html#list
    '''
    return db.token.list_tokens(headers)


def newDatabricksToken(db, lifetime_seconds, comment, headers=None):
    '''
    Create and return a token. This call returns the error QUOTA_EXCEEDED if the current number of 
    non-expired tokens exceeds the token quota. The token quota for a user is 600.
    https://docs.databricks.com/dev-tools/api/latest/tokens.html#create
    '''
    response = None
    try:
        response = db.token.create_token(lifetime_seconds, comment, headers)
        print(f"Successfully created new token with comment '{comment}'")
    except Exception as e:
        print(f"Failed to create new token. Exception: {str(e)}")
    return response


def revokeDatabricksToken(db, token_id, headers=None):
    '''
    Revoke an access token. This call returns the error RESOURCE_DOES_NOT_EXIST if a token with the specified ID is not valid.
    https://docs.databricks.com/dev-tools/api/latest/tokens.html#revoke
    '''
    try:
        db.token.revoke_token(token_id, headers)
        print(f"Successfully removed Token with token id {token_id}")
    except Exception as e:
        print(f"Failed to remove Token with token id {token_id}. Exception: {str(e)}")


# # test
# from databricks_api import DatabricksAPI
# db = DatabricksAPI(
#     host="https://adb-4447139959491338.18.azuredatabricks.net/",
#     token= ""
# )

# # # getDatabricksToken
# # print(getDatabricksToken(db))

# # newDatabricksToken
# token_details = newDatabricksToken(db, lifetime_seconds=6000, comment="Created using Ascend To Test CreatToken Function")
# print(token_details)

# # revokeDatabricksToken
# revokeDatabricksToken(db, token_details["token_info"]["token_id"])
# revokeDatabricksToken(db, "12345678901234567890a")

# # No clean up needed