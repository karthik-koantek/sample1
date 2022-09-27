import time

def setDatabricksSecretACLForPrincipal(db, scope, principal, permission, headers=None):
    '''
    Create or overwrite the ACL associated with the given principal (user, service principal, or group) on the specified scope point. 
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#put-secret-acl
    '''
    try:
        db.secret.put_acl(scope=scope, principal=principal, permission=permission, headers=headers)
        print(f"Successfully created ACL for scope: {scope}, principal: {principal}, permission: {permission}")
    except Exception as e:
        print(f"Failed to create ACL for scope: {scope}, principal: {principal}, permission: {permission}. Exception: {str(e)}")


def newDatabricksSecretScope(db, scope, initial_manage_principal="users", scope_backend_type=None, backend_azure_keyvault=None, headers=None):
    '''
    Create a Databricks-backed secret scope in which secrets are stored in Databricks-managed storage 
    and encrypted with a cloud-based specific encryption key.
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#create-secret-scope
    '''
    try:
        db.secret.create_scope(
            scope=scope, 
            initial_manage_principal=initial_manage_principal,
            scope_backend_type=scope_backend_type,
            backend_azure_keyvault=backend_azure_keyvault,
            headers=headers)
        print(f"Created secret Scope '{scope}' with managed principal '{initial_manage_principal}'")
    except Exception as e:
        print(f"Failed to created secret scope {scope}. Exception: {str(e)}")


def getDatabricksSecretScope(db, headers=None):
    '''
    List all secret scopes available in workspace
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#list-secret-scopes
    '''
    return db.secret.list_scopes(headers=headers)


def addDatabricksSecretScope(db, scope, initial_manage_principal):
    scopes = getDatabricksSecretScope(db)
    scope_name = [obj.get("name") for obj in scopes.get("scopes") if obj.get("name") == scope]

    if scope not in scope_name:
        newDatabricksSecretScope(db, scope=scope, initial_manage_principal=initial_manage_principal)
    else:
        print(f"{scope} already exists")


def removeDatabricksSecretScope(db, scope, headers=None):
    '''
    Delete a secret scope
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#delete-secret-scope
    '''
    try:
        db.secret.delete_scope(scope=scope, headers=headers)
        print(f"Successfully deleted scope '{scope}' deleted from workspace.")
    except Exception as e:
        print(f"Failed to delete scope '{scope}. Exception: {str(e)}'")


def getDatabricksSecrets(db, scope, headers=None):
    '''
    List the secret keys that are stored at this scope. This is a metadata-only operation; 
    you cannot retrieve secret data using this API. You must have READ permission to make this call.
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#list-secrets
    '''
    response = None
    try:
        response = db.secret.list_secrets(scope, headers=headers)
        print(f"Successfully listed secrets in scope '{scope}'")
    except Exception as e:
        print(f"Failed to list secrets in scope {scope}. Exception: {str(e)}")
    return response


def removeDatabricksSecret(db, scope, key, headers=None):
    '''
    Delete the secret stored in this secret scope. You must have WRITE or MANAGE permission on the secret scope.
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#delete-secret
    '''
    try:
        secrets = getDatabricksSecrets(db, scope)
        found_secret = [obj.get("key") for obj in secrets.get("secrets") if obj.get("key") == key]
        if key in found_secret:
            db.secret.delete_secret(scope=scope, key=key, headers=headers)
            print(f"Successfully deleted secret '{key}' from scope '{scope}'")
        else:
            print(f"key: {key} does not exist under scope: {scope}")
    except Exception as e:
        print(f"Failed to delete secret '{key}' from scope '{scope}'. Exception: {str(e)}")


def getDatabrickssecretacls(db, scope, principal, headers=None):
    '''
    Describe the details about the given ACL, such as the group and permission.
    You must have the MANAGE permission to invoke this API.
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#get-secret-acl
    '''
    response = None
    try:
        response = db.secret.list_acls(scope=scope, headers=headers)
        print(f"Successfully pulled ACL for scope '{scope}' and principal '{principal}'")
    except Exception as e:
        print(f"Failed to pull ACL for scope '{scope}' and principal '{principal}'. Exception: {str(e)}")
    return response


def removeDatabricksSecretACLForPrincipal(db, scope, principal, headers=None):
    '''
    Delete the given ACL on the given scope. You must have the MANAGE permission to invoke this API.
    https://docs.databricks.com/dev-tools/api/latest/secrets.html#delete-secret-acl
    '''
    try: 
        acls = getDatabrickssecretacls(db, scope=scope, principal=principal)
        found = [a.get("principal") for a in acls["items"] if a.get("principal") == principal]
        if principal in found:
            db.secret.delete_acl(scope=scope, principal=principal, headers=headers)
            print(f"Successfully deleted ACL for principal '{principal}' in scope '{scope}'")
        else:
            print("Secret ACL does not exist.")
    except Exception as e:
        print(f"Failed to delete ACL for principal '{principal}' in scope '{scope}'. Exception: {str(e)}")


# # test
# from databricks_api import DatabricksAPI
# db = DatabricksAPI(
#     host="https://adb-4447139959491338.18.azuredatabricks.net/",
#     token= ""
# )

# # newDatabricksSecretScope
# newDatabricksSecretScope(db, scope="my-simple-databricks-scope")

# # getDatabricksSecretScope
# print(getDatabricksSecretScope(db))

# # addDatabricksSecretScope
# addDatabricksSecretScope(db, scope="my-simple-databricks-scope", initial_manage_principal="users")
# addDatabricksSecretScope(db, scope="my-simple-databricks-scope", initial_manage_principal="admin")
# addDatabricksSecretScope(db, scope="my-simple-ascend-test-scope", initial_manage_principal="users")

# # removeDatabricksSecretScope
# removeDatabricksSecretScope(db, scope="my-simple-databricks-scope")
# removeDatabricksSecretScope(db, scope="my-simple-databricks-scope")

# # getDatabricksSecrets
# print(getDatabricksSecrets(db, scope="my-simple-ascend-test-scope"))
# print(getDatabricksSecrets(db, scope="adf"))

# # removeDatabricksSecret
# removeDatabricksSecret(db, scope='my-simple-ascend-test-scope', key='', headers=None)

# # getDatabrickssecretacls
# print(getDatabrickssecretacls(db, scope="my-simple-ascend-test-scope", principal="data-engineers"))
# print(getDatabrickssecretacls(db, scope="my-simple-ascend-test-scope", principal="users"))

# # removeDatabricksSecretACLForPrincipal
# removeDatabricksSecretACLForPrincipal(db, scope="my-simple-ascend-test-scope", principal="data-engineers")
# print(getDatabrickssecretacls(db, scope="my-simple-ascend-test-scope", principal="data-engineers"))

# # setDatabricksSecretACLForPrincipal
# setDatabricksSecretACLForPrincipal(db, scope="my-simple-databricks-scope", principal="data engineers", permission="READ")
# setDatabricksSecretACLForPrincipal(db, scope="my-simple-ascend-test-scope", principal="data engineers", permission="WRITE")
# print(getDatabrickssecretacls(db, scope="my-simple-databricks-scope", principal="data engineers"))
# print(getDatabrickssecretacls(db, scope="my-simple-ascend-test-scope", principal="data engineers"))

# # cleanup
# removeDatabricksSecretScope(db, scope="my-simple-databricks-scope")
# removeDatabricksSecretScope(db, scope="my-simple-ascend-test-scope")
# print(getDatabrickssecretacls(db, scope="my-simple-ascend-test-scope", principal="data engineers"))
