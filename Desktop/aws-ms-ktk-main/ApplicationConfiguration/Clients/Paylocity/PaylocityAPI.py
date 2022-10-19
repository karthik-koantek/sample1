import sys
import requests
import json
import logging
import time

class PaylocityAPI:

    #init function -
    def __init__(self):
        self.client_id = 'WE+NmsX3Bk6cXdH4qYsmFS04NTg1ODA5OTE5NDY2MDQwMzg2'
        self.client_secret = 'sc44Tw0Dsx+wLZ+n++AThlDuFfQYx+aL/n71FbPnmIqEVi1WUAkD08uU8quGRv+1/zU1sW3WQ2eGT7sGsdYJoA=='
        self.getToken()


    def getToken (self):
        auth_server_url = "https://api.paylocity.com/IdentityServer/connect/token"
        token_req_payload = {'grant_type': 'client_credentials', 'scope': 'WebLinkAPI'}

        token_response = requests.post(auth_server_url,data=token_req_payload, verify=False, allow_redirects=False,auth=(self.client_id, self.client_secret))

        if token_response.status_code !=200:
            print("Failed to obtain token from the OAuth 2.0 server", file=sys.stderr)
            sys.exit(1)
        print("Successfuly obtained a new token")
        tokens = json.loads(token_response.text)

        token = tokens['access_token']
        self.token = token
    

    def make_request(self, url):
        bearerToken = "Bearer {0}".format(self.token)
        payload = {}
        headers = {
            'Authorization': bearerToken
        }
        r = requests.request("GET", url, headers=headers, data=payload)
        return r

    def getEmployees(self, companyid):
        url = "https://api.paylocity.com/api/v2/companies/{0}/employees/".format(companyid)
        return self.make_request(url)

    def getEmployee(self, companyid, employeeid):
        url = "https://api.paylocity.com/api/v2/companies/{0}/employees/{1}".format(companyid, employeeid)
        return self.make_request(url)


api = PaylocityAPI()
response = api.getEmployee("36764", "101695")
