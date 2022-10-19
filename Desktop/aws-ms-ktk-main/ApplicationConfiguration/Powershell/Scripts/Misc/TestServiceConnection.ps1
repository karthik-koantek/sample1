Function Get-AADToken {
    [CmdletBinding()]
    [OutputType([string])]
    PARAM (
        [String]$TenantID,
        [string]$ServicePrincipalId,
        [securestring]$ServicePrincipalPwd
    )
    Try {
        # Set Resource URI to Azure Database
        $resourceAppIdURI = 'https://database.windows.net/'

        # Set Authority to Azure AD Tenant
        $authority = 'https://login.windows.net/' + $TenantId
        $ClientCred = [Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential]::new($ServicePrincipalId, $ServicePrincipalPwd)
        $authContext = [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext]::new($authority)
        $authResult = $authContext.AcquireTokenAsync($resourceAppIdURI, $ClientCred)
        #$Token = $authResult.Result.CreateAuthorizationHeader()
        $Token = $authResult.Result.AccessToken
    }
    Catch {
        Throw $_
        $ErrorMessage = 'Failed to aquire Azure AD token.'
        Write-Error -Message $ErrorMessage
    }
    $Token
}

#region Connect to db using SPN Account
$TenantId = "2d8cc149-4c57-462b-a1fb-6e548ffd73bd"
$ServicePrincipalId = $(Get-AzureRmADServicePrincipal -SearchString ADFtoKusto).ApplicationId
$SecureStringPassword = ConvertTo-SecureString -String "iw?2iRZ5b-6cJRIwe9EiZJex?XSRXNh@" -AsPlainText -Force
$SQLServerName = "armdeploymentsqlserverdev"
Get-AADToken -TenantID $TenantId -ServicePrincipalId $ServicePrincipalId -ServicePrincipalPwd $SecureStringPassword -OutVariable SPNToken


Write-Verbose "Create SQL connectionstring"
$conn = New-Object System.Data.SqlClient.SQLConnection
$DatabaseName = 'mwtOnRampReporting'
$conn.ConnectionString = "Data Source=$SQLServerName.database.windows.net;Initial Catalog=$DatabaseName;Connect Timeout=30"
$conn.AccessToken = $($SPNToken)
$conn

Write-Verbose "Connect to database and execute SQL script"
$conn.Open()
$query = 'select @@version'
$command = New-Object -TypeName System.Data.SqlClient.SqlCommand($query, $conn)
$Result = $command.ExecuteScalar()
$Result
$conn.Close()
#endregions

