[string]$ApplicationId = "adbe55d2-faa6-4031-84b9-16d17174e029"
[string]$Secret = ":Qk?iqJ@I=0RwdJqyWq1Vky60bkaWcbP"
[string]$ResourceGroupName = "AnalyticsAcceleratorDemo"
[string]$WorkspaceName = "analyticsacceleratordemo"
[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$TenantId = "5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd"
[string]$Region = "westus2"
[string]$DatabricksOrgId = "6707205789541644"
[string]$Token = "dapifa6e319f00cb71ab43ba9770072bf7f4"

$Secret = [System.Web.HttpUtility]::UrlEncode($Secret)
$BodyText="grant_type=client_credentials&client_id=$ApplicationId&resource=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d&client_secret=$Secret"
$URI = "https://login.microsoftonline.com/$TenantId/oauth2/token/"
$Response = Invoke-RestMethod -Method POST -Body $BodyText -Uri $URI -ContentType application/x-www-form-urlencoded
$DatabricksAccessToken = $Response.access_token
$Header = @{"Authorization"="Bearer $DatabricksAccessToken";
"X-Databricks-Org-Id"="$DatabricksOrgId"
}
#$Header
$DatabricksURI = "https://$Region.azuredatabricks.net/api/2.0/token/create"
$LifetimeSeconds = 3600
$Comment = "admin"
$Body = @{}
if ($LifetimeSeconds){$Body['lifetime_seconds']=$LifetimeSeconds}
if ($Comment){$Body['comment']=$Comment}
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$BodyText = $Body | ConvertTo-Json -Depth 10
$Response = Invoke-RestMethod -Method "POST" -Uri $DatabricksURI -Headers $Header -Body $BodyText
$Response

Function New-DatabricksBearerToken {
    [cmdletbinding()]
    param(
        [parameter(Mandatory = $false)] [int]$LifetimeSeconds,
        [parameter(Mandatory = $false)] [string]$Comment
    )
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    $Body = @{}
    if ($LifetimeSeconds){$Body['lifetime_seconds']=$LifetimeSeconds}
    if ($Comment){$Body['comment']=$Comment}

    Return Invoke-DatabricksAPI  -Method POST -API "api/2.0/token/create" -Body $Body
}

$Response = New-DatabricksBearerToken
$Response