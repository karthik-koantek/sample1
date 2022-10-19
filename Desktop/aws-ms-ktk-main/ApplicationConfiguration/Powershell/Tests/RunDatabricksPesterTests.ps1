param
(
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $false, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $false, Position = 2)]
    [string]$Region,
    [Parameter(Mandatory = $false, Position = 3)]
    [string]$BearerToken,
    [Parameter(Mandatory = $false, Position = 4)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $false, Position = 5)]
    [string]$ScriptsDirectory,
    [Parameter(Mandatory = $false, Position = 6)]
    [string]$TestsDirectory,
    [Parameter(Mandatory = $false, Position = 7)]
    [string]$DeployUATTesting = "false"
)

#$Modules = Get-Module -ListAvailable
#If ($Modules.Name -notcontains 'pester') {
Install-Module Pester -Force -SkipPublisherCheck -RequiredVersion 4.10.1
#}
#Install-Module -Name Az.Storage -RequiredVersion 1.11.1-preview -AllowPrerelease -SkipPublisherCheck

Invoke-Pester -Script "$TestsDirectory/Catalyst.Databricks.Tests.ps1" -OutputFile "$TestsDirectory/DatabricksTest-Databricks.XML" -OutputFormat 'NUnitXML'

