param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$Location,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$DataFactoryName,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$ADFARMTemplateFile
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

Write-Host "Creating Resource Group" -ForegroundColor Cyan
New-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName -Location $Location

$Resource = Get-AzResource -ResourceGroupName $ResourceGroupName -Name $DataFactoryName
if (!$Resource) {
    Write-Host "Running ADF ARM Template" -ForegroundColor Cyan
    $ADFTemplateParameterObject = @{
        factoryName = "$DataFactoryName";
        location = "$Location";
    }
    New-ResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $ADFARMTemplateFile -TemplateParameterObject $ADFTemplateParameterObject -Mode "Incremental"
} else {
    Write-Host "ADF Resource exists, skipping"
}

