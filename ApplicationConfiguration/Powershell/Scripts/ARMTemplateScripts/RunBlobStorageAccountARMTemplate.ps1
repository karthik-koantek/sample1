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
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$BlobStorageAccountName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$VnetName,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$PrivateSubnetName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$PublicSubnetName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$BlobStorageAccountARMTemplateFile,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$IpAddressFirewallException
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

Write-Host "Creating Resource Group" -ForegroundColor Cyan
New-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName -Location $Location

$Resource = Get-AzResource -ResourceGroupName $ResourceGroupName -Name $BlobStorageAccountName
if (!$Resource) {
    Write-Host "Running Blob Storage ARM Template" -ForegroundColor Cyan
    $BlobStorageAccountTemplateParameterObject = @{
        storageAccountName = "$BlobStorageAccountName";
        location = "$Location";
        vnetName = "$VnetName";
        privateSubnetName = "$PrivateSubnetName";
        publicSubnetName = "$PublicSubnetName";
        ipAddressFirewallException = "$IpAddressFirewallException"
    }
    New-ResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $BlobStorageAccountARMTemplateFile -TemplateParameterObject $BlobStorageAccountTemplateParameterObject -Mode "Incremental"
} else {
    Write-Host "Blob Storage Account exists, skipping"
}

