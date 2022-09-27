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
    [string]$StorageAccountName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$VnetName,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$PrivateSubnetName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$PublicSubnetName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$StorageAccountARMTemplateFile,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$IpAddressFirewallException,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$StorageAccountSku,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$StorageAccountKind,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$StorageAccountAccessTier,
    [Parameter(Mandatory = $true, Position = 12)]
    [bool]$IsHnsEnabled
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

Write-Host "Creating Resource Group" -ForegroundColor Cyan
New-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName -Location $Location

$Resource = Get-AzResource -ResourceGroupName $ResourceGroupName -Name $StorageAccountName
if (!$Resource) {
    Write-Host "Running Storage ARM Template" -ForegroundColor Cyan
    $StorageAccountTemplateParameterObject = @{
        storageAccountName = "$StorageAccountName";
        location = "$Location";
        vnetName = "$VnetName";
        privateSubnetName = "$PrivateSubnetName";
        publicSubnetName = "$PublicSubnetName";
        ipAddressFirewallException = "$IpAddressFirewallException";
        storageAccountSku = "$StorageAccountSku";
        storageAccountKind = "$StorageAccountKind";
        storageAccountAccessTier = "$StorageAccountAccessTier";
        isHnsEnabled = $IsHnsEnabled;
    }
    New-ResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $StorageAccountARMTemplateFile -TemplateParameterObject $StorageAccountTemplateParameterObject -Mode "Incremental"
} else {
    Write-Host "Storage Account exists, skipping"
}

