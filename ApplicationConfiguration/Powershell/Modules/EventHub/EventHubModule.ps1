
Function Get-EventHubNS {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$NamespaceName
    )
    $EventHubNSp = Get-AzEventHubNamespace -ResourceGroupName $ResourceGroupName
    $EventHubNS = $EventHubNSp | Where-Object {$_.Name -eq $NamespaceName}
    return $EventHubNS
}


Function New-EventHubNS {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$Location
    )
    $EventHubNS = Get-EventHubNS -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName
    if(!$EventHubNS) {
        New-AzEventHubNamespace -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -Location $Location
    } else {
        Write-Host "Event Hub NameSpace already exists."
    }
}

Function Remove-EventHubNS {
    param (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$NamespaceName
    )
    $EHNS = Get-EventHubNS -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName
    if ($EHNS){
    Remove-AzEventHubNamespace -ResourceGroupName $ResourceGroupName -Name $NamespaceName
    }
    else{
        Write-Host "Event Hub NameSpace does not exist."
    }
}

Function Get-EventHub {
    param (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$EventHubName
    )
    $EHs = Get-AzEventHub -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName
    $EH = $EHs | Where-Object {$_.Name -eq $EventHubName}
    return $EH
}


Function New-EventHub{
    param (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$EventHubName,
        [Parameter(Mandatory=$false, Position = 3)]
        [int]$MessageRetentionInDays = 2,
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$PartitionCount = 2
    )
    $EH = Get-EventHub -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -EventHubName $EventHubName
    if(!$EH){
        New-AzEventHub -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -Name $EventHubName -MessageRetentionInDays $MessageRetentionInDays -PartitionCount $PartitionCount
    }
    else{
        Write-Host "Event Hub already exists."
    }
}

Function Remove-EventHub {
    param (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$EventHubName
    )
    $EH = Get-EventHub -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -EventHubName $EventHubName
    if ($EH){
    Remove-AzEventHub -ResourceGroupName $ResourceGroupName -Namespace $NamespaceName -Name $EventHubName
    }
    else{
        Write-Host "Event Hub does not exist."
    }
}

Function Get-EventHubCG {
    param (
        [Parameter(Mandatory = $true,Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory = $true,Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory = $true,Position = 2)]
        [string]$EventHubName,
        [Parameter(Mandatory = $true,Position = 3)]
        [string]$ConsumerGroupName
    )
    $CGs = Get-AzEventHubConsumerGroup -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -EventHubName $EventHubName
    $CG = $CGs | Where-Object {$_.Name -eq $ConsumerGroupName}
    return $CG
}

Function New-EventHubCG{
    param (
        [Parameter(Mandatory = $true,Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory = $true,Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory = $true,Position = 2)]
        [string]$EventHubName,
        [Parameter(Mandatory = $true,Position = 3)]
        [string]$ConsumerGroupName
    )

    $CG = Get-EventHubCG -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -EventHubName $EventHubName -ConsumerGroupName $ConsumerGroupName
    if (!$CG){
    New-AzEventHubConsumerGroup -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -EventHubName $EventHubName -ConsumerGroupName $ConsumerGroupName
    }
    else{
        Write-Host "Event Hub Consumer Group already exists"
    }
}

Function Remove-EventHubCG {
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory = $true,Position = 1)]
        [string]$NamespaceName,
        [Parameter(Mandatory = $true,Position = 2)]
        [string]$EventHubName,
        [Parameter(Mandatory = $true,Position = 3)]
        [string]$ConsumerGroupName
    )
    $CG = Get-EventHubCG -ResourceGroupName $ResourceGroupName -NamespaceName $NamespaceName -EventHubName $EventHubName -ConsumerGroupName $ConsumerGroupName
    if ($CG){
        Remove-AzEventHubConsumerGroup -ResourceGroupName $ResourceGroupName -Namespace $NamespaceName -EventHub $EventHubName -Name $ConsumerGroupName
    }
    else{
        Write-Host "Event Hub Consumer Group does not exist"
    }
}

