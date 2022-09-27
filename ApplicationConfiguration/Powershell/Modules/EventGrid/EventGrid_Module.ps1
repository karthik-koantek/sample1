
# Requirement: The Subscribtion needs to be registered before deploying the first Event Grid
# Register-AzResourceProvider -ProviderNamespace Microsoft.EventGrid

Function Get-StorageAccountEndpoint{
  param(
    [Parameter(Mandatory = $true,Position = 0)]
    [string] $ResourceGroupName,
    [Parameter(Mandatory = $true,Position = 1)]
    [string] $StorageAccountName
  )
   ## Configuring the Event Grid to subscribe to the storage account
   $SAcct = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -AccountName $StorageAccountName
   if ($SAcct){
       $StorageId = $SAcct.Id
       return $StorageId
   }
   else{
     Write-Host "The provided Storage Account does not exist" -ForegroundColor Cyan
   }

}


Function New-EventGridSub {
    param(
      [Parameter(Mandatory = $true,Position = 0)]
      [string] $ModulesDirectory,
      [Parameter(Mandatory = $true,Position = 1)]
      [string] $EventSubscriptionName,
      [Parameter(Mandatory = $true,Position = 2)]
      [string] $SubscriptionId,
      [Parameter(Mandatory = $true,Position = 3)]
      [string] $ResourceGroupName,
      [Parameter(Mandatory = $true,Position = 4)]
      [string] $EventHubNamespace,
      [Parameter(Mandatory = $true,Position = 5)]
      [string] $EventHub,
      [Parameter(Mandatory = $true,Position = 6)]
      [string] $StorageAccountName,
      [Parameter(Mandatory = $true,Position = 7)]
      [string] $StorageContainer
    )


    $StorageId = Get-StorageAccountEndpoint -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
    ## Configuring Event Hub as the endpoint of the subscription

    . $ModulesDirectory\EventHubModule.ps1
    $EH = Get-EventHub -ResourceGroupName $ResourceGroupName -NamespaceName $EventHubNamespace -EventHubName $EventHub
    if ($EH){
      [string]$endpoint = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.EventHub/namespaces/$EventHubNamespace/eventhubs/$EventHub"
    }
    else{
      Write-Host "Event Hub endpoint does not exist" -ForegroundColor Cyan
    }

    New-AzEventGridSubscription `
  -EventSubscriptionName $EventSubscriptionName `
  -EndpointType "eventhub" `
  -Endpoint $endpoint `
  -SubjectBeginsWith "/blobServices/default/containers/$StorageContainer" `
  -ResourceId $StorageId `
  -IncludedEventType "Microsoft.Storage.BlobCreated"
}

Function Get-EventGridSub{
  param(
    [Parameter(Mandatory = $true,Position = 0)]
    [string] $EventSubscriptionName,
    [Parameter(Mandatory = $true,Position = 1)]
    [string] $ResourceGroupName,
    [Parameter(Mandatory = $true,Position = 2)]
    [string] $StorageAccountName
  )

  $StorageId = Get-StorageAccountEndpoint -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
  $EGS = Get-AzEventGridSubscription -EventSubscriptionName $EventSubscriptionName -ResourceId $StorageId
  if ($EGS){
    return $EGS
  }
  else{
    Write-Host "Event Grid Subscription does not exist" -ForegroundColor Cyan
  }
}

Function Remove-EventGridSub {
  param(
    [Parameter(Mandatory = $true,Position = 0)]
    [string] $EventSubscriptionName,
    [Parameter(Mandatory = $true,Position = 1)]
    [string] $ResourceGroupName,
    [Parameter(Mandatory = $true,Position = 2)]
    [string] $StorageAccountName
  )

  $StorageId = Get-StorageAccountEndpoint -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
  $EGS = Get-AzEventGridSubscription -EventSubscriptionName $EventSubscriptionName -ResourceId $StorageId
  if ($EGS){
    Remove-AzEventGridSubscription -EventSubscriptionName $EventSubscriptionName -ResourceId $StorageId
  }
  else{
    Write-Host "Event Grid Subscription does not exist" -ForegroundColor Cyan
  }

}