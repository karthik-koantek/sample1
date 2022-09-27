
#Install-Module -Name Az.Kusto -RequiredVersion 0.1.2

Function Get-KustoCluster {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KustoClusterName
    )
    $KustoClusters = Get-AzKustoCluster -ResourceGroupName $ResourceGroupName
    $KustoCluster = $KustoClusters | Where-Object {$_.Name -eq $KustoClusterName}
    return $KustoCluster
}

Function New-KustoCluster {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KustoClusterName,
        [Parameter(Mandatory=$false, Position = 2)]
        [string]$KustoSku="D13_v2",
        [Parameter(Mandatory=$false, Position = 3)]
        [int]$Capacity=2
    )

    $KustoCluster = Get-KustoCluster -ResourceGroupName $ResourceGroupName -KustoClusterName $KustoClusterName
    if(!$KustoCluster) {
        New-AzKustoCluster -ResourceGroupName $ResourceGroupName -Name $KustoClusterName -Location $Location -Sku $KustoSku -Capacity $Capacity
    } else {
        Write-Host "Kusto Cluster already exists."
    }
}

Function Remove-KustoCluster {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KustoClusterName
    )

    $KustoCluster = Get-KustoCluster -ResourceGroupName $ResourceGroupName -KustoClusterName $KustoClusterName
    if($KustoCluster) {
        Remove-AzKustoCluster -ResourceGroupName $ResourceGroupName -Name $KustoClusterName
    } else {
        Write-Host "Kusto Cluster does not exist."
    }
}

Function Get-KustoDatabase {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KustoClusterName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$KustoDatabaseName
    )

    $KustoDatabases = Get-AzKustoDatabase -ResourceGroupName $ResourceGroupName -ClusterName $KustoClusterName
    $KustoDatabase = $KustoDatabases | Where-Object {$_.Name -eq "$KustoClusterName/$KustoDatabaseName"}
        return $KustoDatabase
}

Function New-KustoDatabase {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KustoClusterName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$KustoDatabaseName,
        [Parameter(Mandatory=$false, Position = 3)]
        [int]$SoftDeletePeriodInDays = 0,
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$HotCachePeriodInDays = 7
    )

    $KustoDatabase = Get-KustoDatabase -ResourceGroupName $ResourceGroupName -KustoClusterName $KustoClusterName -KustoDatabaseName $KustoDatabaseName
    if (!$KustoDatabase) {
        New-AzKustoDatabase -ResourceGroupName $ResourceGroupName -ClusterName $KustoClusterName -Name $KustoDatabaseName -SoftDeletePeriodInDays 365 -HotCachePeriodInDays 7
    } else {
        Write-Host "Kusto Database already exists."
    }
}

Function Remove-KustoDatabase {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KustoClusterName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$KustoDatabaseName
    )

    $KustoDatabase = Get-KustoDatabase -ResourceGroupName $ResourceGroupName -KustoClusterName $KustoClusterName -KustoDatabaseName $KustoDatabaseName
    if ($KustoDatabase) {
        Remove-AzKustoDatabase -ResourceGroupName $ResourceGroupName -ClusterName $KustoClusterName -Name $KustoDatabaseName
    } else {
        Write-Host "Kusto Database does not exist."
    }
}

