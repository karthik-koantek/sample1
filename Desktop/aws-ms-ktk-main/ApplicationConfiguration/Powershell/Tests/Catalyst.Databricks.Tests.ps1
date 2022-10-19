Set-AzContext -SubscriptionId $SubscriptionId
Install-Module -Name Az.Databricks -Force

$DatabricksWorkspace = Get-AzDatabricksWorkspace -ResourceGroupName $ResourceGroupName
[string]$DatabricksWorkspaceUrl = $DatabricksWorkspace.Url
[string]$Machine = "https://$DatabricksWorkspaceUrl"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksGroups.ps1
. $ModulesDirectory\Databricks\DatabricksClusters.ps1
. $ModulesDirectory\Databricks\DatabricksInstancePools.ps1
. $ModulesDirectory\Databricks\DatabricksLibraries.ps1
. $ModulesDirectory\Databricks\DatabricksJobs.ps1
. $ModulesDirectory\Databricks\DatabricksSecrets.ps1
. $ModulesDirectory\Databricks\DatabricksWorkspace.ps1
. $ModulesDirectory\Databricks\DatabricksDBFS.ps1
. $ModulesDirectory\Databricks\DatabricksPermissions.ps1
. $ModulesDirectory\Databricks\DatabricksRepos.ps1
. $ModulesDirectory\Databricks\DatabricksSQL.ps1

[string]$ExpectedSparkVersion = "11.0.x-scala2.12"
[string]$ExpectedPhotonVersion = "11.0.x-scala2.12"
[string]$ExpectedMLRuntimeSparkVersion = "11.0.x-cpu-ml-scala2.12"
[string]$ExpectedNodeType = "Standard_DS3_v2"
[string]$ExpectedPhotonNodeType = "Standard_E4ds_v4"
[string]$InitScriptPath = "dbfs:/databricks/pyodbc.sh"
[string]$ProjectNotebook = "/Framework/Orchestration/Orchestration"
[string]$WheelFile = "dbfs:/FileStore/jars/ktk-0.0.2-py3-none-any.whl"
[string]$DevelopmentBranch = "features/december"

#region begin collect details 

#region begin Groups
$DatabricksGroups = (Get-DatabricksGroups -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json
$adminMembers = (Get-DatabricksGroupMembers -BaseURI $URIBase -Token $Token -GroupName "admins").Content | ConvertFrom-Json
$ContributorMembers = (Get-DatabricksGroupMembers -BaseURI $URIBase -Token $Token -GroupName "Contributors").Content | ConvertFrom-Json
$ReaderMembers = (Get-DatabricksGroupMembers -BaseURI $URIBase -Token $Token -GroupName "Readers").Content | ConvertFrom-Json
#endregion

#region begin Notebooks
$FrameworkContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework").Content | ConvertFrom-Json).objects
$DataEngineeringContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering").Content | ConvertFrom-Json).objects
$DataEngineeringMetadataContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Metadata").Content | ConvertFrom-Json).objects
$DataEngineeringSilverZoneContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Silver Zone").Content | ConvertFrom-Json).objects
$DataEngineeringBronzeZoneContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Bronze Zone").Content | ConvertFrom-Json).objects
$DataEngineeringPresentationZoneContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Presentation Zone").Content | ConvertFrom-Json).objects
$DataEngineeringStreamingContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Streaming").Content | ConvertFrom-Json).objects
$DataEngineeringGoldZoneContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Gold Zone").Content | ConvertFrom-Json).objects
$DataEngineeringSandboxZoneContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Engineering/Sandbox Zone").Content | ConvertFrom-Json).objects
$DevelopmentContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Development").Content | ConvertFrom-Json).objects
$OrchestrationContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Orchestration").Content | ConvertFrom-Json).objects
$SecretsContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Secrets").Content | ConvertFrom-Json).objects
$VirtualDataConnectorsContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Virtual Data Connectors").Content | ConvertFrom-Json).objects
$OverviewContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Overview").Content | ConvertFrom-Json).objects
$MLContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/ML").Content | ConvertFrom-Json).objects
$MLSparkContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/ML/spark").Content | ConvertFrom-Json).objects
$EDAContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Exploratory Data Analysis").Content | ConvertFrom-Json).objects
$DQRulesEngineContents = ((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Data Quality Rules Engine").Content | ConvertFrom-Json).objects
#endregion

#region begin Secrets
$Scopes = ((Get-DatabricksSecretScopes -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).scopes
$InternalSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "internal").Content | ConvertFrom-Json).secrets
$MetadataDBSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "metadatadb").Content | ConvertFrom-Json).secrets
$DWDBSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "dwdb").Content | ConvertFrom-Json).secrets
if($DeployUATTests -eq "true") {
    $AdventureWorksLTSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "adventureworkslt").Content | ConvertFrom-Json).secrets
    $AzureDataExplorerSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "AzureDataExplorer").Content | ConvertFrom-Json).secrets
    $CognitiveServicesSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "CognitiveServices").Content | ConvertFrom-Json).secrets
    $CosmosDBSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "CosmosDB").Content | ConvertFrom-Json).secrets
    $EventHubCVDistributionSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "EventHubCVDistribution").Content | ConvertFrom-Json).secrets
    $EventHubCVIngestionSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "EventHubCVIngestion").Content | ConvertFrom-Json).secrets
    $EventHubTwitterDistributionSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "EventHubTwitterDistribution").Content | ConvertFrom-Json).secrets
    $EventHubTwitterIngestionSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "EventHubTwitterIngestion").Content | ConvertFrom-Json).secrets
    $ExternalBlobStoreSecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "ExternalBlobStore").Content | ConvertFrom-Json).secrets
    $TwitterAPISecrets = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName "TwitterAPI").Content | ConvertFrom-Json).secrets
}
#endregion

#region begin Pools and Clusters
$DefaultInstancePool = ((Get-InstancePools -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).instance_pools | Where-Object {$_.instance_pool_name -eq "Default"}
$DeltaEngineInstancePool = ((Get-InstancePools -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).instance_pools | Where-Object {$_.instance_pool_name -eq "Photon"}
$HCInstancePool = ((Get-InstancePools -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).instance_pools | Where-Object {$_.instance_pool_name -eq "HighConcurrency"}

$DefaultInstancePoolPermissions = ((Get-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId $DefaultInstancePool.instance_pool_id).Content | ConvertFrom-Json).access_control_list
$DeltaEngineInstancePoolPermissions = ((Get-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId $DeltaEngineInstancePool.instance_pool_id).Content | ConvertFrom-Json).access_control_list
$HCInstancePoolPermissions = ((Get-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId $HCInstancePool.instance_pool_id).Content | ConvertFrom-Json).access_control_list

$DefaultClusterDetail = (Get-DatabricksClusterDetail -BaseURI $URIBase -Token $Token -ClusterName "Default").Content | ConvertFrom-Json
$HCClusterDetail = (Get-DatabricksClusterDetail -BaseURI $URIBase -Token $Token -ClusterName "HighConcurrency").Content | ConvertFrom-Json
$SingleNodeClusterDetail = (Get-DatabricksClusterDetail -BaseURI $URIBase -Token $Token -ClusterName "SingleNode").Content | ConvertFrom-Json
$DefaultClusterMavenLibraries = ((Get-ClusterLibraryStatus -BaseURI $URIBase -Token $Token -ClusterID $DefaultClusterDetail.cluster_id).Content | ConvertFrom-Json).library_statuses.library.maven.coordinates
$DefaultClusterWhlLibraries = ((Get-ClusterLibraryStatus -BaseURI $URIBase -Token $Token -ClusterID $DefaultClusterDetail.cluster_id).Content | ConvertFrom-Json).library_statuses.library.whl
$DefaultClusterPyPiLibraries = ((Get-ClusterLibraryStatus -BaseURI $URIBase -Token $Token -ClusterID $DefaultClusterDetail.cluster_id).Content | ConvertFrom-Json).library_statuses.library.pypi.package
$HCClusterWhlLibraries = ((Get-ClusterLibraryStatus -BaseURI $URIBase -Token $Token -ClusterID $HCClusterDetail.cluster_id).Content | ConvertFrom-Json).library_statuses.library.whl
$SingleNodeClusterWhlLibraries = ((Get-ClusterLibraryStatus -BaseURI $URIBase -Token $Token -ClusterID $SingleNodeClusterDetail.cluster_id).Content | ConvertFrom-Json).library_statuses.library.whl
$DefaultClusterPermissions = (Get-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $DefaultClusterDetail.cluster_id).Content | ConvertFrom-Json 
$SingleNodeClusterPermissions = (Get-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $SingleNodeClusterDetail.cluster_id).Content | ConvertFrom-Json 
$HighConcurrencyClusterPermissions = (Get-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $HCClusterDetail.cluster_id).Content | ConvertFrom-Json 
#endregion

#region begin Jobs
$Jobs = ((Get-DatabricksJobs -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).jobs
$DataCatalogJob = $Jobs | Where-Object {$_.settings.name -like "Data Catalog*"}
$DataCatalogJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $DataCatalogJob.job_id).Content | ConvertFrom-Json).access_control_list
if($DeployUATTests -eq "true") {
    $StreamingIngestionJob = $Jobs.settings | Where-Object {$_.name -eq "Streaming Ingestion Job"}
    $StreamingIngestionJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $StreamingIngestionJob.job_id).Content | ConvertFrom-Json).access_control_list

    $BatchIngestionJob = $Jobs.settings | Where-Object {$_.name -eq "Batch Ingestion Job"}
    $StreamingIngestionJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $StreamingIngestionJob.job_id).Content | ConvertFrom-Json).access_control_list

    $ADXProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "ADX Processing Job"}
    $ADXProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $ADXProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $TableMaintenanceJob = $Jobs.settings | Where-Object {$_.name -eq "Table Maintenance"}
    $TableMaintenanceJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $TableMaintenanceJob.job_id).Content | ConvertFrom-Json).access_control_list

    $MLPipelineJob = $Jobs.settings | Where-Object {$_.name -eq "ML Pipeline"}
    $MLPipelineJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $MLPipelineJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATADXProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_ADX"}
    $UATADXProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATADXProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATCosmosProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_Cosmos"}
    $UATCosmosProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATCosmosProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATEventHubProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_EventHub"}
    $UATEventHubProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATEventHubProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATExternalFilesProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_ExternalFiles_and_GoldZone"}
    $UATExternalFilesProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATExternalFilesProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATMLProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_ML"}
    $UATMLProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATMLProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATSQLProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_SQL"}
    $UATSQLProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATSQLProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list

    $UATTableMaintenanceProcessingJob = $Jobs.settings | Where-Object {$_.name -eq "UAT_Table_Maintenance"}
    $UATTableMaintenanceProcessingJobPermissions = ((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId $UATTableMaintenanceProcessingJob.job_id).Content | ConvertFrom-Json).access_control_list
}
#endregion

#region begin Repos
$Repos = ((Get-DatabricksRepos -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).repos
$DevRepo = $Repos | Where-Object {$_.path -like "/Repos/Development/*"}
$ProdRepo = $Repos | Where-Object {$_.path -like "/Repos/Production/*"}
#endregion

#region begin SQL Endpoints
$2xSmallSQLEndpoint = Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName "2xSmall"
$SmallSQLEndpoint = Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName "Small"
$MediumSQLEndpoint = Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName "Medium"
$2xSmallSQLEndpointPermissions = Get-SQLEndpointPermissions -BaseURI $URIBase -Token $Token -SQLEndpointId $2xSmallSQLEndpoint.id 
$SmallSQLEndpointPermissions = Get-SQLEndpointPermissions -BaseURI $URIBase -Token $Token -SQLEndpointId $SmallSQLEndpoint.id 
$MediumSQLEndpointPermissions = Get-SQLEndpointPermissions -BaseURI $URIBase -Token $Token -SQLEndpointId $MediumSQLEndpoint.id 

#endregion

#endregion

Describe 'Databricks Unit Tests' {
    Context 'Databricks Groups' {
        It 'Readers Group should exist' {
            "Readers" | Should -BeIn $DatabricksGroups.group_names
        }
        It 'admins Group should exist' {
            "admins" | Should -BeIn $DatabricksGroups.group_names
        }
        It 'Contributors Group should exist' {
            "Contributors" | Should -BeIn $DatabricksGroups.group_names
        }
        It 'admins Group should contain at least 7 members' {
            $adminMembers.members.count | Should -BeGreaterOrEqual 7
        }
        It 'Readers Group should contain at least 7 members' {
            $ReaderMembers.members.count | Should -BeGreaterOrEqual 7
        }
        It 'Contributors Group should contain at least 7 members' {
            $ContributorMembers.members.count | Should -BeGreaterOrEqual 7
        }
        It 'admins Group should contain datalakeowners AD Group' {
            $adminMembers.members.user_name -like "*datalakeowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'admins Group should contain TransientZoneOwners AD Group' {
            $adminMembers.members.user_name -like "*transientzoneowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'admins Group should contain BronzeZoneOwners AD Group' {
            $adminMembers.members.user_name -like "*bronzezoneowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'admins Group should contain SilverGoldZoneOwners AD Group' {
            $adminMembers.members.user_name -like "*silvergoldzoneowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'admins Group should contain SandboxZoneOwners AD Group' {
            $adminMembers.members.user_name -like "*sandboxzoneowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'admins Group should contain SilverGoldEncryptedDataOwners AD Group' {
            $adminMembers.members.user_name -like "*silvergoldencrypteddataowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'admins Group should contain SilverGoldUnencryptedDataOwners AD Group' {
            $adminMembers.members.user_name -like "*silvergoldunencrypteddataowners@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain datalakecontributors AD Group' {
            $ContributorMembers.members.user_name -like "*datalakecontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain TransientZoneContributors AD Group' {
            $ContributorMembers.members.user_name -like "*transientzonecontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain BronzeZoneContributors AD Group' {
            $ContributorMembers.members.user_name -like "*bronzezonecontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain SilverGoldZoneContributors AD Group' {
            $ContributorMembers.members.user_name -like "*silvergoldzonecontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain SandboxZoneContributors AD Group' {
            $ContributorMembers.members.user_name -like "*sandboxzonecontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain SilverGoldEncryptedDataContributors AD Group' {
            $ContributorMembers.members.user_name -like "*silvergoldencrypteddatacontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Contributors Group should contain SilverGoldUnencryptedDataContributors AD Group' {
            $ContributorMembers.members.user_name -like "*silvergoldunencrypteddatacontributors@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain datalakereaders AD Group' {
            $ReaderMembers.members.user_name -like "*datalakereaders@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain TransientZoneReaders AD Group' {
            $ReaderMembers.members.user_name -like "*transientzonereaders@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain BronzeZoneReaders AD Group' {
            $ReaderMembers.members.user_name -like "*bronzezonereaders@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain SilverGoldZoneReaders AD Group' {
            $ReaderMembers.members.user_name -like "*silvergoldzonereaders@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain SandboxZoneReaders AD Group' {
            $ReaderMembers.members.user_name -like "*sandboxzonereaders@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain SilverGoldEncryptedDataReaders AD Group' {
            $ReaderMembers.members.user_name -like "*silvergoldencrypteddatareaders@*" | Should -BeGreaterOrEqual 1
        }
        It 'Readers Group should contain SilverGoldUnencryptedDataReaders AD Group' {
            $ReaderMembers.members.user_name -like "*silvergoldunencrypteddatareaders@*" | Should -BeGreaterOrEqual 1
        }
    }
    Context 'Databricks Default Instance Pool' {
        It 'Default Instance Pool should exist' {
            $DefaultInstancePool | Should -BeTrue
        }
        It 'Should be named Default' {
            $DefaultInstancePool.instance_pool_name | Should -Be "Default"
        }
        It 'Should have zero minimum idle instances' {
            $DefaultInstancePool.min_idle_instances | Should -BeExactly 0
        }
        It 'Should autoterminate idle instances after a time period' {
            $DefaultInstancePool.idle_instance_autotermination_minutes | Should -BeGreaterOrEqual 10
        }
        It 'Should use the expected node type' {
            $DefaultInstancePool.node_type_id | Should -Be $ExpectedNodeType
        }
        It 'Should use the latest stable spark version' {
            $DefaultInstancePool.preloaded_spark_versions[0] | Should -Be $ExpectedSparkVersion
        }
        It 'Contributors should have CAN_ATTACH_TO permissions' {
            ($DefaultInstancePoolPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
        It 'Readers should have CAN_ATTACH_TO permissions' {
            ($DefaultInstancePoolPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
    }
    Context 'Databricks Photon Instance Pool' {
        It 'Photon Instance Pool should exist' {
            $DeltaEngineInstancePool | Should -BeTrue
        }
        It 'Should be named Photon' {
            $DeltaEngineInstancePool.instance_pool_name | Should -Be "Photon"
        }
        It 'Should have zero minimum idle instances' {
            $DeltaEngineInstancePool.min_idle_instances | Should -BeExactly 0
        }
        It 'Should autoterminate idle instances after a time period' {
            $DeltaEngineInstancePool.idle_instance_autotermination_minutes | Should -BeGreaterOrEqual 10
        }
        It 'Should use the expected node type' {
            $DeltaEngineInstancePool.node_type_id | Should -Be $ExpectedPhotonNodeType
        }
        It 'Should use the latest stable spark version' {
            $DeltaEngineInstancePool.preloaded_spark_versions[0] | Should -Be $ExpectedPhotonVersion
        }
        It 'Contributors should have CAN_ATTACH_TO permissions' {
            ($DefaultInstancePoolPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
        It 'Readers should have CAN_ATTACH_TO permissions' {
            ($DefaultInstancePoolPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
    }
    Context 'Databricks HighConcurrency Instance Pool' {
        It 'HighConcurrency Instance Pool should exist' {
            $HCInstancePool | Should -BeTrue
        }
        It 'Should be named HighConcurrency' {
            $HCInstancePool.instance_pool_name | Should -Be "HighConcurrency"
        }
        It 'Should have zero minimum idle instances' {
            $HCInstancePool.min_idle_instances | Should -BeExactly 0
        }
        It 'Should autoterminate idle instances after a time period' {
            $HCInstancePool.idle_instance_autotermination_minutes | Should -BeGreaterOrEqual 10
        }
        It 'Should use the expected node type' {
            $HCInstancePool.node_type_id | Should -Be $ExpectedNodeType
        }
        It 'Should use the latest stable spark version' {
            $HCInstancePool.preloaded_spark_versions[0] | Should -Be $ExpectedMLRuntimeSparkVersion
        }
        It 'Contributors should have CAN_ATTACH_TO permissions' {
            ($DefaultInstancePoolPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
        It 'Readers should have CAN_ATTACH_TO permissions' {
            ($DefaultInstancePoolPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
    }
}

Describe 'Databricks Workspace' {
    Context 'Databricks Workspace' {
        It '/Framework/Data Engineering Directory should exist' {
            "/Framework/Data Engineering" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Development Directory should exist' {
            "/Framework/Development" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Exploratory Data Analysis Directory should exist' {
            "/Framework/Exploratory Data Analysis" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Orchestration Directory should exist' {
            "/Framework/Orchestration" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Secrets Directory should exist' {
            "/Framework/Secrets" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Virtual Data Connectors Directory should exist' {
            "/Framework/Virtual Data Connectors" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Overview Directory should exist' {
            "/Framework/Overview" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/ML Directory should exist' {
            "/Framework/ML" | Should -BeIn $FrameworkContents.path
        }
        It '/Framework/Data Quality Rules Engine should exist' {
            "/Framework/Data Quality Rules Engine" | Should -BeIn $FrameworkContents.path
        }
    }
    Context 'Data Engineering Workspace' {
        It '/Framework/Data Engineering Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Data Engineering"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Data Engineering Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Data Engineering"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Data Engineering/Metadata Directory should exist' {
            "/Framework/Data Engineering/Metadata" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Silver Zone Directory should exist' {
            "/Framework/Data Engineering/Silver Zone" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Bronze Zone Directory should exist' {
            "/Framework/Data Engineering/Bronze Zone" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Presentation Zone Directory should exist' {
            "/Framework/Data Engineering/Presentation Zone" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Streaming Directory should exist' {
            "/Framework/Data Engineering/Streaming" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Gold Zone Directory should exist' {
            "/Framework/Data Engineering/Gold Zone" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Sandbox Zone Directory should exist' {
            "/Framework/Data Engineering/Sandbox Zone" | Should -BeIn $DataEngineeringContents.path
        }
        It '/Framework/Data Engineering/Metadata/Generate Metadata for Presentation Zone Notebok should exist' {
            "/Framework/Data Engineering/Metadata/Generate Metadata for Presentation Zone" | Should -BeIn $DataEngineeringMetadataContents.path
        }
        It '/Framework/Data Engineering/Metadata/Generate Metadata for SQL Source Notebok should exist' {
            "/Framework/Data Engineering/Metadata/Generate Metadata for SQL Source" | Should -BeIn $DataEngineeringMetadataContents.path
        }
        It '/Framework/Data Engineering/Metadata/Generate Metadata for Gold Zone Notebok should exist' {
            "/Framework/Data Engineering/Metadata/Generate Metadata for Gold Zone" | Should -BeIn $DataEngineeringMetadataContents.path
        }
        It '/Framework/Data Engineering/Metadata/Metadata Review Notebok should exist' {
            "/Framework/Data Engineering/Metadata/Metadata Review" | Should -BeIn $DataEngineeringMetadataContents.path
        }
        It '/Framework/Data Engineering/Silver Zone/Delta Load Notebook should exist' {
            "/Framework/Data Engineering/Silver Zone/Delta Load" | Should -BeIn $DataEngineeringSilverZoneContents.path
        }
        It '/Framework/Data Engineering/Silver Zone/Transform Silver Zone Table Notebook should exist' {
            "/Framework/Data Engineering/Silver Zone/Transform Silver Zone Table" | Should -BeIn $DataEngineeringSilverZoneContents.path
        }
        It '/Framework/Data Engineering/Silver Zone/Get Schema Notebook should exist' {
            "/Framework/Data Engineering/Silver Zone/Get Schema" | Should -BeIn $DataEngineeringSilverZoneContents.path
        }
        It '/Framework/Data Engineering/Bronze Zone/Batch CosmosDB Notebook should exist' {
            "/Framework/Data Engineering/Bronze Zone/Batch CosmosDB" | Should -BeIn $DataEngineeringBronzeZoneContents.path
        }
        It '/Framework/Data Engineering/Bronze Zone/Batch File CSV Notebook should exist' {
            "/Framework/Data Engineering/Bronze Zone/Batch File CSV" | Should -BeIn $DataEngineeringBronzeZoneContents.path
        }
        It '/Framework/Data Engineering/Bronze Zone/Batch File Parquet Notebook should exist' {
            "/Framework/Data Engineering/Bronze Zone/Batch File Parquet" | Should -BeIn $DataEngineeringBronzeZoneContents.path
        }
        It '/Framework/Data Engineering/Bronze Zone/Batch SQL Notebook should exist' {
            "/Framework/Data Engineering/Bronze Zone/Batch SQL" | Should -BeIn $DataEngineeringBronzeZoneContents.path
        }
        It '/Framework/Data Engineering/Presentation Zone/Azure Data Explorer Notebook should exist' {
            "/Framework/Data Engineering/Presentation Zone/Azure Data Explorer" | Should -BeIn $DataEngineeringPresentationZoneContents.path
        }
        It '/Framework/Data Engineering/Presentation Zone/Azure SQL DW Notebook should exist' {
            "/Framework/Data Engineering/Presentation Zone/Azure SQL DW" | Should -BeIn $DataEngineeringPresentationZoneContents.path
        }
        It '/Framework/Data Engineering/Presentation Zone/Cosmos DB Notebook should exist' {
            "/Framework/Data Engineering/Presentation Zone/Cosmos DB" | Should -BeIn $DataEngineeringPresentationZoneContents.path
        }
        It '/Framework/Data Engineering/Presentation Zone/SQL JDBC Notebook should exist' {
            "/Framework/Data Engineering/Presentation Zone/SQL JDBC" | Should -BeIn $DataEngineeringPresentationZoneContents.path
        }
        It '/Framework/Data Engineering/Presentation Zone/SQL Spark Connector Notebook should exist' {
            "/Framework/Data Engineering/Presentation Zone/SQL Spark Connector" | Should -BeIn $DataEngineeringPresentationZoneContents.path
        }
        It '/Framework/Data Engineering/Streaming/Ingest Event Hub Notebook should exist' {
            "/Framework/Data Engineering/Streaming/Ingest Event Hub" | Should -BeIn $DataEngineeringStreamingContents.path
        }
        It '/Framework/Data Engineering/Streaming/Ingest JSON Notebook should exist' {
            "/Framework/Data Engineering/Streaming/Ingest JSON" | Should -BeIn $DataEngineeringStreamingContents.path
        }
        It '/Framework/Data Engineering/Streaming/Output to Event Hub Notebook should exist' {
            "/Framework/Data Engineering/Streaming/Output to Event Hub" | Should -BeIn $DataEngineeringStreamingContents.path
        }
        It '/Framework/Data Engineering/Streaming/Sentiment Analysis using Azure Cognitive Services Notebook should exist' {
            "/Framework/Data Engineering/Streaming/Sentiment Analysis using Azure Cognitive Services" | Should -BeIn $DataEngineeringStreamingContents.path
        }
        It '/Framework/Data Engineering/Streaming/AppInsights Directory should exist' {
            "/Framework/Data Engineering/Streaming/AppInsights" | Should -BeIn $DataEngineeringStreamingContents.path
        }
        It '/Framework/Data Engineering/Gold Zone/Power BI File Notebook should exist' {
            "/Framework/Data Engineering/Gold Zone/Power BI File" | Should -BeIn $DataEngineeringGoldZoneContents.path
        }
        It '/Framework/Data Engineering/Gold Zone/Gold Load Notebook should exist' {
            "/Framework/Data Engineering/Gold Zone/Gold Load" | Should -BeIn $DataEngineeringGoldZoneContents.path
        }
        It '/Framework/Data Engineering/Sandbox Zone/Clone Table Notebook should exist' {
            "/Framework/Data Engineering/Sandbox Zone/Clone Table" | Should -BeIn $DataEngineeringSandboxZoneContents.path
        }
    }
    Context 'DQRulesEngineContents Workspace' {
        It '/Framework/Data Quality Rules Engine Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Data Quality Rules Engine"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Data Quality Rules Engine Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Data Quality Rules Engine"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Data Quality Rules Engine/Create Data Quality Rules Schema Notebook should exist' {
            "/Framework/Data Quality Rules Engine/Create Data Quality Rules Schema" | Should -BeIn $DQRulesEngineContents.path
        }
        It '/Framework/Data Quality Rules Engine/Data Quality Assessment Notebook should exist' {
            "/Framework/Data Quality Rules Engine/Data Quality Assessment" | Should -BeIn $DQRulesEngineContents.path
        }
        It '/Framework/Data Quality Rules Engine/Data Quality Profiling and Rules Development Notebook should exist' {
            "/Framework/Data Quality Rules Engine/Data Quality Profiling and Rules Development" | Should -BeIn $DQRulesEngineContents.path
        }
        It '/Framework/Data Quality Rules Engine/Export Data Quality Rules Notebook should exist' {
            "/Framework/Data Quality Rules Engine/Export Data Quality Rules" | Should -BeIn $DQRulesEngineContents.path
        }
        It '/Framework/Data Quality Rules Engine/Generate Default Data Quality Rules Notebook should exist' {
            "/Framework/Data Quality Rules Engine/Generate Default Data Quality Rules" | Should -BeIn $DQRulesEngineContents.path
        }
        It '/Framework/Data Quality Rules Engine/Import Data Quality Rules Notebook should exist' {
            "/Framework/Data Quality Rules Engine/Import Data Quality Rules" | Should -BeIn $DQRulesEngineContents.path
        }
    }
    Context 'Orchestration Workspace' {
        It '/Framework/Orchestration Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Orchestration"}).object_id
            "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        #It '/Framework/Orchestration Directory Reader Permissions' {
        #    $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Orchestration"}).object_id
        #    @("CAN_RUN","CAN_READ") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        #}
        It '/Framework/Orchestration/Orchestration Functions Notebook should exist' {
            "/Framework/Orchestration/Accelerator Functions" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/pyodbc init Notebook should exist' {
            "/Framework/Orchestration/pyodbc init" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Run Job Notebook should exist' {
            "/Framework/Orchestration/Run Job" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Run Project Notebook should exist' {
            $ProjectNotebook | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Run Stage Notebook should exist' {
            "/Framework/Orchestration/Run Stage" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Run Step Notebook should exist' {
            "/Framework/Orchestration/Run Step" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Run System Notebook should exist' {
            "/Framework/Orchestration/Run System" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Spark Table Maintenance Notebook should exist' {
            "/Framework/Orchestration/Spark Table Maintenance" | Should -BeIn $OrchestrationContents.path
        }
        It '/Framework/Orchestration/Orchestration Notebook should exist' {
            "/Framework/Orchestration/Orchestration" | Should -BeIn $OrchestrationContents.path
        }
    }
    Context 'Development Workspace' {
        It '/Framework/Development Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Development"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Development Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Development"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Development/Notebook Template should exist' {
            "/Framework/Development/Notebook Template" | Should -BeIn $DevelopmentContents.path
        }
        It '/Framework/Development/Send Tweets To Event Hub should exist' {
            "/Framework/Development/Send Tweets To Event Hub" | Should -BeIn $DevelopmentContents.path
        }
        It '/Framework/Development/Send JSON Payload To Event Hub should exist' {
            "/Framework/Development/Send JSON Payload To Event Hub" | Should -BeIn $DevelopmentContents.path
        }
        It '/Framework/Development/Utilities should exist' {
            "/Framework/Development/Utilities" | Should -BeIn $DevelopmentContents.path
        }
    }
    Context 'Secrets Workspace' {
        It '/Framework/Secrets Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Secrets"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Secrets Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Secrets"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Secrets/Import Data Lake Secrets Notebook should exist' {
            "/Framework/Secrets/Import Data Lake Secrets" | Should -BeIn $SecretsContents.path
        }
        It '/Framework/Secrets/Secrets Review Notebook should exist' {
            "/Framework/Secrets/Secrets Review" | Should -BeIn $SecretsContents.path
        }
    }
    Context 'Virtual Data Connectors Workspace' {
        It '/Framework/Virtual Data Connectors Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Virtual Data Connectors"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Virtual Data Connectors Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Virtual Data Connectors"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Virtual Data Connectors/ADLS Gen 2 Storage Account Notebook should exist' {
            "/Framework/Virtual Data Connectors/ADLS Gen 2 Storage Account" | Should -BeIn $VirtualDataConnectorsContents.path
        }
        It '/Framework/Virtual Data Connectors/Azure Data Explorer Notebook should exist' {
            "/Framework/Virtual Data Connectors/Azure Data Explorer" | Should -BeIn $VirtualDataConnectorsContents.path
        }
        It '/Framework/Virtual Data Connectors/Cosmos DB Notebook should exist' {
            "/Framework/Virtual Data Connectors/Cosmos DB" | Should -BeIn $VirtualDataConnectorsContents.path
        }
        It '/Framework/Virtual Data Connectors/Event Hub Notebook should exist' {
            "/Framework/Virtual Data Connectors/Event Hub" | Should -BeIn $VirtualDataConnectorsContents.path
        }
        It '/Framework/Virtual Data Connectors/SQL Notebook should exist' {
            "/Framework/Virtual Data Connectors/SQL" | Should -BeIn $VirtualDataConnectorsContents.path
        }
    }
    Context 'Overview Workspace' {
        It '/Framework/Overview Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Overview"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Overview Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Overview"}).object_id
           @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Overview/Framework Notebook should exist' {
            "/Framework/Overview/Framework" | Should -BeIn $OverviewContents.path
        }
        It '/Framework/Overview/Security Notebook should exist' {
            "/Framework/Overview/Security" | Should -BeIn $OverviewContents.path
        }
    }
    Context 'ML Workspace' {
        It '/Framework/ML Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/ML"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/ML Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/ML"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/ML/MLFunctions Notebook should exist' {
            "/Framework/ML/MLFunctions" | Should -BeIn $MLContents.path
        }
        It '/Framework/ML/Batch Inference Notebook should exist' {
            "/Framework/ML/Batch Inference" | Should -BeIn $MLContents.path
        }
        It '/Framework/ML/Streaming Inference Notebook should exist' {
            "/Framework/ML/Streaming Inference" | Should -BeIn $MLContents.path
        }
        It '/Framework/ML/spark Directory should exist' {
            "/Framework/ML/spark" | Should -BeIn $MLContents.path
        }
        It '/Framework/ML/spark/Train Linear Regression Notebook should exist' {
            "/Framework/ML/spark/Train Linear Regression" | Should -BeIn $MLSparkContents.path
        }
    }
    Context 'Exploratory Data Analysis Workspace' {
        It '/Framework/Exploratory Data Analysis Directory Contributor Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Exploratory Data Analysis"}).object_id
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Exploratory Data Analysis Directory Reader Permissions' {
            $WorkspaceDirectoryId = ($FrameworkContents | Where-Object {$_.path -eq "/Framework/Exploratory Data Analysis"}).object_id
            @("CAN_RUN") | Should -BeIn (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $WorkspaceDirectoryId).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
        It '/Framework/Exploratory Data Analysis/Data Catalog Database Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Catalog Database" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Catalog Review Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Catalog Review" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Catalog Table Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Catalog Table" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Catalog File Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Catalog File" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Catalog Master Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Catalog Master" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - Custom Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - Custom" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - Custom Bivariate Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - Custom Bivariate" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - Custom Univariate Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - Custom Univariate" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - DataPrep Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - DataPrep" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - DataPrep Bivariate Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - DataPrep Bivariate" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - DataPrep Univariate Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - DataPrep Univariate" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Data Profiling - Pandas Profiling Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Data Profiling - Pandas Profiling" | Should -BeIn $EDAContents.path
        }
        It '/Framework/Exploratory Data Analysis/Term Search Notebook should exist' {
            "/Framework/Exploratory Data Analysis/Term Search" | Should -BeIn $EDAContents.path
        }
    }
}
Describe 'Databricks Clusters' {
    Context 'Databricks Default Cluster' {
        It 'Default Cluster should exist' {
            $DefaultClusterDetail.cluster_name | Should -Be "Default"
        }
        It 'Should use the latest stable spark version' {
            $DefaultClusterDetail.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
        }
        It 'Should use Python3' {
            $DefaultClusterDetail.spark_env_vars.PYSPARK_PYTHON | Should -Be "/databricks/python3/bin/python3"
        }
        It 'Should autoterminate after a time period' {
            $DefaultClusterDetail.autotermination_minutes | Should -BeGreaterOrEqual 10
        }
        It 'Should load pyodbc as an init script' {
            $DefaultClusterDetail.init_scripts.dbfs.destination | Should -Be $InitScriptPath
        }
        It 'Should use the Default Instance Pool' {
            $DefaultClusterDetail.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
        }
        It 'Should be set to autoscale' {
            $DefaultClusterDetail.autoscale | Should -Be $true
        }
        It 'Should have 1 min_worker' {
            $DefaultClusterDetail.autoscale.min_workers | Should -BeExactly 1
        }
        It 'Should have at least 2 max workers' {
            $DefaultClusterDetail.autoscale.max_workers | Should -BeGreaterOrEqual 2
        }
        It 'Should use spark.scheduler.mode FAIR' {
            $DefaultClusterDetail.spark_conf.'spark.scheduler.mode' | Should -Be "FAIR"
        }
        It 'Should enable delta preview' {
            $DefaultClusterDetail.spark_conf.'spark.databricks.delta.preview.enabled' | Should -Be 'true'
        }
        It 'Should install the Spark SQL Connector Library' {
            "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0" | Should -BeIn $DefaultClusterMavenLibraries
        }
        It 'Should install the Azure Cosmos DB Connector Library' {
            "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.3.0" | Should -BeIn $DefaultClusterMavenLibraries
        }
        It 'Should install the Spark XML Connector Library' {
            "com.databricks:spark-xml_2.12:0.12.0" | Should -BeIn $DefaultClusterMavenLibraries
        }
        It 'Should install the Framework Whl' {
            $WheelFile | Should -BeIn $DefaultClusterWhlLibraries
        }
        It 'Should install the great_expectations Library' {
            "great_expectations" | Should -BeIn $DefaultClusterPyPiLibraries
        }
        It 'Readers group should have can_attach_to permissions' {
            ($DefaultClusterPermissions.access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
        It 'Contributors group should have can_restart permissions' {
            ($DefaultClusterPermissions.access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_RESTART"
        }
    }
    Context 'Databricks HighConcurrency Cluster' {
        It 'HighConcurrency Cluster should exist' {
            $HCClusterDetail.cluster_name | Should -Be "HighConcurrency"
        }
        It 'Should use the latest stable spark version' {
            $HCClusterDetail.spark_version | Should -Be $ExpectedSparkVersion
        }
        It 'Should use Python3' {
            $HCClusterDetail.spark_env_vars.PYSPARK_PYTHON | Should -Be "/databricks/python3/bin/python3"
        }
        It 'Should autoterminate after a time period' {
            $HCClusterDetail.autotermination_minutes | Should -BeGreaterOrEqual 10
        }
        It 'Should use the HighConcurrency Instance Pool' {
            $HCClusterDetail.instance_pool_id | Should -Be $HCInstancePool.instance_pool_id
        }
        It 'Should be set to autoscale' {
            $HCClusterDetail.autoscale | Should -Be $true
        }
        It 'Should have 1 min_worker' {
            $HCClusterDetail.autoscale.min_workers | Should -BeExactly 1
        }
        It 'Should have at least 2 max workers' {
            $HCClusterDetail.autoscale.max_workers | Should -BeGreaterOrEqual 2
        }
        It 'Should support python,sql' {
            $HCClusterDetail.spark_conf.'spark.databricks.repl.allowedLanguages' | Should -Be "python,sql"
        }
        It 'Should enable ProcessIsolation' {
            $HCClusterDetail.spark_conf.'spark.databricks.pyspark.enableProcessIsolation' | Should -Be "true"
        }
        It 'Should enable delta preview' {
            $HCClusterDetail.spark_conf.'spark.databricks.delta.preview.enabled' | Should -Be 'true'
        }
        #If enabled (this is non-compatible with Table ACLs) so either/or,
        #It 'Should support ADLS passthrough' {
        #    $HCClusterDetail.spark_conf.'spark.databricks.passthrough.enabled' | Should -Be "true"
        #}
        It 'Should support Table Access Control' {
            $HCClusterDetail.spark_conf.'spark.databricks.acl.dfAclsEnabled' | Should -Be "true"
        }
        It 'Should support Adaptive Query' {
            $HCClusterDetail.spark_conf.'spark.sql.adaptive.enabled' | Should -Be "true"
        }
        It 'Should be serverless' {
            $HCClusterDetail.spark_conf.'spark.databricks.cluster.profile' | Should -Be "serverless"
        }
        It 'Should install the Framework Whl' {
            $WheelFile | Should -BeIn $HCClusterWhlLibraries
        }
        It 'Readers group should have can_attach_to permissions' {
            ($HighConcurrencyClusterPermissions.access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
        It 'Contributors group should have can_restart permissions' {
            ($HighConcurrencyClusterPermissions.access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_RESTART"
        }
    }
    Context 'Databricks SingleNode Cluster' {
        It 'SingleNode Cluster should exist' {
            $SingleNodeClusterDetail.cluster_name | Should -Be "SingleNode"
        }
        It 'Should use the latest stable spark version' {
            $SingleNodeClusterDetail.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
        }
        It 'Should use Python3' {
            $SingleNodeClusterDetail.spark_env_vars.PYSPARK_PYTHON | Should -Be "/databricks/python3/bin/python3"
        }
        It 'Should autoterminate after a time period' {
            $SingleNodeClusterDetail.autotermination_minutes | Should -BeGreaterOrEqual 10
        }
        It 'Should load pyodbc as an init script' {
            $SingleNodeClusterDetail.init_scripts.dbfs.destination | Should -Be $InitScriptPath
        }
        It 'Should use the Default Instance Pool' {
            $SingleNodeClusterDetail.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
        }
        It 'Should enable delta preview' {
            $SingleNodeClusterDetail.spark_conf.'spark.databricks.delta.preview.enabled' | Should -Be 'true'
        }
        It 'spak.databricks.cluster.profile should be singleNode' {
            $SingleNodeClusterDetail.spark_conf.'spark.databricks.cluster.profile' | Should -Be 'singleNode'
        }
        It 'spak.master should be local[*]' {
            $SingleNodeClusterDetail.spark_conf.'spark.master' | Should -Be 'local[*]'
        }
        It 'Should install the Framework Whl' {
            $WheelFile | Should -BeIn $SingleNodeClusterWhlLibraries
        }
        It 'Readers group should have can_attach_to permissions' {
            ($SingleNodeClusterPermissions.access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_ATTACH_TO"
        }
        It 'Contributors group should have can_restart permissions' {
            ($SingleNodeClusterPermissions.access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_RESTART"
        }
    }
}

Describe 'Databricks Secrets and Scopes' {
    Context 'Databricks Secrets' {
        It 'There should be an internal secret scope' {
            "internal" | Should -BeIn $Scopes.name
        }
        It 'There should be an metadatadb secret scope' {
            "metadatadb" | Should -BeIn $Scopes.name
        }
        It 'There should be an dwdb secret scope' {
            "dwdb" | Should -BeIn $Scopes.name
        }
    }
}

if($DeployUATTests -eq "true") {
    Describe 'Databricks UAT Secrets and Scopes' {
        Context 'Databricks UAT Secrets' {
            It 'There should be an adventureworkslt secret scope' {
                "adventureworkslt" | Should -BeIn $Scopes.name
            }
            It 'There should be an AzureDataExplorer secret scope' {
                "AzureDataExplorer" | Should -BeIn $Scopes.name
            }
            It 'There should be an CognitiveServices secret scope' {
                "CognitiveServices" | Should -BeIn $Scopes.name
            }
            It 'There should be an CosmosDB secret scope' {
                "CosmosDB" | Should -BeIn $Scopes.name
            }
            It 'There should be an EventHubCVDistribution secret scope' {
                "EventHubCVDistribution" | Should -BeIn $Scopes.name
            }
            It 'There should be an EventHubCVIngestion secret scope' {
                "EventHubCVIngestion" | Should -BeIn $Scopes.name
            }
            It 'There should be an EventHubTwitterDistribution secret scope' {
                "EventHubTwitterDistribution" | Should -BeIn $Scopes.name
            }
            It 'There should be an EventHubTwitterIngestion secret scope' {
                "EventHubTwitterIngestion" | Should -BeIn $Scopes.name
            }
            It 'There should be an ExternalBlobStore secret scope' {
                "ExternalBlobStore" | Should -BeIn $Scopes.name
            }
            It 'There should be an TwitterAPI secret scope' {
                "TwitterAPI" | Should -BeIn $Scopes.name
            }
        }
        Context 'AdventureWorksLT Secret Scope' {
            It 'AdventureWorksLT Secret Scope should contain secret for DatabaseName' {
                "DatabaseName" | Should -BeIn $AdventureWorksLTSecrets.key
            }
            It 'AdventureWorksLT Secret Scope should contain secret for Login' {
                "Login" | Should -BeIn $AdventureWorksLTSecrets.key
            }
            It 'AdventureWorksLT Secret Scope should contain secret for Pwd' {
                "Pwd" | Should -BeIn $AdventureWorksLTSecrets.key
            }
            It 'AdventureWorksLT Secret Scope should contain secret for SQLServerName' {
                "SQLServerName" | Should -BeIn $AdventureWorksLTSecrets.key
            }
        }
        Context 'AzureDataExplorer Secret Scope' {
            It 'AzureDataExplorer Secret Scope should contain secret for ADXApplicationAuthorityId' {
                "ADXApplicationAuthorityId" | Should -BeIn $AzureDataExplorerSecrets.key
            }
            It 'AzureDataExplorer Secret Scope should contain secret for ADXApplicationId' {
                "ADXApplicationId" | Should -BeIn $AzureDataExplorerSecrets.key
            }
            It 'AzureDataExplorer Secret Scope should contain secret for ADXApplicationKey' {
                "ADXApplicationKey" | Should -BeIn $AzureDataExplorerSecrets.key
            }
            It 'AzureDataExplorer Secret Scope should contain secret for ADXClusterName' {
                "ADXClusterName" | Should -BeIn $AzureDataExplorerSecrets.key
            }
        }
        Context 'CognitiveServices Secret Scope' {
            It 'CognitiveServices Secret Scope should contain secret for AccessKey' {
                "AccessKey" | Should -BeIN $CognitiveServicesSecrets.key
            }
            It 'CognitiveServices Secret Scope should contain secret for Endpoint' {
                "Endpoint" | Should -BeIN $CognitiveServicesSecrets.key
            }
            It 'CognitiveServices Secret Scope should contain secret for LanguagesPath' {
                "LanguagesPath" | Should -BeIN $CognitiveServicesSecrets.key
            }
            It 'CognitiveServices Secret Scope should contain secret for SentimentPath' {
                "SentimentPath" | Should -BeIN $CognitiveServicesSecrets.key
            }
        }
        Context 'CosmosDB Secret Scope' {
            It 'CosmosDB Secret Scope should contain secret for Database' {
                "Database" | Should -BeIN $CosmosDBSecrets.key
            }
            It 'CosmosDB Secret Scope should contain secret for EndPoint' {
                "EndPoint" | Should -BeIN $CosmosDBSecrets.key
            }
            It 'CosmosDB Secret Scope should contain secret for MasterKey' {
                "MasterKey" | Should -BeIN $CosmosDBSecrets.key
            }
            It 'CosmosDB Secret Scope should contain secret for PreferredRegions' {
                "PreferredRegions" | Should -BeIN $CosmosDBSecrets.key
            }
        }
        Context 'EventHubCVDistribution Secret Scope' {
            It 'EventHubCVDistribution Secret Scope should contain secret for EventHubConnectionString' {
                "EventHubConnectionString" | Should -BeIN $EventHubCVDistributionSecrets.key
            }
            It 'EventHubCVDistribution Secret Scope should contain secret for EventHubConsumerGroup' {
                "EventHubConsumerGroup" | Should -BeIN $EventHubCVDistributionSecrets.key
            }
            It 'EventHubCVDistribution Secret Scope should contain secret for EventHubName' {
                "EventHubName" | Should -BeIN $EventHubCVDistributionSecrets.key
            }
        }
        Context 'EventHubCVIngestion Secret Scope' {
            It 'EventHubCVIngestion Secret Scope should contain secret for EventHubConnectionString' {
                "EventHubConnectionString" | Should -BeIN $EventHubCVIngestionSecrets.key
            }
            It 'EventHubCVIngestion Secret Scope should contain secret for EventHubConsumerGroup' {
                "EventHubConsumerGroup" | Should -BeIN $EventHubCVIngestionSecrets.key
            }
            It 'EventHubCVIngestion Secret Scope should contain secret for EventHubName' {
                "EventHubName" | Should -BeIN $EventHubCVIngestionSecrets.key
            }
        }
        Context 'EventHubTwitterDistribution Secret Scope' {
            It 'EventHubTwitterDistribution Secret Scope should contain secret for EventHubConnectionString' {
                "EventHubConnectionString" | Should -BeIN $EventHubTwitterDistributionSecrets.key
            }
            It 'EventHubTwitterDistribution Secret Scope should contain secret for EventHubConsumerGroup' {
                "EventHubConsumerGroup" | Should -BeIN $EventHubTwitterDistributionSecrets.key
            }
            It 'EventHubTwitterDistribution Secret Scope should contain secret for EventHubName' {
                "EventHubName" | Should -BeIN $EventHubTwitterDistributionSecrets.key
            }
        }
        Context 'EventHubTwitterIngestion Secret Scope' {
            It 'EventHubTwitterIngestion Secret Scope should contain secret for EventHubConnectionString' {
                "EventHubConnectionString" | Should -BeIN $EventHubTwitterIngestionSecrets.key
            }
            It 'EventHubTwitterIngestion Secret Scope should contain secret for EventHubConsumerGroup' {
                "EventHubConsumerGroup" | Should -BeIN $EventHubTwitterIngestionSecrets.key
            }
            It 'EventHubTwitterIngestion Secret Scope should contain secret for EventHubName' {
                "EventHubName" | Should -BeIN $EventHubTwitterIngestionSecrets.key
            }
        }
        Context 'ExternalBlobStore Secret Scope' {
            It 'ExternalBlobStore Secret Scope should contain secret for BasePath' {
                "BasePath" | Should -BeIN $ExternalBlobStoreSecrets.key
            }
            It 'ExternalBlobStore Secret Scope should contain secret for ContainerOrFileSystemName' {
                "ContainerOrFileSystemName" | Should -BeIN $ExternalBlobStoreSecrets.key
            }
            It 'ExternalBlobStore Secret Scope should contain secret for StorageAccountKey' {
                "StorageAccountKey" | Should -BeIN $ExternalBlobStoreSecrets.key
            }
            It 'ExternalBlobStore Secret Scope should contain secret for StorageAccountName' {
                "StorageAccountName" | Should -BeIN $ExternalBlobStoreSecrets.key
            }
        }
        Context 'TwitterAPI Secret Scope' {
            It 'TwitterAPI Secret Scope should contain secret for AccessToken' {
                "AccessToken" | Should -BeIN $TwitterAPISecrets.key
            }
            It 'TwitterAPI Secret Scope should contain secret for AccessTokenSecret' {
                "AccessTokenSecret" | Should -BeIN $TwitterAPISecrets.key
            }
            It 'TwitterAPI Secret Scope should contain secret for APIKey' {
                "APIKey" | Should -BeIN $TwitterAPISecrets.key
            }
            It 'TwitterAPI Secret Scope should contain secret for SecretKey' {
                "SecretKey" | Should -BeIN $TwitterAPISecrets.key
            }
        }
    }
}

Describe 'Databricks Jobs' {
    Context 'Databricks Jobs' {
        It 'Data Catalog Job should exist' {
            $DataCatalogJob.settings.name -like "Data Catalog*" | Should -BeGreaterOrEqual 1
        }
    }
    Context 'Data Catalog Job' {
        It 'Data Catalog Job should have azure-storage-file-datalake libary installed' {
            "azure-storage-file-datalake" | Should -BeIn $DataCatalogJob.settings.libraries.pypi.package
        }
        It 'Data Catalog Job should use latest Spark Version' {
            $DataCatalogJob.settings.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
        }
        It 'Data Catalog Job should install pyodbc.sh init script' {
            $DataCatalogJob.settings.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
        }
        It 'Data Catalog Job should use default instance pool' {
            $DataCatalogJob.settings.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
        }
        It 'Data Catalog Job should run the Run Project Notebook' {
            $DataCatalogJob.settings.notebook_task.notebook_path | Should -Be $ProjectNotebook
        }
        It 'Data Catalog Job ProjectName parameter should be Batch Pipeline' {
            $DataCatalogJob.settings.notebook_task.base_parameters.projectName | Should -Be "Data Catalog"
        }
        It 'Data Catalog Job should have min workers 2' {
            $DataCatalogJob.settings.new_cluster.autoscale.min_workers | Should -BeExactly 2
        }
        It 'Readers group should have can_attach_to permissions' {
            ($DataCatalogJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
        }
        It 'Readers group should have can_attach_to permissions' {
            ($DataCatalogJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
        }
    }
}

if ($DeployUATTests -eq "true") {
    Describe 'Databricks UAT Jobs' {
        Context 'Databricks UAT Jobs' {
            It 'Streaming Ingestion Job should exist' {
                "Streaming Ingestion Job" | Should -BeIn $Jobs.settings.name
            }
            It 'Batch Ingestion Job should exist' {
                "Batch Ingestion Job" | Should -BeIn $Jobs.settings.name
            }
            It 'ADX Processing Job should exist' {
                "ADX Processing Job" | Should -BeIn $Jobs.settings.name
            }
            It 'Table Maintenance Job should exist' {
                "Table Maintenance" | Should -BeIn $Jobs.settings.name
            }
            It 'ML Pipeline Job should exist' {
                "ML Pipeline" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_ADX Job should exist' {
                "UAT_ADX" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_Cosmos Job should exist' {
                "UAT_Cosmos" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_EventHub Job should exist' {
                "UAT_EventHub" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_ExternalFiles_and_GoldZone Job should exist' {
                "UAT_ExternalFiles_and_GoldZone" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_ML Job should exist' {
                "UAT_ML" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_SQL Job should exist' {
                "UAT_SQL" | Should -BeIn $Jobs.settings.name
            }
            It 'UAT_Table_Maintenance Job should exist' {
                "UAT_Table_Maintenance" | Should -BeIn $Jobs.settings.name
            }
        }
        Context 'Streaming Ingestion Job' {    
            It 'Streaming Ingestion Job should have event hub maven libary installed' {
                "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17" | Should -BeIn $StreamingIngestionJob.libraries.maven.coordinates
            }
            It 'Streaming Ingestion Job should have twitter maven libary installed' {
                "org.twitter4j:twitter4j-core:4.0.7" | Should -BeIn $StreamingIngestionJob.libraries.maven.coordinates
            }
            It 'Streaming Ingestion Job should use latest Spark Version' {
                $StreamingIngestionJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'Streaming Ingestion Job should install pyodbc.sh init script' {
                $StreamingIngestionJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'Streaming Ingestion Job should use default instance pool' {
                $StreamingIngestionJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'Streaming Ingestion Job should run the Run Project Notebook' {
                $StreamingIngestionJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'Streaming Ingestion Job ProjectName parameter should be Batch Pipeline' {
                $StreamingIngestionJob.notebook_task.base_parameters.projectName | Should -Be "Streaming Pipeline"
            }
            It 'Streaming Ingestion Job should use 2 workers' {
                $StreamingIngestionJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($StreamingIngestionJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($StreamingIngestionJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'Batch Ingestion Job' {
            It 'Batch Ingestion Job should have spark connector maven libary installed' {
                "com.microsoft.azure:azure-sqldb-spark:1.0.2" | Should -BeIn $BatchIngestionJob.libraries.maven.coordinates
            }
            It 'Batch Ingestion Job should have cosmos db maven libary installed' {
                "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.3.0" | Should -BeIn $BatchIngestionJob.libraries.maven.coordinates
            }
            It 'Batch Ingestion Job should have spark-xml maven libary installed' {
                "com.databricks:spark-xml_2.12:0.10.0" | Should -BeIn $BatchIngestionJob.libraries.maven.coordinates
            }
            It 'Batch Ingestion Job should use 6.6.x-scala2.11 (SQL, Cosmos, and XML Libraries do not currently support Spark 3)' {
                $BatchIngestionJob.new_cluster.spark_version | Should -Be "6.6.x-scala2.11"
            }
            It 'Batch Ingestion Job should install pyodbc.sh init script' {
                $BatchIngestionJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'Batch Ingestion Job should use default instance pool' {
                $BatchIngestionJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'Batch Ingestion Job should run the Run Project Notebook' {
                $BatchIngestionJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'Batch Ingestion Job ProjectName parameter should be Batch Pipeline' {
                $BatchIngestionJob.notebook_task.base_parameters.projectName | Should -Be "Batch Pipeline"
            }
            It 'Batch Ingestion Job should use 2 workers' {
                $BatchIngestionJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($BatchIngestionJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($BatchIngestionJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'ADX Processing Job' {
            It 'ADX Processing Job should have kusto-spark maven libary installed' {
                "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:2.9.1" | Should -BeIn $ADXProcessingJob.libraries.maven.coordinates
            }
            It 'ADX Processing Job should use ExpectedMLRuntimeSparkVersion ' {
                $ADXProcessingJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'ADX Processing Job should install pyodbc.sh init script' {
                $ADXProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'ADX Processing Job should use default instance pool' {
                $ADXProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'ADX Processing Job should run the Run Project Notebook' {
                $ADXProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'ADX Processing Job ProjectName parameter should be Batch Pipeline' {
                $ADXProcessingJob.notebook_task.base_parameters.projectName | Should -Be "ADX Pipeline"
            }
            It 'ADX Processing Job should use 2 workers' {
                $ADXProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($ADXProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($ADXProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'Table Maintenance Job' {
            It 'Table Maintenance Job should use latest Spark Version' {
                $TableMaintenanceJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'Table Maintenance Job should install pyodbc.sh init script' {
                $TableMaintenanceJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'Table Maintenance Job should use default instance pool' {
                $TableMaintenanceJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'Table Maintenance Job should run the Run Project Notebook' {
                $TableMaintenanceJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'Table Maintenance Job ProjectName parameter should be Batch Pipeline' {
                $TableMaintenanceJob.notebook_task.base_parameters.projectName | Should -Be "Table Maintenance"
            }
            It 'Table Maintenance Job should use 2 workers' {
                $TableMaintenanceJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($TableMaintenanceJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($TableMaintenanceJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'ML Pipeline Job' {
            It 'ML Pipeline Job should have pandas-profiling pypi libary installed' {
                "pandas-profiling" | Should -BeIn $MLPipelineJob.libraries.pypi.package
            }
            It 'ML Pipeline Job should use latest Spark Version' {
                $MLPipelineJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'ML Pipeline Job should install pyodbc.sh init script' {
                $MLPipelineJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'ML Pipeline Job should use default instance pool' {
                $MLPipelineJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'ML Pipeline Job should run the Run Project Notebook' {
                $MLPipelineJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'ML Pipeline Job ProjectName parameter should be ML Pipeline' {
                $MLPipelineJob.notebook_task.base_parameters.projectName | Should -Be "ML Pipeline"
            }
            It 'ML Pipeline Job should use 2 workers' {
                $MLPipelineJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($MLPipelineJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($MLPipelineJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_ADX Processing Job' {
            It 'ADX Processing Job should have kusto-spark maven libary installed' {
                "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:2.9.1" | Should -BeIn $UATADXProcessingJob.libraries.maven.coordinates
            }
            It 'ADX Processing Job should use ExpectedMLRuntimeSparkVersion ' {
                $UATADXProcessingJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'UAT_ADX Processing Job should install pyodbc.sh init script' {
                $UATADXProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_ADX Processing Job should use default instance pool' {
                $UATADXProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_ADX Processing Job should run the Run Project Notebook' {
                $UATADXProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_ADX Processing Job ProjectName parameter should be Batch Pipeline' {
                $UATADXProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_ADX"
            }
            It 'UAT_ADX Processing Job should use 2 workers' {
                $UATADXProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATADXProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATADXProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_Cosmos Processing Job' {
            It 'UAT_Cosmos Processing Job should have cosmos maven libary installed' {
                "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.5.2" | Should -BeIn $UATCosmosProcessingJob.libraries.maven.coordinates
            }
            It 'UAT_Cosmos Processing Job should use 10.2.x-cpu-ml-scala2.12' {
                $UATCosmosProcessingJob.new_cluster.spark_version | Should -Be "10.2.x-cpu-ml-scala2.12"
            }
            It 'UAT_Cosmos Processing Job should install pyodbc.sh init script' {
                $UATCosmosProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_Cosmos Processing Job should use default instance pool' {
                $UATCosmosProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_Cosmos Processing Job should run the Run Project Notebook' {
                $UATCosmosProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_Cosmos Processing Job ProjectName parameter should be Batch Pipeline' {
                $UATCosmosProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_Cosmos"
            }
            It 'UAT_Cosmos Processing Job should use 2 workers' {
                $UATCosmosProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATCosmosProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATCosmosProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_EventHub Processing Job' {
            It 'UAT_EventHub Processing Job should have event hub maven libary installed' {
                "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17" | Should -BeIn $UATEventHubProcessingJob.libraries.maven.coordinates
            }
            It 'UAT_EventHub Processing Job should have twitter maven libary installed' {
                "org.twitter4j:twitter4j-core:4.0.7" | Should -BeIn $UATEventHubProcessingJob.libraries.maven.coordinates
            }
            It 'UAT_EventHub Processing Job should use latest Spark Version' {
                $UATEventHubProcessingJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'UAT_EventHub Processing Job should install pyodbc.sh init script' {
                $UATEventHubProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_EventHub Processing Job should use default instance pool' {
                $UATEventHubProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_EventHub Processing Job should run the Run Project Notebook' {
                $UATEventHubProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_EventHub Processing Job ProjectName parameter should be Batch Pipeline' {
                $UATEventHubProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_EventHub"
            }
            It 'UAT_EventHub Processing Job should use 2 workers' {
                $UATEventHubProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATEventHubProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATEventHubProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_ExternalFiles_and_GoldZone Processing Job' {
            It 'UAT_ExternalFiles_and_GoldZone Processing Job should have spark-xml maven libary installed' {
                "com.databricks:spark-xml_2.12:0.10.0" | Should -BeIn $UATExternalFilesProcessingJob.libraries.maven.coordinates
            }
            It 'UAT_ExternalFiles_and_GoldZone Processing Job should use latest Spark Version' {
                $UATExternalFilesProcessingJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'UAT_ExternalFiles_and_GoldZone Processing Job should install pyodbc.sh init script' {
                $UATExternalFilesProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_ExternalFiles_and_GoldZone Processing Job should use default instance pool' {
                $UATExternalFilesProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_ExternalFiles_and_GoldZone Processing Job should run the Run Project Notebook' {
                $UATExternalFilesProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_ExternalFiles_and_GoldZone Processing Job ProjectName parameter should be Batch Pipeline' {
                $UATExternalFilesProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_ExternalFiles_and_GoldZone"
            }
            It 'UAT_ExternalFiles_and_GoldZone Processing Job should use 2 workers' {
                $UATExternalFilesProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATExternalFilesProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATExternalFilesProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_SQL Processing Job' {
            It 'UAT_SQL Processing Job should have spark sql maven libary installed' {
                "com.microsoft.azure:azure-sqldb-spark:1.0.2" | Should -BeIn $UATSQLProcessingJob.libraries.maven.coordinates
            }
            It 'UAT_SQL Processing Job should use 6.6.x-scala2.11 (Spark 3 not currently supported)' {
                $UATSQLProcessingJob.new_cluster.spark_version | Should -Be "6.6.x-scala2.11"
            }
            It 'UAT_SQL Processing Job should install pyodbc.sh init script' {
                $UATSQLProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_SQL Processing Job should use default instance pool' {
                $UATSQLProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_SQL Processing Job should run the Run Project Notebook' {
                $UATSQLProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_SQL Processing Job ProjectName parameter should be Batch Pipeline' {
                $UATSQLProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_SQL"
            }
            It 'UAT_SQL Processing Job should use 2 workers' {
                $UATSQLProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATSQLProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATSQLProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_ML Processing Job' {
            It 'UAT_ML Processing Job should have spark sql maven libary installed' {
                "com.microsoft.azure:azure-sqldb-spark:1.0.2" | Should -BeIn $UATMLProcessingJob.libraries.maven.coordinates
            }
            It 'UAT_ML Processing Job should install pyodbc.sh init script' {
                $UATMLProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_ML Processing Job should use default instance pool' {
                $UATMLProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_ML Processing Job should run the Run Project Notebook' {
                $UATMLProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_ML Processing Job ProjectName parameter should be UAT_ML' {
                $UATMLProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_ML"
            }
            It 'UAT_ML Processing Job should use 2 workers' {
                $UATMLProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATMLProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATMLProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
        Context 'UAT_Table_Maintenance Processing Job' {
            It 'UAT_Table_Maintenance Processing Job should use latest Spark Version' {
                $UATTableMaintenanceProcessingJob.new_cluster.spark_version | Should -Be $ExpectedMLRuntimeSparkVersion
            }
            It 'UAT_Table_Maintenance Processing Job should install pyodbc.sh init script' {
                $UATTableMaintenanceProcessingJob.new_cluster.init_scripts.dbfs.destination | Should -Be $InitScriptPath
            }
            It 'UAT_Table_Maintenance Processing Job should use default instance pool' {
                $UATTableMaintenanceProcessingJob.new_cluster.instance_pool_id | Should -Be $DefaultInstancePool.instance_pool_id
            }
            It 'UAT_Table_Maintenance Processing Job should run the Run Project Notebook' {
                $UATTableMaintenanceProcessingJob.notebook_task.notebook_path | Should -Be $ProjectNotebook
            }
            It 'UAT_Table_Maintenance Processing Job ProjectName parameter should be Batch Pipeline' {
                $UATTableMaintenanceProcessingJob.notebook_task.base_parameters.projectName | Should -Be "UAT_Table_Maintenance"
            }
            It 'UAT_Table_Maintenance Processing Job should use 2 workers' {
                $UATTableMaintenanceProcessingJob.new_cluster.num_workers | Should -BeExactly 2
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATTableMaintenanceProcessingJobPermissions | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_VIEW"
            }
            It 'Readers group should have can_attach_to permissions' {
                ($UATTableMaintenanceProcessingJobPermissions | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_MANAGE_RUN"
            }
        }
    }
}

Describe 'Databricks Repos' {
    Context 'Dev Repo' {
        It 'Should use the current development features branch' {
            $DevRepo.branch | Should -Be $DevelopmentBranch 
        }
        It 'Should use the Databricks Framework Repo' {
            $DevRepo.url | Should -Be "https://dev.azure.com/koantek/_git/Azure%20Databricks"
        }
        It 'Contributor Permissions' {
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $DevRepo.id).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Data Engineering Directory Reader Permissions' {
           "CAN_RUN" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $DevRepo.id).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
    }
    Context 'Prod Repo' {
        It 'Should use the current development features branch' {
            $ProdRepo.branch | Should -Be "master" 
        }
        It 'Should use the Databricks Framework Repo' {
            $ProdRepo.url | Should -Be "https://dev.azure.com/koantek/_git/Azure%20Databricks"
        }
        It 'Contributor Permissions' {
           "CAN_EDIT" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $ProdRepo.id).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level
        }
        It '/Framework/Data Engineering Directory Reader Permissions' {
           "CAN_RUN" | Should -Be (((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId $ProdRepo.id).Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level
        }
    }
}

Describe 'Databricks SQL Endpoints' {
    Context '2xSmall Endpoint' {
        It 'Should be named 2xSmall' {
            $2xSmallSQLEndpoint.name | Should -Be "2xSmall"
        }
        It 'Should be size XXSMALL' {
            $2xSmallSQLEndpoint.size | Should -Be "XXSMALL"
        }
        It 'Should be cluster_size 2X-Small' {
            $2xSmallSQLEndpoint.cluster_size | Should -Be "2X-Small"
        }
        It 'Should be min_num_clusters 1' {
            $2xSmallSQLEndpoint.min_num_clusters | Should -BeExactly 1
        }
        It 'Should be max_num_clusters 1' {
            $2xSmallSQLEndpoint.max_num_clusters | Should -BeExactly 1
        }
        It 'Should be auto_stop_mins 10' {
            $2xSmallSQLEndpoint.auto_stop_mins | Should -BeExactly 10
        }
        It 'Should be auto_resume True' {
            $2xSmallSQLEndpoint.auto_resume | Should -Be "True"
        }
        It 'Should be spot_instance_policy RELIABILITY_OPTIMIZED' {
            $2xSmallSQLEndpoint.spot_instance_policy | Should -Be "RELIABILITY_OPTIMIZED"
        }
        It 'Should be enable_photon True' {
            $2xSmallSQLEndpoint.enable_photon | Should -Be "True"
        }
        It 'Should be enable_serverless_compute False' {
            $2xSmallSQLEndpoint.enable_serverless_compute | Should -Be "False"
        }
        It 'Contributors group should have CAN_USE permissions' {
            (($2xSmallSQLEndpointPermissions.Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_USE"
        }
        It 'Readers group should have CAN_USE permissions' {
            (($2xSmallSQLEndpointPermissions.Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_USE"
        }
    }
    Context 'Small Endpoint' {
        It 'Should be named Small' {
            $SmallSQLEndpoint.name | Should -Be "Small"
        }
        It 'Should be size SMALL' {
            $SmallSQLEndpoint.size | Should -Be "SMALL"
        }
        It 'Should be cluster_size Small' {
            $SmallSQLEndpoint.cluster_size | Should -Be "Small"
        }
        It 'Should be min_num_clusters 1' {
            $SmallSQLEndpoint.min_num_clusters | Should -BeExactly 1
        }
        It 'Should be max_num_clusters 1' {
            $SmallSQLEndpoint.max_num_clusters | Should -BeExactly 1
        }
        It 'Should be auto_stop_mins 10' {
            $SmallSQLEndpoint.auto_stop_mins | Should -BeExactly 10
        }
        It 'Should be auto_resume True' {
            $SmallSQLEndpoint.auto_resume | Should -Be "True"
        }
        It 'Should be spot_instance_policy RELIABILITY_OPTIMIZED' {
            $SmallSQLEndpoint.spot_instance_policy | Should -Be "RELIABILITY_OPTIMIZED"
        }
        It 'Should be enable_photon True' {
            $SmallSQLEndpoint.enable_photon | Should -Be "True"
        }
        It 'Should be enable_serverless_compute False' {
            $SmallSQLEndpoint.enable_serverless_compute | Should -Be "False"
        }
        It 'Contributors group should have CAN_USE permissions' {
            (($SmallSQLEndpointPermissions.Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_USE"
        }
        It 'Readers group should have CAN_USE permissions' {
            (($SmallSQLEndpointPermissions.Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_USE"
        }
    }
    Context 'Medium Endpoint' {
        It 'Should be named Medium' {
            $MediumSQLEndpoint.name | Should -Be "Medium"
        }
        It 'Should be size MEDIUM' {
            $MediumSQLEndpoint.size | Should -Be "MEDIUM"
        }
        It 'Should be cluster_size Medium' {
            $MediumSQLEndpoint.cluster_size | Should -Be "Medium"
        }
        It 'Should be min_num_clusters 1' {
            $MediumSQLEndpoint.min_num_clusters | Should -BeExactly 1
        }
        It 'Should be max_num_clusters 1' {
            $MediumSQLEndpoint.max_num_clusters | Should -BeExactly 1
        }
        It 'Should be auto_stop_mins 10' {
            $MediumSQLEndpoint.auto_stop_mins | Should -BeExactly 10
        }
        It 'Should be auto_resume True' {
            $MediumSQLEndpoint.auto_resume | Should -Be "True"
        }
        It 'Should be spot_instance_policy RELIABILITY_OPTIMIZED' {
            $MediumSQLEndpoint.spot_instance_policy | Should -Be "RELIABILITY_OPTIMIZED"
        }
        It 'Should be enable_photon True' {
            $MediumSQLEndpoint.enable_photon | Should -Be "True"
        }
        It 'Should be enable_serverless_compute False' {
            $MediumSQLEndpoint.enable_serverless_compute | Should -Be "False"
        }
        It 'Contributors group should have CAN_USE permissions' {
            (($MediumSQLEndpointPermissions.Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Contributors"}).all_permissions.permission_level | Should -Be "CAN_USE"
        }
        It 'Readers group should have CAN_USE permissions' {
            (($MediumSQLEndpointPermissions.Content | ConvertFrom-Json).access_control_list | Where-Object {$_.group_name -eq "Readers"}).all_permissions.permission_level | Should -Be "CAN_USE"
        }

    }
}