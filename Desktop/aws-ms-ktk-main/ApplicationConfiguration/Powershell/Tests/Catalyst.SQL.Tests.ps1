Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\SQL\AzureSQLModule.ps1
. $ModulesDirectory\SQL\SqlServerModule.ps1

$Server = Get-AzSqlServer -ResourceGroupName $ResourceGroupName
$Databases = Get-Databases -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName
$MetadataDB = $Databases | Where-Object {$_.DatabaseName -eq $MetadataDBName}
$DWDB = $Databases | Where-Object {$_.DatabaseName -eq $DWDatabaseName}

$TableSQL = @"
SELECT
     TABLE_SCHEMA + '.' + TABLE_NAME AS TableName
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE';
"@

$ProcSQL = @"
SELECT
     s.name + '.' + o.name AS ProcedureName
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type_desc = 'SQL_STORED_PROCEDURE'
"@

$Tables = Invoke-Sqlcmd -ServerInstance "$SQLServerName.database.windows.net" -Database $MetadataDBName -Username $SQLServerLogin -Password $SQLServerPwd -Query $TableSQL
$Procs = Invoke-SqlCmd -ServerInstance "$SQLServerName.database.windows.net" -Database $MetadataDBName -UserName $SQLServerLogin -Password $SQLServerPwd -Query $ProcSQL

Describe 'Azure SQL Unit Tests' {
    Context 'Server' {
        It 'Azure SQL Server Exists' {
            $Server.ServerName | Should -Be $SQLServerName
        }
        It 'Version should be 12.0' {
            $Server.ServerVersion | Should -Be "12.0"
        }
    }
    Context 'MetaDataDB' {
        It 'Metadata DB Exists' {
            $MetadataDB.DatabaseName | Should -Be $MetadataDBName
        }
        It 'Edition should be Basic' {
            $MetadataDB.Edition | Should -Be "Basic"
        }
        It 'Collation should be SQL_Latin1_General_CP1_CI_AS' {
            $MetadataDB.CollationName | Should -Be "SQL_Latin1_General_CP1_CI_AS"
        }
        It 'Max Size Should be >= 2 GB' {
            $MetadataDB.MaxSizeBytes | Should -BeGreaterOrEqual 2147483648
        }
        It 'Status should be Online' {
            $MetadataDB.Status | Should -Be "Online"
        }
        It 'Capacity should be >= 5' {
            $MetadataDB.Capacity | Should -BeGreaterOrEqual 5
        }
        It 'dbo.DatabaseCatalog Table should exist' {
            "dbo.DatabaseCatalog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogColumn Table should exist' {
            "dbo.DataCatalogColumn" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogColumnValueCounts Table should exist' {
            "dbo.DataCatalogColumnValueCounts" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogDatabase Table should exist' {
            "dbo.DataCatalogDatabase" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogFile Table should exist' {
            "dbo.DataCatalogFile" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogFileGrowth Table should exist' {
            "dbo.DataCatalogFileGrowth" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogTable Table should exist' {
            "dbo.DataCatalogTable" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataCatalogTableGrowth Table should exist' {
            "dbo.DataCatalogTableGrowth" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataLakeZone Table should exist' {
            "dbo.DataLakeZone" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataQualityValidationResult Table should exist' {
            "dbo.DataQualityValidationResult" | Should -BeIn $Tables.TableName
        }
        It 'dbo.DataQualityValidationResultDetail Table should exist' {
            "dbo.DataQualityValidationResultDetail" | Should -BeIn $Tables.TableName
        }
        It 'dbo.Job Table should exist' {
            "dbo.Job" | Should -BeIn $Tables.TableName
        }
        It 'dbo.JobLog Table should exist' {
            "dbo.JobLog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.LogStatus Table should exist' {
            "dbo.LogStatus" | Should -BeIn $Tables.TableName
        }
        It 'dbo.NotebookLog Table should exist' {
            "dbo.NotebookLog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.Parameter Table should exist' {
            "dbo.Parameter" | Should -BeIn $Tables.TableName
        }
        It 'dbo.Project Table should exist' {
            "dbo.Project" | Should -BeIn $Tables.TableName
        }
        It 'dbo.ProjectLog Table should exist' {
            "dbo.ProjectLog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.Stage Table should exist' {
            "dbo.Stage" | Should -BeIn $Tables.TableName
        }
        It 'dbo.StageLog Table should exist' {
            "dbo.StageLog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.Step Table should exist' {
            "dbo.Step" | Should -BeIn $Tables.TableName
        }
        It 'dbo.StepLog Table should exist' {
            "dbo.StepLog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.System Table should exist' {
            "dbo.System" | Should -BeIn $Tables.TableName
        }
        It 'dbo.SystemLog Table should exist' {
            "dbo.SystemLog" | Should -BeIn $Tables.TableName
        }
        It 'dbo.Validation  Table should exist' {
            "dbo.Validation" | Should -BeIn $Tables.TableName
        }
        It 'dbo.ValidationObjectType Table should exist' {
            "dbo.ValidationObjectType" | Should -BeIn $Tables.TableName
        }
        It 'dbo.ValidationStatus  Table should exist' {
            "dbo.ValidationStatus" | Should -BeIn $Tables.TableName
        }
        It 'dbo.WindowedExtraction  Table should exist' {
            "dbo.WindowedExtraction" | Should -BeIn $Tables.TableName
        }
        It 'dbo.GetProjectRunDetail Stored Procedure should exist' {
            "dbo.GetProjectRunDetail" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.GetProjectRuns Stored Procedure should exist' {
            "dbo.GetProjectRuns" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.InsertProject Stored Procedure should exist' {
            "dbo.InsertProject" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.InsertSystem Stored Procedure should exist' {
            "dbo.InsertSystem" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogJobEnd Stored Procedure should exist' {
            "dbo.LogJobEnd" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogJobError Stored Procedure should exist' {
            "dbo.LogJobError" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogJobStart Stored Procedure should exist' {
            "dbo.LogJobStart" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogNotebookEnd Stored Procedure should exist' {
            "dbo.LogNotebookEnd" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogNotebookError Stored Procedure should exist' {
            "dbo.LogNotebookError" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogNotebookStart Stored Procedure should exist' {
            "dbo.LogNotebookStart" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogProjectEnd Stored Procedure should exist' {
            "dbo.LogProjectEnd" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogProjectError Stored Procedure should exist' {
            "dbo.LogProjectError" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogProjectStart Stored Procedure should exist' {
            "dbo.LogProjectStart" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogStageEnd Stored Procedure should exist' {
            "dbo.LogStageEnd" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogStageError Stored Procedure should exist' {
            "dbo.LogStageError" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogStageStart Stored Procedure should exist' {
            "dbo.LogStageStart" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogStepEnd Stored Procedure should exist' {
            "dbo.LogStepEnd" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogStepError Stored Procedure should exist' {
            "dbo.LogStepError" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogStepStart Stored Procedure should exist' {
            "dbo.LogStepStart" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogSystemEnd Stored Procedure should exist' {
            "dbo.LogSystemEnd" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogSystemError Stored Procedure should exist' {
            "dbo.LogSystemError" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.LogSystemStart Stored Procedure should exist' {
            "dbo.LogSystemStart" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.ResetJob Stored Procedure should exist' {
            "dbo.ResetJob" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.ResetProject Stored Procedure should exist' {
            "dbo.ResetProject" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.ResetStage Stored Procedure should exist' {
            "dbo.ResetStage" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.ResetSystem Stored Procedure should exist' {
            "dbo.ResetSystem" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.TruncateHydration Stored Procedure should exist' {
            "dbo.TruncateHydration" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.InsertStage Stored Procedure should exist' {
            "dbo.InsertStage" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.InsertJob Stored Procedure should exist' {
            "dbo.InsertJob" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.InsertStep Stored Procedure should exist' {
            "dbo.InsertStep" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.InsertParameters Stored Procedure should exist' {
            "dbo.InsertParameters" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.HydrateAppInsights Stored Procedure should exist' {
            "dbo.HydrateAppInsights" | Should -BeIn $Procs.ProcedureName
        }
        #It 'dbo.HydrateBatchParquet Stored Procedure should exist' {
        #    "dbo.HydrateBatchParquet" | Should -BeIn $Procs.ProcedureName
        #}
        It 'dbo.HydrateBatchSQL Stored Procedure should exist' {
            "dbo.HydrateBatchSQL" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.HydrateCosmos Stored Procedure should exist' {
            "dbo.HydrateCosmos" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.HydratePresentationZone Stored Procedure should exist' {
            "dbo.HydratePresentationZone" | Should -BeIn $Procs.ProcedureName
        }
        It 'dbo.HydrateGoldZone Stored Procedure should exist' {
            "dbo.HydrateGoldZone" | Should -BeIn $Procs.ProcedureName
        }
    }
    Context 'Data Warehouse DB' {
        It 'Data Warehouse DB Exists' {
            $DWDB.DatabaseName | Should -Be $DWDatabaseName
        }
        It 'Collation should be SQL_Latin1_General_CP1_CI_AS' {
            $DWDB.CollationName | Should -Be "SQL_Latin1_General_CP1_CI_AS"
        }
        It 'Max Size Should be >= 2 GB' {
            $DWDB.MaxSizeBytes | Should -BeGreaterOrEqual 2147483648
        }
        It 'Status should be Online' {
            $DWDB.Status | Should -Be "Online"
        }
        It 'Capacity should be >= 5' {
            $DWDB.Capacity | Should -BeGreaterOrEqual 5
        }
    }
}