dir $kustoPackagesRoot\* | Unblock-File
[System.Reflection.Assembly]::LoadFrom("$kustoPackagesRoot\Kusto.Data.dll")

$cluster = "$clusterUrl;Fed=True"
$kcsb = New-Object Kusto.Data.KustoConnectionStringBuilder ($cluster, $databaseName)
$kcsb = $kcsb.WithAadApplicationKeyAuthentication($applicationId, $applicationKey, $authority)

function invoke-KustoAdminCommand ($query)
{
    $adminProvider = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslAdminProvider($kcsb)
    Write-Host "Executing command: '$query' with connection string: '$($kcsb.ToString())'"
    $reader = $adminProvider.ExecuteControlCommand($query)
    return $reader
}

function invoke-KustoQuery ($query)
{
    $queryProvider = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslQueryProvider($kcsb)
    Write-Host "Executing query: '$query' with connection string: '$($kcsb.ToString())'"
    #   Optional: set a client request ID and set a client request property (e.g. Server Timeout)
    $crp = New-Object Kusto.Data.Common.ClientRequestProperties
    $crp.ClientRequestId = "MyPowershellScript.ExecuteQuery." + [Guid]::NewGuid().ToString()
    $crp.SetOption([Kusto.Data.Common.ClientRequestProperties]::OptionServerTimeout, [TimeSpan]::FromSeconds(30))
    $reader = $queryProvider.ExecuteQuery($query, $crp)
    $dataTable = [Kusto.Cloud.Platform.Data.ExtendedDataReader]::ToDataSet($reader).Tables[0]
    $dataView = New-Object System.Data.DataView($dataTable)
    #$dataView | Sort StartTime -Descending | Format-Table -AutoSize
    return $dataView
}

function new-KustoExternalTable($relativePath, $dataformat, $tableName, $cols)
{
$query = @"
.create external table $tableName ($cols)
kind=adl
dataformat=$dataformat
(
h@'abfss://$adlsContainerName@$adlsStorageAccountName.dfs.core.windows.net/$relativePath;sharedkey=$adlsStorageAccountKey'
)
with
(
docstring='$tableName'
)
"@
write-host $query
invoke-KustoAdminCommand $query
}

function get-adlsgen2DirectoryContents($directory, $recursive=$false, $maxResults=5000)
{
    # Rest documentation:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list

    $date = [System.DateTime]::UtcNow.ToString("R")

    $n = "`n"
    $method = "GET"

    # $stringToSign = "GET`n`n`n`n`n`n`n`n`n`n`n`n"
    $stringToSign = "$method$n" #VERB
    $stringToSign += "$n" # Content-Encoding + "\n" +
    $stringToSign += "$n" # Content-Language + "\n" +
    $stringToSign += "$n" # Content-Length + "\n" +
    $stringToSign += "$n" # Content-MD5 + "\n" +
    $stringToSign += "$n" # Content-Type + "\n" +
    $stringToSign += "$n" # Date + "\n" +
    $stringToSign += "$n" # If-Modified-Since + "\n" +
    $stringToSign += "$n" # If-Match + "\n" +
    $stringToSign += "$n" # If-None-Match + "\n" +
    $stringToSign += "$n" # If-Unmodified-Since + "\n" +
    $stringToSign += "$n" # Range + "\n" +
    $stringToSign +=
                        <# SECTION: CanonicalizedHeaders + "\n" #>
                        "x-ms-date:$date" + $n +
                        "x-ms-version:2018-11-09" + $n #
                        <# SECTION: CanonicalizedHeaders + "\n" #>
    $stringToSign +=
                        <# SECTION: CanonicalizedResource + "\n" #>
                        "/$adlsStorageAccountName/$adlsContainerName" + $n +
                        "directory:" + $directory + $n +
                        "maxresults:" + $maxResults + $n +
                        "recursive:$recursive" + $n +
                        "resource:filesystem"#
                        <# SECTION: CanonicalizedResource + "\n" #>

    $sharedKey = [System.Convert]::FromBase64String($adlsStorageAccountKey)
    $hasher = New-Object System.Security.Cryptography.HMACSHA256
    $hasher.Key = $sharedKey
    $signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSign)))
    $authHeader = "SharedKey ${adlsStorageAccountName}:$signedSignature"
    $headers = @{"x-ms-date"=$date}
    $headers.Add("x-ms-version","2018-11-09")
    $headers.Add("Authorization",$authHeader)
    $URI = "https://$adlsStorageAccountName.dfs.core.windows.net/" + $adlsContainerName + "?directory=$directory&maxresults=$maxResults&recursive=$recursive&resource=filesystem"
    $URI
    $result = Invoke-RestMethod -method $method -Uri $URI -Headers $headers
    return $result
}
function new-KustoManagedTable($relativePath, $dataformat, $tableName, $cols, $mapping)
{
    $query = ".create table $tableName ($cols)"
    write-host $query
    invoke-KustoAdminCommand $query

    $query = ".create table $tableName ingestion $dataformat mapping 'mapping$tableName' '[$mapping]'"
    write-host $query
    invoke-KustoAdminCommand $query
}

function invoke-KustoManagedTableIngestion($relativePath, $tableName)
{
    $result = get-adlsgen2DirectoryContents $relativePath
    $paths = $result.paths.name
    $query = ".ingest into table $tableName ("
    foreach ($path in $paths)
    {
        $fullyQualifiedPath = "h'abfss://$adlsContainerName@$adlsStorageAccountName.dfs.core.windows.net/$path;sharedkey=$adlsStorageAccountKey',"
        $query += $fullyQualifiedPath
    }
    $query = $query.substring(0,$query.Length-1)
    $query += ") with(jsonMappingReference='mapping$tableName', format='json')"
    invoke-KustoAdminCommand $query

}

