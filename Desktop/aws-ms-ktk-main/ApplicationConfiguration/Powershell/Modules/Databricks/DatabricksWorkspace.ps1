. $PSScriptRoot\Helper.ps1

Function New-DatabricksWorkspaceFolder {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path
	)

	$workspaceInfo = @{}
	$workspaceInfo.path = $Path
	$workspaceInfojson = New-Object -TypeName PSObject -Property $workspaceInfo | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/workspace/mkdirs" -Token $Token -Body $workspaceInfojson
}

Function Import-Notebook {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path,
		[Parameter(Position = 3)]
		[string]$Format = "SOURCE",
		[Parameter(Position = 4)]
		[string]$Language = "PYTHON",
		[Parameter(Mandatory=$true, Position = 5)]
		[string]$LocalDirectory,
		[Parameter(Position = 6)]
		[boolean]$Overwrite = $false
	)

	$base64string = [Convert]::ToBase64String([IO.File]::ReadAllBytes($LocalDirectory))
	$importnotebookinfo = @{}
	$importnotebookinfo.path = $Path
	$importnotebookinfo.format = $Format
	$importnotebookinfo.language = $Language
	$importnotebookinfo.content = $base64string
	$importnotebookinfo.overwrite = $Overwrite
	$importnotebookjson = New-Object -TypeName PSObject -Property $importnotebookinfo | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/workspace/import" -Token $Token -Body $importnotebookjson
}

Function Import-NotebookDirectory {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$LocalPath,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$WorkspacePath
	)

	#$notebooks = Get-ChildItem $LocalPath | Where-Object {$_.Mode -ne "d-----"}
	$notebooks = [IO.Directory]::GetFiles($LocalPath)
	foreach ($notebook in $notebooks)
	{
		$notebookName = $notebook.Split(".")

		If($notebookName[1] -eq "py") {
			$notebookLanguage = "PYTHON"
		} ElseIf($notebookName[1] -eq "scala") {
			$notebookLanguage = "SCALA"
		} ElseIf($notebookName[1] -eq "sql") {
			$notebookLanguage = "SQL"
		} ElseIf($notebookName[1] -eq "r") {
			$notebookLanguage = "R"
		}

		$notebookDestinationName = Split-Path $notebookName[0] -Leaf
		$importnotebookresponse = Import-Notebook -BaseURI $BaseURI -Token $Token -Path "${WorkspacePath}/${notebookDestinationName}" -Format "SOURCE" -Language $notebookLanguage -LocalDirectory $notebook -Overwrite $true
	}
}

Function Get-DatabricksWorkbookDirectoryContents {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$NotebookDirectory
	)

	$getdirectorycontentsbodyinfo = @{}
	$getdirectorycontentsbodyinfo.path = $NotebookDirectory
	$getdirectorycontentsbody = New-Object -TypeName PSObject -Property $getdirectorycontentsbodyinfo
	$getdirectorycontentsjson = $getdirectorycontentsbody | ConvertTo-Json

	Return Invoke-GetRESTRequest -URI "${BaseURI}/workspace/list" -Token $Token -Body $getdirectorycontentsjson
}

Function Export-Notebook {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Format,
		[Parameter(Position = 4)]
		[boolean]$DirectDownload = $true,
		[Parameter(Mandatory=$true, Position = 5)]
		[string]$OutputPath
	)

	$exportnotebookinfo = @{}
	$exportnotebookinfo.path = $Path
	$exportnotebookinfo.format = $Format
	$exportnotebookjson = New-Object -TypeName PSObject -Property $exportnotebookinfo | ConvertTo-Json

	$exportnotebookuri = "$uribase/workspace/export?path=${Path}"
	If($DirectDownload) {
		$exportnotebookuri += "&direct_download=true"
	}

	Return Invoke-GetRESTRequest -URI $exportnotebookuri -Token $Token -Body $exportnotebookjson -OutFile $OutputPath
}

Function Remove-DatabricksWorkspaceFolder {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path,
		[Parameter(Position = 3)]
		[boolean]$Recursive = $true
	)

	$workspaceInfo = @{}
	$workspaceInfo.path = [string]$Path
	$workspaceInfo.recursive = $Recursive
	$workspaceInfojson = New-Object -TypeName PSObject -Property $workspaceInfo | ConvertTo-Json
	Write-Host $workspaceInfojson

	Return Invoke-PostRESTRequest -URI "${BaseURI}/workspace/delete" -Token $Token -Body $workspaceInfojson
}
Function Export-NotebookDirectory {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path,
		[Parameter(Position = 3)]
		[boolean]$DirectDownload = $true,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$OutputPath,
		[Parameter(Mandatory=$true, Position = 5)]
		[string]$ExportFormat
	)

	$Contents = Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory $Path
	$Notebooks = ($Contents.Content | ConvertFrom-Json).objects
	ForEach ($Notebook in $Notebooks) {
		if ($Notebook.object_type -eq "NOTEBOOK") {
			$NotebookPath = $Notebook.path
			$NotebookName = $NotebookPath.Split("/")[-1]
			$Language = $Notebook.language
			$Extension = Switch($Language) {
				"PYTHON" {"py"}
				"SCALA" {"scala"}
				"SQL" {"sql"}
				"R" {"r"}
			}
			$NotebookOutputPath = "${OutputPath}\${NotebookName}.${Extension}"
			Write-Host "Export-Notebook -BaseURI $BaseURI -Token $Token -Path $NotebookPath -Format $ExportFormat -DirectDownload $DirectDownload -OutputPath $NotebookOutputPath"
			Start-Sleep -Seconds 5
			Export-Notebook -BaseURI $BaseURI -Token $Token -Path $NotebookPath -Format $ExportFormat -DirectDownload $DirectDownload -OutputPath $NotebookOutputPath
		}
	}
}

Function Export-NotebookDirectoryRecursive {
    param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$NotebookDirectoryBase,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$NotebookDirectory,
        [Parameter(Mandatory=$false, Position = 4)]
        [bool]$DirectDownload = $true,
        [Parameter(Mandatory=$true, Position = 5)]
        [string]$OutputPathBase,
        [Parameter(Mandatory = $true, Position = 6)]
        [string]$OutputPath,
        [Parameter(Mandatory=$false, Position = 7)]
	    [string]$ExportFormat = "SOURCE"
	)
    Export-NotebookDirectory -BaseURI $BaseURI -Token $Token -Path $NotebookDirectory -DirectDownload $DirectDownload -OutputPath $OutputPath -ExportFormat $ExportFormat
    $Contents = Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory $NotebookDirectory
    $DirectoryObjects = ($Contents.Content | ConvertFrom-Json).objects
    foreach($DirectoryObject in $DirectoryObjects) {
        $ObjectType = $DirectoryObject.object_type
        if ($ObjectType -eq "DIRECTORY") {
            $Path = $DirectoryObject.path
            $OutputPath = $Path.Replace($NotebookDirectoryBase, $OutputPathBase).Replace("/","\")
            Write-Host "Exporting Noteooks from $Path to $OutputPath"
            mkdir $OutputPath -Force
            Export-NotebookDirectoryRecursive -BaseURI $BaseURI -Token $Token -NotebookDirectoryBase $NotebookDirectoryBase -NotebookDirectory $Path -DirectDownload $DirectDownload -OutputPathBase $OutputPathBase -OutputPath $OutputPath -ExportFormat $ExportFormat
        }
    }
}