Function Get-StringHash() {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [String] $String,
        [Parameter(Mandatory = $true, Position = 1)]
        [String] $Salt,
        [Parameter(Mandatory = $false, Position = 2)]
        $HashName = "MD5"
    )
    $Crypto = New-Object System.Security.Cryptography.SHA256Managed | Select-Object -Index 0;
    $Hashed = $Crypto.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Salt + $String)) | ForEach-Object {$_.ToString("x2")};
    return [system.String]::Join("", $Hashed);
}

Function New-UniqueString {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$String,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$Salt,
        [Parameter(Mandatory = $false, Position = 2)]
        [int]$Length = 3
    )
    $UniqueString =(Get-StringHash -Salt $Salt -String $String);
    $Match = [regex]::Match($UniqueString, '[a-zA-Z]');
    $UniqueString = $UniqueString.SubString($Match.Index, $Length);
    Return $UniqueString
}
