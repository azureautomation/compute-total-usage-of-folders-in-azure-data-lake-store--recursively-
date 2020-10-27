workflow adlssizes {
    Param (
        [Parameter (Mandatory=$true)]
        [String] $adlsAccount,

        [Parameter (Mandatory=$false)]
        [String] $rootPath = "/",

        [Parameter (Mandatory=$false)]
        [String] $adlsOutputDir = "/adlssize",

        [Parameter (Mandatory=$false)]
        [int] $parallelJobs = 5,

        [Parameter (Mandatory=$false)]
        [bool] $failOnError = $False
    )
    $ErrorActionPreference = "Stop" 

    $Conn = Get-AutomationConnection -Name AzureRunAsConnection
    Add-AzureRMAccount -ServicePrincipal -Tenant $Conn.TenantID -ApplicationId $Conn.ApplicationID -CertificateThumbprint $Conn.CertificateThumbprint

    $rundate=Get-Date -format yyyy-MM-dd

    $outPath="$adlsOutputDir/$rundate.tsv"
    New-AzureRmDataLakeStoreItem -Account $adlsAccount -Path $outPath -Value "date`tname`tsize`n" -Force

    $dirs = Get-AzureRmDataLakeStoreChildItem -Account $adlsAccount -Path $rootPath | sort
    ForEach -Parallel -ThrottleLimit $parallelJobs ($topdir in $dirs) {
        InlineScript {     
            $Conn = $Using:Conn
            $topdir = $Using:topdir
            $adlsAccount = $Using:adlsAccount
            $rootPath = $Using:rootPath
            $rundate = $Using:rundate
            $outPath = $Using:outPath
            $failOnError = $Using:failOnError

            if ($topdir.Type -eq "DIRECTORY") {
                $topdirName = $topdir.Name
                
                $totalSize = 0
                $stack = New-Object System.Collections.Stack
                $stack.Push($rootPath  + '/' + $topdirName)
                while ($stack.Count -gt 0) {
                    $dir = $stack.Pop()
                    try {
                        $children = Get-AzureRmDataLakeStoreChildItem -Account $adlsAccount -Path $dir
                        ForEach ($child in $children) {
                            $totalSize += $child.Length
                            if ($child.Type -eq "DIRECTORY") {
                                $stack.Push($dir + '/' + $child.Name)
                            }
                        }
                    }
                    catch [Microsoft.Azure.Management.DataLake.Store.Models.AdlsErrorException]
                    { 
                        Write-Host "Could not list content of $dir"
                        if ($failOnError) { 
                            throw [System.Exception]::new("Could not list content of $dir",$PSItem.Exception)
                        }
                    }
                }

                Add-AzureRmDataLakeStoreItemContent -Account $adlsAccount -Path $outPath -Value "$rundate`t$topdirName`t$totalSize`n" 
            }
        }
    }
}
