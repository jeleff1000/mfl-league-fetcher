param(
    [string]$OutDir = (Join-Path (Split-Path -Parent $PSScriptRoot) "tmp\cfb_2025_pilot"),
    [int]$Year = 2025,
    [int]$CdpPort = 9223,
    [int]$SleepSeconds = 4,
    [string[]]$PageKinds = @("main", "gamelog", "splits")
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $RepoRoot "tmp\cfb_player_scrape_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$SupervisorLog = Join-Path $LogDir "finish_cfb_players_$Year.log"
$FinalStatusPath = Join-Path $LogDir "finish_cfb_players_$Year.status.json"
$Scraper = Join-Path $RepoRoot "scripts\scrape_cfb_tables.py"

function Write-SupervisorLog {
    param([string]$Message)
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -LiteralPath $SupervisorLog -Value $line
}

function Invoke-CfbStatus {
    $json = & python $Scraper status --out-dir $OutDir
    if ($LASTEXITCODE -ne 0) {
        throw "status command failed with exit code $LASTEXITCODE"
    }
    return ($json | ConvertFrom-Json)
}

function Invoke-CfbCommand {
    param([string[]]$Arguments, [string]$Label)
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $outLog = Join-Path $LogDir "$Label`_$Year`_$stamp.out.log"
    $errLog = Join-Path $LogDir "$Label`_$Year`_$stamp.err.log"
    Write-SupervisorLog "starting $Label stdout=$outLog stderr=$errLog"
    $pythonArgs = @($Scraper) + $Arguments
    $process = Start-Process -FilePath "python" `
        -ArgumentList $pythonArgs `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -WindowStyle Hidden `
        -PassThru
    $process.WaitForExit()
    $code = $process.ExitCode
    Write-SupervisorLog "$Label exited code=$code"
    return $code
}

function Get-CompletedPlayerPages {
    param($Status)
    if ($Status.datasets.PSObject.Properties.Name -contains "players") {
        return [int]($Status.datasets.players.completed_pages)
    }
    return 0
}

Write-SupervisorLog "player supervisor started out_dir=$OutDir year=$Year cdp_port=$CdpPort sleep=$SleepSeconds page_kinds=$($PageKinds -join ',')"

$commonFetch = @(
    "--out-dir", $OutDir,
    "--year", "$Year",
    "--cdp-port", "$CdpPort",
    "--fetch-mode", "cdp",
    "--sleep", "$SleepSeconds",
    "--challenge-retries", "2",
    "--challenge-sleep", "20",
    "--skip-existing"
)
foreach ($kind in $PageKinds) {
    $commonFetch += @("--page-kind", $kind)
}

while ($true) {
    $status = Invoke-CfbStatus
    $players = [int]($status.players)
    if ($players -le 0) {
        throw "No player_index.parquet rows found in $OutDir"
    }
    $target = $players * $PageKinds.Count
    $done = Get-CompletedPlayerPages $status
    Write-SupervisorLog "player status completed=$done/$target players=$players"
    if ($done -ge $target) {
        break
    }

    $before = $done
    $code = Invoke-CfbCommand -Label "players" -Arguments (@("scrape-players") + $commonFetch)
    $afterStatus = Invoke-CfbStatus
    $after = Get-CompletedPlayerPages $afterStatus
    if ($code -ne 0 -and $after -le $before) {
        throw "player scrape exited with $code and made no page progress"
    }
    Start-Sleep -Seconds 60
}

Invoke-CfbCommand -Label "compact_players" -Arguments @(
    "compact",
    "--out-dir", $OutDir,
    "--dataset", "players",
    "--delete-parts"
) | Out-Null

$final = Invoke-CfbStatus
$final | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $FinalStatusPath
Write-SupervisorLog "finished player scrape; final status at $FinalStatusPath"
