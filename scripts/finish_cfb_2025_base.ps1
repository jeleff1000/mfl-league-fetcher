param(
    [string]$OutDir = (Join-Path (Split-Path -Parent $PSScriptRoot) "tmp\cfb_2025_pilot"),
    [int]$Year = 2025,
    [int]$CdpPort = 9223,
    [int]$SleepSeconds = 4
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $RepoRoot "tmp\cfb_2025_scrape_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$SupervisorLog = Join-Path $LogDir "finish_cfb_2025_base.log"
$FinalStatusPath = Join-Path $LogDir "finish_cfb_2025_base_status.json"
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
    $log = Join-Path $LogDir "$Label`_$stamp.log"
    Write-SupervisorLog "starting $Label log=$log"
    $output = & python $Scraper @Arguments 2>&1
    $output | Set-Content -LiteralPath $log
    $code = $LASTEXITCODE
    Write-SupervisorLog "$Label exited code=$code"
    return $code
}

Write-SupervisorLog "base supervisor started out_dir=$OutDir year=$Year cdp_port=$CdpPort sleep=$SleepSeconds"

$commonFetch = @(
    "--out-dir", $OutDir,
    "--cdp-port", "$CdpPort",
    "--fetch-mode", "cdp",
    "--sleep", "$SleepSeconds",
    "--challenge-retries", "2",
    "--challenge-sleep", "20",
    "--skip-existing",
    "--stop-on-error"
)

Invoke-CfbCommand -Label "context" -Arguments (@("scrape-pages", "--group", "context") + $commonFetch) | Out-Null
Invoke-CfbCommand -Label "team_pages" -Arguments (@("scrape-pages", "--group", "team") + $commonFetch) | Out-Null

while ($true) {
    $status = Invoke-CfbStatus
    $target = [int]($status.boxscores)
    $done = 0
    if ($status.datasets.PSObject.Properties.Name -contains "boxscores") {
        $done = [int]($status.datasets.boxscores.completed_pages)
    }
    Write-SupervisorLog "boxscore status completed=$done/$target"
    if ($target -gt 0 -and $done -ge $target) {
        break
    }
    $before = $done
    $code = Invoke-CfbCommand -Label "boxscores" -Arguments (@("scrape-boxscores") + $commonFetch)
    $afterStatus = Invoke-CfbStatus
    $after = 0
    if ($afterStatus.datasets.PSObject.Properties.Name -contains "boxscores") {
        $after = [int]($afterStatus.datasets.boxscores.completed_pages)
    }
    if ($code -ne 0 -and $after -le $before) {
        throw "boxscore scrape exited with $code and made no page progress"
    }
    Start-Sleep -Seconds 60
}

foreach ($dataset in @("context", "team_pages", "boxscores")) {
    Invoke-CfbCommand -Label "compact_$dataset" -Arguments @(
        "compact",
        "--out-dir", $OutDir,
        "--dataset", $dataset,
        "--delete-parts"
    ) | Out-Null
}

Invoke-CfbCommand -Label "build_player_index" -Arguments @(
    "build-player-index",
    "--out-dir", $OutDir
) | Out-Null

$final = Invoke-CfbStatus
$final | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $FinalStatusPath
Write-SupervisorLog "finished base scrape; final status at $FinalStatusPath"
