param(
    [string]$ToolRoot = "D:\league-history-data\nfl\tools\newspaper_atom_conveyor",
    [string]$ReadStateDir = "",
    [string]$PacketId = "",
    [string]$Label = "packet_review_prep",
    [int]$MaxRegionChars = 2500,
    [int]$PreviewLines = 120,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $ToolRoot "newspaper_packet_review_prep.py"
if (!(Test-Path -LiteralPath $scriptPath)) {
    throw "Missing conveyor tool: $scriptPath"
}

$arguments = @("--label", $Label, "--max-region-chars", "$MaxRegionChars", "--preview-lines", "$PreviewLines")
if ($ReadStateDir) {
    $arguments += @("--read-state-dir", $ReadStateDir)
}
if ($PacketId) {
    $arguments += @("--packet-id", $PacketId)
}

$commandPreview = @("py", "-3.10", $scriptPath) + $arguments
Write-Host ($commandPreview -join " ")

if ($DryRun) {
    Write-Host "Dry run complete. Packet prep was not executed."
    exit 0
}

& py -3.10 $scriptPath @arguments
if ($LASTEXITCODE -ne 0) {
    throw "Packet review prep failed ($LASTEXITCODE)"
}
