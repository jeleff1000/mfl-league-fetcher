param(
    [string]$ToolRoot = "D:\league-history-data\nfl\tools\newspaper_atom_conveyor",
    [string]$Manifest = "",
    [string]$Label = "newspaper_ocr_followup_review_packets",
    [int]$PacketDocs = 2,
    [int]$MaxOcrChars = 50000,
    [int]$MaxRegionChars = 1200,
    [switch]$NoPriorRegions,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $ToolRoot "newspaper_ocr_followup_review_packets.py"
if (!(Test-Path -LiteralPath $scriptPath)) {
    throw "Missing conveyor tool: $scriptPath"
}

$arguments = @(
    "--label", $Label,
    "--packet-docs", "$PacketDocs",
    "--max-ocr-chars", "$MaxOcrChars",
    "--max-region-chars", "$MaxRegionChars"
)
if ($Manifest) {
    $arguments += @("--manifest", $Manifest)
}
if ($NoPriorRegions) {
    $arguments += "--no-prior-regions"
}

$commandPreview = @("py", "-3.10", $scriptPath) + $arguments
Write-Host ($commandPreview -join " ")

if ($DryRun) {
    Write-Host "Dry run complete. OCR follow-up review packet prep was not executed."
    exit 0
}

& py -3.10 $scriptPath @arguments
if ($LASTEXITCODE -ne 0) {
    throw "OCR follow-up review packet prep failed ($LASTEXITCODE)"
}
