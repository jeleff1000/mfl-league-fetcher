param(
    [string]$PacketRunDir = "D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_packets\20260625T145621Z_1920_1939_batch0001_llm_review_packets_row_schema_handoff_v3",
    [string]$ToolRoot = "D:\league-history-data\nfl\tools\newspaper_atom_conveyor",
    [string]$LabelPrefix = "1920_1939_batch0001",
    [switch]$BuildOcrFollowupReviewPackets,
    [int]$OcrFollowupReviewPacketDocs = 2,
    [int]$MaxOcrFollowupChars = 50000,
    [switch]$AutoAcceptPromotionReviewReady,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Invoke-ConveyorStep {
    param(
        [string]$Name,
        [string]$ScriptName,
        [string[]]$Arguments
    )

    $scriptPath = Join-Path $ToolRoot $ScriptName
    if (!(Test-Path -LiteralPath $scriptPath)) {
        throw "Missing conveyor tool: $scriptPath"
    }

    $commandPreview = @("py", "-3.10", $scriptPath) + $Arguments
    Write-Host ""
    Write-Host "== $Name =="
    Write-Host ($commandPreview -join " ")

    if ($DryRun) {
        return
    }

    & py -3.10 $scriptPath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Conveyor step failed: $Name ($LASTEXITCODE)"
    }
}

$suffix = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")

Invoke-ConveyorStep `
    -Name "Refresh LLM read-state" `
    -ScriptName "newspaper_llm_read_state.py" `
    -Arguments @(
        "--packet-run-dir", $PacketRunDir,
        "--label", "${LabelPrefix}_llm_read_state_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Ingest LLM reviews" `
    -ScriptName "newspaper_llm_review_ingest.py" `
    -Arguments @(
        "--packet-run-dir", $PacketRunDir,
        "--label", "${LabelPrefix}_llm_review_ingest_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Materialize reviewed rows" `
    -ScriptName "newspaper_llm_materialize_rows.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_llm_materialized_rows_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Build promotion packages" `
    -ScriptName "newspaper_llm_promotion_packages.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_llm_promotion_packages_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Build action queues" `
    -ScriptName "newspaper_conveyor_action_queues.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_conveyor_action_queues_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Prepare OCR follow-up work" `
    -ScriptName "newspaper_ocr_followup_preps.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_ocr_followup_prep_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Build OCR follow-up round manifest" `
    -ScriptName "newspaper_ocr_followup_round_manifest.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_ocr_followup_round_manifest_refresh_${suffix}"
    )

if ($BuildOcrFollowupReviewPackets) {
    Invoke-ConveyorStep `
        -Name "Prepare OCR follow-up review packets" `
        -ScriptName "newspaper_ocr_followup_review_packets.py" `
        -Arguments @(
            "--label", "${LabelPrefix}_ocr_followup_review_packets_refresh_${suffix}",
            "--packet-docs", "$OcrFollowupReviewPacketDocs",
            "--max-ocr-chars", "$MaxOcrFollowupChars"
        )
}

Invoke-ConveyorStep `
    -Name "Prepare semantic follow-up work" `
    -ScriptName "newspaper_semantic_followup_preps.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_semantic_followup_prep_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Prepare quality review work" `
    -ScriptName "newspaper_quality_review_preps.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_quality_review_prep_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Prepare promotion review work" `
    -ScriptName "newspaper_promotion_review_preps.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_promotion_review_prep_refresh_${suffix}"
    )

Invoke-ConveyorStep `
    -Name "Build review decision ledger" `
    -ScriptName "newspaper_review_decision_ledger.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_review_decision_ledger_refresh_${suffix}"
    )

$decisionApplyArgs = @(
    "--label", "${LabelPrefix}_review_decision_apply_refresh_${suffix}"
)
if ($AutoAcceptPromotionReviewReady) {
    $decisionApplyArgs += "--auto-accept-promotion-review-ready"
}

Invoke-ConveyorStep `
    -Name "Apply review decisions locally" `
    -ScriptName "newspaper_review_decision_apply.py" `
    -Arguments $decisionApplyArgs

Invoke-ConveyorStep `
    -Name "Write conveyor status report" `
    -ScriptName "newspaper_conveyor_status_report.py" `
    -Arguments @(
        "--label", "${LabelPrefix}_conveyor_status_refresh_${suffix}"
    )

Write-Host ""
if ($DryRun) {
    Write-Host "Dry run complete. No conveyor steps were executed."
} else {
    Write-Host "Conveyor refresh complete."
}
