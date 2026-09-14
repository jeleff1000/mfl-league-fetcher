import unittest

from scripts.newspaper_review.build_residual_queue import (
    classify_source,
    merge_signal_rows,
)


class ClassifySourceTests(unittest.TestCase):
    def test_asset_problem_is_excluded_even_with_other_signals(self):
        result = classify_source(
            decision="asset_problem",
            notes="scoring summary present but too small",
            sanity_flags={"ocr_scoring_terms_no_scoring_event"},
            consistency_flags={"low_atom_count_vs_paper_year"},
            closure_status="closed_with_source_note",
        )
        self.assertTrue(result["excluded"])
        self.assertEqual(result["tier"], 0)
        self.assertIn("asset_problem", result["reasons"])

    def test_explicit_deferred_evidence_is_tier_one_despite_generic_closure(self):
        result = classify_source(
            decision="extract_atoms",
            notes="The scoring summary is present but too small for promotion in this pass.",
            sanity_flags=set(),
            consistency_flags={"missing_typical_scoring_event"},
            closure_status="closed_with_source_note",
        )
        self.assertFalse(result["excluded"])
        self.assertEqual(result["tier"], 1)
        self.assertIn("explicit_deferred_evidence", result["reasons"])
        self.assertIn("generic_closure_not_exhaustion_proof", result["reasons"])

    def test_direct_structural_gap_precedes_raw_ocr_and_consistency(self):
        result = classify_source(
            decision="extract_atoms",
            notes="",
            sanity_flags={
                "extract_missing_game_candidate",
                "ocr_attendance_terms_no_attendance_atom",
            },
            consistency_flags={"low_atom_count_vs_paper_year"},
            closure_status="",
        )
        self.assertEqual(result["tier"], 2)
        self.assertIn("direct_structural_gap", result["reasons"])

    def test_raw_ocr_gap_is_tier_three(self):
        result = classify_source(
            decision="extract_atoms",
            notes="",
            sanity_flags={"ocr_lineup_terms_no_lineup_or_skip_note"},
            consistency_flags=set(),
            closure_status="",
        )
        self.assertEqual(result["tier"], 3)

    def test_consistency_only_is_tier_four(self):
        result = classify_source(
            decision="extract_atoms",
            notes="",
            sanity_flags=set(),
            consistency_flags={"low_atom_count_vs_paper_year"},
            closure_status="",
        )
        self.assertEqual(result["tier"], 4)


class MergeSignalRowsTests(unittest.TestCase):
    def test_merge_deduplicates_source_and_accumulates_flags(self):
        merged = merge_signal_rows(
            sanity_rows=[
                {
                    "source_document_id": "doc1",
                    "flag": "ocr_scoring_terms_no_scoring_event",
                    "decision": "extract_atoms",
                    "notes_excerpt": "",
                },
                {
                    "source_document_id": "doc1",
                    "flag": "ocr_attendance_terms_no_attendance_atom",
                    "decision": "extract_atoms",
                    "notes_excerpt": "",
                },
            ],
            consistency_rows=[
                {
                    "source_document_id": "doc1",
                    "flags": "low_atom_count_vs_paper_year;missing_typical_attendance",
                }
            ],
            reviewed_rows=[],
            closure_rows=[],
        )
        self.assertEqual(list(merged), ["doc1"])
        self.assertEqual(
            merged["doc1"]["sanity_flags"],
            {
                "ocr_scoring_terms_no_scoring_event",
                "ocr_attendance_terms_no_attendance_atom",
            },
        )
        self.assertEqual(
            merged["doc1"]["consistency_flags"],
            {"low_atom_count_vs_paper_year", "missing_typical_attendance"},
        )


if __name__ == "__main__":
    unittest.main()
