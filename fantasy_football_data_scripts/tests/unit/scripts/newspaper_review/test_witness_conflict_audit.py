import json
import unittest

from scripts.newspaper_review.witness_conflict_audit import (
    canonical_team_values,
    evidence_payload,
    find_scalar_conflicts,
    normalize_line_score,
    team_stat_comparison_rows,
)


class WitnessConflictAuditTests(unittest.TestCase):
    def test_distinct_values_from_two_sources_form_a_scalar_conflict(self):
        self.assertIsNotNone(find_scalar_conflicts)
        rows = [
            {
                "boxscore_id": "193311300bkn",
                "stat_name": "attendance",
                "value": "25,000",
                "source_document_id": "source-a",
            },
            {
                "boxscore_id": "193311300bkn",
                "stat_name": "attendance",
                "value": "24,000",
                "source_document_id": "source-b",
            },
        ]

        conflicts = find_scalar_conflicts(
            rows,
            key_fields=("boxscore_id", "stat_name"),
            value_field="value",
        )

        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["values"], ["24000", "25000"])
        self.assertEqual(conflicts[0]["source_documents"], ["source-a", "source-b"])

    def test_reversed_team_order_has_one_canonical_score_value(self):
        self.assertIsNotNone(canonical_team_values)
        forward = canonical_team_values("New York Giants", "10", "Brooklyn Dodgers", "0")
        reverse = canonical_team_values("Brooklyn Dodgers", "0", "New York Giants", "10")

        self.assertEqual(forward, "brooklyn dodgers=0|new york giants=10")
        self.assertEqual(forward, reverse)

    def test_integer_zero_is_preserved_in_canonical_team_values(self):
        self.assertEqual(
            canonical_team_values("RII", 3, "GNB", 0),
            "gnb=0|rii=3",
        )

    def test_two_values_inside_one_document_are_not_a_witness_conflict(self):
        rows = [
            {
                "boxscore_id": "193311300bkn",
                "stat_name": "attendance",
                "value": "25,000",
                "source_document_id": "source-a",
            },
            {
                "boxscore_id": "193311300bkn",
                "stat_name": "attendance",
                "value": "24,000",
                "source_document_id": "source-a",
            },
        ]

        conflicts = find_scalar_conflicts(
            rows,
            key_fields=("boxscore_id", "stat_name"),
            value_field="value",
        )

        self.assertEqual(conflicts, [])

    def test_team_stat_comparisons_keep_each_team_on_its_own_key(self):
        self.assertIsNotNone(team_stat_comparison_rows)
        rows = [
            {
                "boxscore_id": "193611290was",
                "stat_name": "first_downs",
                "team_1_nfl_team": "BOS",
                "team_1_value": "10",
                "team_2_nfl_team": "PIT",
                "team_2_value": "2",
                "source_document_id": "source-a",
            }
        ]

        facts = team_stat_comparison_rows(rows)

        self.assertEqual(
            {(fact["_comparison_key"], fact["_comparison_value"]) for fact in facts},
            {
                ("193611290was|first_downs|bos", "10"),
                ("193611290was|first_downs|pit", "2"),
            },
        )

    def test_full_line_score_encodings_normalize_and_partial_values_do_not(self):
        self.assertIsNotNone(normalize_line_score)
        self.assertEqual(normalize_line_score("7,14,7,0,total_28"), "7|14|7|0|28")
        self.assertEqual(normalize_line_score("7,14,7,0=28"), "7|14|7|0|28")
        self.assertEqual(normalize_line_score("7"), "")

    def test_generated_decision_payload_preserves_crop_and_evidence_text(self):
        payload = evidence_payload(
            {
                "atom_claim_id": "decision-id",
                "generated_proposed_fields_json": json.dumps(
                    {
                        "artifact_path": r"D:\league-history-data\crop.png",
                        "evidence_text": "Brooklyn won 21-3.",
                        "region_id": "visual-review-region",
                        "source_document_id": "source-a",
                    }
                ),
            }
        )

        self.assertEqual(payload["evidence_image_path"], r"D:\league-history-data\crop.png")
        self.assertEqual(payload["evidence_text"], "Brooklyn won 21-3.")
        self.assertEqual(payload["region_id"], "visual-review-region")
        self.assertEqual(payload["source_document_id"], "source-a")


if __name__ == "__main__":
    unittest.main()
