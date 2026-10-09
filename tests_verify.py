"""Comprehensive production verification tests for EU CTIS ETL Pipeline."""

import os
import sys
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Import ETL modules
from ctis_etl import parser, database, config
from ctis_etl.models import RawTrialDossier


class TestCTISEdgeCasesAndIntegrity(unittest.TestCase):
    def setUp(self):
        # Use an isolated temp database for tests
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()
        self.orig_sqlite_path = config.SQLITE_DB_PATH
        config.SQLITE_DB_PATH = Path(self.temp_db.name)
        database.init_sqlite_db()

    def tearDown(self):
        config.SQLITE_DB_PATH = self.orig_sqlite_path
        if os.path.exists(self.temp_db.name):
            try:
                os.remove(self.temp_db.name)
            except Exception:
                pass

    def test_01_six_domain_split_and_validation(self):
        """Verify that sample trial splits cleanly into all 6 valid Silver domain payloads."""
        sample_path = Path("sample/2026-527084-15-00.json")
        self.assertTrue(sample_path.exists(), "Sample payload file must exist")
        with open(sample_path, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)

        # Invariant validation
        dossier = RawTrialDossier(**raw_payload)
        self.assertEqual(dossier.ct_number, "2026-527084-15-00")

        domains = parser.parse_trial_dossier(raw_payload)
        
        expected_files = [
            "meta_data.json",
            "summary.json",
            "full_trial_information.json",
            "trial_documents.json",
            "trial_results.json",
            "locations_and_contact_points.json",
        ]
        for ef in expected_files:
            self.assertIn(ef, domains)
            self.assertIsNotNone(domains[ef])

        self.assertEqual(domains["meta_data.json"]["ctNumber"], "2026-527084-15-00")
        self.assertEqual(domains["summary.json"]["ctNumber"], "2026-527084-15-00")
        self.assertEqual(domains["locations_and_contact_points.json"]["ctNumber"], "2026-527084-15-00")
        self.assertIsInstance(domains["trial_documents.json"], list)
        self.assertIsInstance(domains["trial_results.json"], dict)
        self.assertIsInstance(domains["full_trial_information.json"], dict)

    def test_02_stage_trial_idempotency_and_states(self):
        """Verify stage_trial handles new trials, unchanged trials, and in-flight states."""
        # 1. New trial
        state1 = database.stage_trial("TRIAL-001", "2026-01-01")
        self.assertEqual(state1, "PENDING")

        # 2. Mark success
        database.mark_success("TRIAL-001", "2026-01-01")
        status = database.get_trial_status("TRIAL-001")
        self.assertEqual(status["status"], "SUCCESS")

        # 3. Same publication date -> UNCHANGED
        state2 = database.stage_trial("TRIAL-001", "2026-01-01")
        self.assertEqual(state2, "UNCHANGED")

        # 4. New publication date -> UPDATE_PENDING
        state3 = database.stage_trial("TRIAL-001", "2026-02-01")
        self.assertEqual(state3, "UPDATE_PENDING")

        # 5. In-flight trial marked as PROCESSING must not be overwritten
        database.mark_processing("TRIAL-001")
        state4 = database.stage_trial("TRIAL-001", "2026-02-01")
        self.assertEqual(state4, "PROCESSING")

    def test_03_stale_processing_recovery_preserves_amendments(self):
        """Verify that stale PROCESSING trials return to UPDATE_PENDING if already fetched, or PENDING if brand new."""
        now = datetime.now(timezone.utc)
        stale_time = (now - timedelta(minutes=30)).isoformat()

        with sqlite3.connect(database.get_sqlite_path()) as conn:
            # Trial A: brand new (last_fetched_at is NULL)
            conn.execute(
                "INSERT INTO trials (ct_number, status, created_at, updated_at) VALUES ('TRIAL-A', 'PROCESSING', ?, ?)",
                (stale_time, stale_time),
            )
            # Trial B: existing update (last_fetched_at is set)
            conn.execute(
                "INSERT INTO trials (ct_number, status, last_fetched_at, created_at, updated_at) VALUES ('TRIAL-B', 'PROCESSING', ?, ?, ?)",
                (stale_time, stale_time, stale_time),
            )
            conn.commit()

        recovered = database.reset_stale_processing(timeout_minutes=15)
        self.assertEqual(recovered, 2)

        status_a = database.get_trial_status("TRIAL-A")
        status_b = database.get_trial_status("TRIAL-B")
        self.assertEqual(status_a["status"], "PENDING")
        self.assertEqual(status_b["status"], "UPDATE_PENDING")

    def test_04_mark_failure_retry_transitions(self):
        """Verify retry count increments, temporary failures restore UPDATE_PENDING or PENDING, and threshold triggers FAILED."""
        # Case A: Brand new trial
        database.mark_failure("TRIAL-FAIL-1", "timeout error")
        s1 = database.get_trial_status("TRIAL-FAIL-1")
        self.assertEqual(s1["retry_count"], 1)
        self.assertEqual(s1["status"], "PENDING")

        # Case B: Existing trial with prior fetch
        database.mark_success("TRIAL-FAIL-2", "2026-01-01")
        database.mark_failure("TRIAL-FAIL-2", "server 500 error")
        s2 = database.get_trial_status("TRIAL-FAIL-2")
        self.assertEqual(s2["retry_count"], 1)
        self.assertEqual(s2["status"], "UPDATE_PENDING")

        # Case C: Exceeding 3 retries
        database.mark_failure("TRIAL-FAIL-2", "server 500 error 2")
        database.mark_failure("TRIAL-FAIL-2", "server 500 error 3")
        s3 = database.get_trial_status("TRIAL-FAIL-2")
        self.assertEqual(s3["retry_count"], 3)
        self.assertEqual(s3["status"], "FAILED")

        # Case D: Failed trial receives new revision from EMA -> should reset retry_count and become UPDATE_PENDING
        new_state = database.stage_trial("TRIAL-FAIL-2", "2026-03-01")
        self.assertEqual(new_state, "UPDATE_PENDING")
        s4 = database.get_trial_status("TRIAL-FAIL-2")
        self.assertEqual(s4["retry_count"], 0)

    def test_05_stale_pipeline_run_recovery(self):
        """Verify that stale RUNNING pipeline runs are marked INTERRUPTED without killing active runs."""
        now = datetime.now(timezone.utc)
        old_time = (now - timedelta(hours=6)).isoformat()
        fresh_time = now.isoformat()

        # Run 1: Old stuck run (6 hours ago)
        database.record_pipeline_run("RUN-OLD", "FULL", old_time, status="RUNNING")
        with sqlite3.connect(database.get_sqlite_path()) as conn:
            conn.execute("UPDATE pipeline_runs SET updated_at = ? WHERE run_id = 'RUN-OLD'", (old_time,))
            conn.commit()

        # Run 2: Fresh active run
        database.record_pipeline_run("RUN-ACTIVE", "INCREMENTAL", fresh_time, status="RUNNING")

        # Run cleaner with 4 hours timeout
        database.set_active_run_id("RUN-ACTIVE")
        interrupted = database.reset_stale_pipeline_runs(timeout_hours=4)
        self.assertEqual(interrupted, 1)

        with sqlite3.connect(database.get_sqlite_path()) as conn:
            conn.row_factory = sqlite3.Row
            r_old = dict(conn.execute("SELECT * FROM pipeline_runs WHERE run_id = 'RUN-OLD'").fetchone())
            r_act = dict(conn.execute("SELECT * FROM pipeline_runs WHERE run_id = 'RUN-ACTIVE'").fetchone())

        self.assertEqual(r_old["status"], "INTERRUPTED")
        self.assertEqual(r_act["status"], "RUNNING")
        database.set_active_run_id(None)

    def test_06_arithmetic_consistency_in_report(self):
        """Verify that report overall metrics perfectly reconcile: total == succeeded + pending + failed."""
        database.stage_trial("T1", "2026-01-01") # PENDING
        database.stage_trial("T2", "2026-01-01")
        database.mark_success("T2", "2026-01-01") # SUCCESS
        database.stage_trial("T3", "2026-01-01")
        database.mark_processing("T3") # PROCESSING
        database.stage_trial("T4", "2026-01-01")
        database.mark_failure("T4", "e1")
        database.mark_failure("T4", "e2")
        database.mark_failure("T4", "e3") # FAILED

        report = database.generate_pipeline_summary_report()
        metrics = report["overall_metrics"]

        total = metrics["total_trials_tracked"]
        succeeded = metrics["succeeded"]
        pending = metrics["pending"]
        failed = metrics["failed"]

        self.assertEqual(total, 4)
        self.assertEqual(succeeded, 1)
        self.assertEqual(pending, 2) # T1 (pending) + T3 (processing)
        self.assertEqual(failed, 1)
        self.assertEqual(total, succeeded + pending + failed)

    def test_07_date_parsing_resilience(self):
        """Verify CTIS date parsing handles prefixes, ISO dates, slashes, and nulls without throwing."""
        from ctis_etl.main import parse_ctis_date
        self.assertIsNone(parse_ctis_date(None))
        self.assertIsNone(parse_ctis_date(""))
        self.assertIsNone(parse_ctis_date("not-a-date"))

        d1 = parse_ctis_date("07/10/2026")
        self.assertIsNotNone(d1)
        self.assertEqual(d1.year, 2026)
        self.assertEqual(d1.day, 7)

        d2 = parse_ctis_date("HU: 07/10/2026")
        self.assertIsNotNone(d2)
        self.assertEqual(d2.year, 2026)

        d3 = parse_ctis_date("2026-10-07")
        self.assertIsNotNone(d3)
        self.assertEqual(d3.year, 2026)

        d4 = parse_ctis_date("2026-10-07T14:30:00")
        self.assertIsNotNone(d4)
        self.assertEqual(d4.hour, 14)

    def test_08_file_persistence_bronze_and_silver(self):
        """Verify saving trial files correctly creates raw.json and all 6 domain files on disk."""
        from ctis_etl import storage
        sample_path = Path("sample/2026-527084-15-00.json")
        with open(sample_path, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)

        silver_files = parser.parse_trial_dossier(raw_payload)
        test_ct = "TEST-TRIAL-999"

        results = storage.save_trial_files(
            ct_number=test_ct,
            parsed_files=silver_files,
            raw_payload=raw_payload,
            backend="local",
        )

        for key, success in results.items():
            self.assertTrue(success, f"Failed writing entity {key}")

        bronze_file = config.BRONZE_DIR / test_ct / "raw.json"
        self.assertTrue(bronze_file.exists())
        with open(bronze_file, "r", encoding="utf-8") as bf:
            bronze_content = json.load(bf)
            self.assertEqual(bronze_content["ctNumber"], "2026-527084-15-00")

        expected_silver = [
            "meta_data.json",
            "summary.json",
            "full_trial_information.json",
            "trial_documents.json",
            "trial_results.json",
            "locations_and_contact_points.json",
        ]
        for sfile in expected_silver:
            spath = config.SILVER_DIR / test_ct / sfile
            self.assertTrue(spath.exists(), f"Missing Silver entity {sfile}")
            with open(spath, "r", encoding="utf-8") as sf:
                scontent = json.load(sf)
                self.assertIsNotNone(scontent)

        # Cleanup test trial directory
        import shutil
        if (config.BRONZE_DIR / test_ct).exists():
            shutil.rmtree(config.BRONZE_DIR / test_ct, ignore_errors=True)
        if (config.SILVER_DIR / test_ct).exists():
            shutil.rmtree(config.SILVER_DIR / test_ct, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
