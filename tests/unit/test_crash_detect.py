"""Unit tests for server/crash_detect.py — crash and soft-failure detection."""

import pytest

from server.crash_detect import detect_crash, detect_soft_failure, is_benign_line


class TestDetectCrash:
    @pytest.mark.unit
    def test_traceback(self):
        content = "some output\nTraceback (most recent call last):\n  File ..."
        assert detect_crash(content) is not None

    @pytest.mark.unit
    def test_value_error(self):
        content = "ValueError: No files found with the given pattern."
        assert detect_crash(content) is not None

    @pytest.mark.unit
    def test_cuda_oom(self):
        content = "RuntimeError: CUDA out of memory"
        assert detect_crash(content) is not None

    @pytest.mark.unit
    def test_srun_error(self):
        assert detect_crash("srun: error: node42: task 0: Killed") is not None

    @pytest.mark.unit
    def test_clean_log(self):
        content = "Epoch 1/10: 100%|██████████| 1000/1000\nTraining complete."
        assert detect_crash(content) is None

    @pytest.mark.unit
    def test_empty_and_none(self):
        assert detect_crash("") is None
        assert detect_crash(None) is None

    @pytest.mark.unit
    def test_false_positive_filtered(self):
        content = "Sandbox state restoration failed — retrying\nTraining complete."
        assert detect_crash(content) is None


class TestDetectSoftFailure:
    @pytest.mark.unit
    def test_no_data_to_process(self):
        content = (
            "Waiting for the server to start...\n"
            "No data to process, exiting.\n"
            "ValueError: No files found\n"
        )
        result = detect_soft_failure(content)
        assert result is not None
        assert "No data to process" in result

    @pytest.mark.unit
    def test_exists_skipping(self):
        content = "File `/data/output.jsonl` exists, skipping generation"
        result = detect_soft_failure(content)
        assert result is not None
        assert "skipping" in result.lower()

    @pytest.mark.unit
    def test_nothing_to_evaluate(self):
        result = detect_soft_failure("nothing to evaluate for this chunk")
        assert result is not None

    @pytest.mark.unit
    def test_zero_samples(self):
        result = detect_soft_failure("0 samples to process in chunk 5")
        assert result is not None

    @pytest.mark.unit
    def test_all_already_completed(self):
        result = detect_soft_failure("all 500 examples already completed")
        assert result is not None

    @pytest.mark.unit
    def test_genuine_failure_not_soft(self):
        content = (
            "Loading model...\n"
            "RuntimeError: CUDA out of memory\n"
        )
        assert detect_soft_failure(content) is None

    @pytest.mark.unit
    def test_empty_and_none(self):
        assert detect_soft_failure("") is None
        assert detect_soft_failure(None) is None

    @pytest.mark.unit
    def test_clean_log_not_soft(self):
        content = "Epoch 1/10 complete. Loss: 0.42"
        assert detect_soft_failure(content) is None


class TestCrashAndSoftFailInteraction:
    """The key semantic: when both crash and soft-fail are detected,
    soft-fail wins (the crash is collateral from the skip)."""

    @pytest.mark.unit
    def test_nemo_retry_pattern(self):
        """Real-world NeMo-Skills retry log: generation skipped,
        eval crashes on missing chunk files."""
        content = (
            "Waiting for the server to start at localhost:5000\n"
            "Successfully connected to server.\n"
            "File `/data/output.jsonl` exists, skipping generation\n"
            "No data to process, exiting.\n"
            "Error executing job with overrides: ['++input_files=...']\n"
            "Traceback (most recent call last):\n"
            "  File \"/nemo_run/code/evaluate_results.py\", line 104\n"
            "ValueError: No files found with the given pattern.\n"
        )
        crash = detect_crash(content)
        soft = detect_soft_failure(content)
        assert crash is not None, "crash should be detected"
        assert soft is not None, "soft-fail should also be detected"

    @pytest.mark.unit
    def test_genuine_crash_no_soft(self):
        """Real crash without skip indicators — should NOT be soft."""
        content = (
            "Loading model weights...\n"
            "Processing chunk 5 of 50\n"
            "Traceback (most recent call last):\n"
            "  File \"generate.py\", line 42\n"
            "RuntimeError: CUDA out of memory\n"
        )
        crash = detect_crash(content)
        soft = detect_soft_failure(content)
        assert crash is not None
        assert soft is None


class TestIsBenignLine:
    @pytest.mark.unit
    def test_sandbox_restoration(self):
        assert is_benign_line("sandbox state restoration failed — retrying")

    @pytest.mark.unit
    def test_sandbox_communication(self):
        assert is_benign_line("sandbox communication error on port 8080")

    @pytest.mark.unit
    def test_normal_line(self):
        assert not is_benign_line("training step 100 complete")
