# Copyright 2024, RapidCopy Contributors, All rights reserved.

"""
Unit tests for the ValidationDispatch settle delay.

The settle delay is a configurable pause inserted before local chunk hashing
starts for post-download (non-inline) validation. It eliminates false-positive
"all chunks corrupt" reports caused by the OS page cache not having flushed all
dirty pages written by LFTP's parallel pget before the validator begins hashing.
"""

import os
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

from common import ValidationConfig
from controller.validate.validation_process import ValidationDispatch, ValidationCommand
from model import ModelFile


def _make_file(name: str = "test.mkv", size: int = 1024 * 1024) -> ModelFile:
    """Helper: create a minimal ModelFile for use in ValidationCommand."""
    f = ModelFile(name, False)
    f.remote_size = size
    return f


class TestValidationSettleDelay(unittest.TestCase):
    """Tests that the settle delay is applied correctly inside _start_validation."""

    def _make_dispatch(self, settle_delay_secs: float) -> ValidationDispatch:
        config = ValidationConfig(
            settle_delay_secs=settle_delay_secs,
            default_chunk_size=1024 * 1024,  # 1 MB chunks
            min_chunk_size=1024 * 1024,
            max_chunk_size=100 * 1024 * 1024,
        )
        sshcp = MagicMock()
        dispatch = ValidationDispatch(
            config=config,
            sshcp=sshcp,
            local_base_path="/local",
            remote_base_path="/remote",
        )
        # Stub out checksum generators so validation short-circuits without real I/O
        dispatch._local_checksum = MagicMock()
        dispatch._remote_checksum = MagicMock()
        dispatch._local_checksum.compute_chunk_checksum.return_value = "abc123"
        dispatch._local_checksum.compute_file_checksum.return_value = "abc123"
        dispatch._remote_checksum.compute_chunk_checksums.return_value = ["abc123"]
        dispatch._remote_checksum.compute_file_checksum.return_value = "abc123"
        return dispatch

    def _make_command(self, inline: bool = False) -> ValidationCommand:
        f = _make_file()
        return ValidationCommand(
            file=f,
            local_path="test.mkv",
            remote_path="test.mkv",
            file_size=1024 * 1024,
            inline=inline,
        )

    @patch("controller.validate.validation_process.time.sleep")
    def test_settle_delay_called_for_post_download(self, mock_sleep):
        """time.sleep(settle_delay_secs) is called once for non-inline (post-download) validation."""
        delay = 5.0
        dispatch = self._make_dispatch(settle_delay_secs=delay)
        command = self._make_command(inline=False)
        dispatch._start_validation(command)
        mock_sleep.assert_called_once_with(delay)

    @patch("controller.validate.validation_process.time.sleep")
    def test_settle_delay_not_called_for_inline(self, mock_sleep):
        """time.sleep is NOT called when inline=True (chunks validated during download)."""
        dispatch = self._make_dispatch(settle_delay_secs=5.0)
        command = self._make_command(inline=True)
        dispatch._start_validation(command)
        mock_sleep.assert_not_called()

    @patch("controller.validate.validation_process.time.sleep")
    def test_settle_delay_zero_skips_sleep(self, mock_sleep):
        """When settle_delay_secs=0, time.sleep is not called even for post-download mode."""
        dispatch = self._make_dispatch(settle_delay_secs=0.0)
        command = self._make_command(inline=False)
        dispatch._start_validation(command)
        mock_sleep.assert_not_called()

    @patch("controller.validate.validation_process.time.sleep")
    def test_settle_delay_custom_value(self, mock_sleep):
        """Custom settle_delay_secs value is passed verbatim to time.sleep."""
        delay = 2.5
        dispatch = self._make_dispatch(settle_delay_secs=delay)
        command = self._make_command(inline=False)
        dispatch._start_validation(command)
        mock_sleep.assert_called_once_with(delay)


class TestInlineStallWatchdog(unittest.TestCase):
    """Tests the watchdog that abandons an inline validation whose download stalls."""

    def _make_dispatch(self) -> ValidationDispatch:
        config = ValidationConfig(
            settle_delay_secs=0.0,
            default_chunk_size=1024 * 1024,
            min_chunk_size=1024 * 1024,
            max_chunk_size=100 * 1024 * 1024,
        )
        return ValidationDispatch(
            config=config, sshcp=MagicMock(), local_base_path="/local", remote_base_path="/remote"
        )

    @staticmethod
    def _stub_chunk_manager(dispatch, pending_chunk_end_offset):
        info = MagicMock()
        info.full_file_checksum = None
        info.local_full_checksum = None
        chunk = MagicMock()
        chunk.end_offset = pending_chunk_end_offset
        cm = MagicMock()
        cm.get_validation_info.return_value = info
        cm.get_pending_chunks.return_value = [chunk]
        dispatch._chunk_manager = cm
        return cm

    def test_stalled_inline_validation_is_abandoned(self):
        """Active inline file with no download progress past the timeout is dropped."""
        d = self._make_dispatch()
        path = "/local/test.mkv"
        d._active_file = path
        d._active_inline = True
        d._inline_local_sizes[path] = 0
        d._active_last_size = 0
        d._active_last_progress_at = datetime.now() - timedelta(
            seconds=ValidationDispatch._INLINE_STALL_TIMEOUT_SECS + 1
        )
        cm = self._stub_chunk_manager(d, pending_chunk_end_offset=1024 * 1024)

        result = d._continue_validation()

        self.assertIsNone(result)
        self.assertIsNone(d._active_file)  # abandoned so the queue can proceed
        cm.remove_file.assert_called_once_with(path)

    def test_download_progress_resets_watchdog(self):
        """Bytes arriving since the last check reset the watchdog; file is NOT abandoned."""
        d = self._make_dispatch()
        path = "/local/test.mkv"
        d._active_file = path
        d._active_inline = True
        d._inline_local_sizes[path] = 5 * 1024 * 1024  # progress since last check
        d._active_last_size = 0
        stale = datetime.now() - timedelta(seconds=ValidationDispatch._INLINE_STALL_TIMEOUT_SECS + 1)
        d._active_last_progress_at = stale
        cm = self._stub_chunk_manager(d, pending_chunk_end_offset=100 * 1024 * 1024)

        result = d._continue_validation()

        self.assertIsNone(result)
        self.assertEqual(d._active_file, path)  # still active
        self.assertEqual(d._active_last_size, 5 * 1024 * 1024)
        self.assertGreater(d._active_last_progress_at, stale)  # watchdog reset
        cm.remove_file.assert_not_called()
