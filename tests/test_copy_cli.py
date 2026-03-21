#!/usr/bin/env python3
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COPY_BIN = ROOT / "copy"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text):
    return ANSI_RE.sub("", text)


def run_copy(args, cwd=None, confirm=False):
    proc = subprocess.run(
        [str(COPY_BIN), *args],
        cwd=str(cwd) if cwd else None,
        input=("y\n" if confirm else "n\n"),
        text=True,
        capture_output=True,
    )
    combined = f"{proc.stdout}\n{proc.stderr}".strip()
    return proc.returncode, strip_ansi(combined), proc.stdout


def write_file(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class CopyCliIntegrationTests(unittest.TestCase):
    def test_help_includes_expected_aliases(self):
        rc, out, _ = run_copy(["--help"])
        self.assertEqual(rc, 0)
        self.assertIn("-m, --move", out)
        self.assertIn("-s, --sudo", out)
        self.assertIn("-c, -C, --contents-only", out)
        self.assertIn("-v, --verbose, --showall", out)

    def test_move_same_slot_to_parent_is_noop_by_default(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "Telegram Backup" / "poo"
            write_file(base / "poo" / "inner.txt", "x\n")
            rc, out, _ = run_copy(["--move", "poo", ".."], cwd=base)
            self.assertEqual(rc, 0)
            self.assertIn("No changes detected; nothing to move.", out)

    def test_move_same_slot_to_parent_with_contents_only_plans_merge(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "Telegram Backup" / "poo"
            write_file(base / "poo" / "sdf", "x\n")
            rc, out, _ = run_copy(["--move", "poo", "..", "-c", "-v"], cwd=base)
            self.assertEqual(rc, 0)
            self.assertNotIn("No changes detected; nothing to move.", out)
            self.assertIn("poo/ (removed)", out)

    def test_move_same_slot_to_parent_with_contents_only_and_overwrite_is_not_noop(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "Telegram Backup" / "poo"
            write_file(base / "poo" / "sdf", "x\n")
            rc, out, _ = run_copy(["--move", "poo", "..", "-c", "-o", "-v"], cwd=base)
            self.assertEqual(rc, 0)
            self.assertNotIn("No changes detected; nothing to move.", out)
            self.assertIn("poo/ (removed)", out)

    def test_copy_directory_default_nests_under_destination(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "file.txt", "payload\n")
            dst.mkdir(parents=True)
            rc, out, _ = run_copy([str(src), str(dst)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertTrue((dst / "A" / "file.txt").exists())

    def test_copy_directory_contents_only_merges_into_destination(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "file.txt", "payload\n")
            dst.mkdir(parents=True)
            rc, out, _ = run_copy([str(src), str(dst), "-c"], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertTrue((dst / "file.txt").exists())
            self.assertFalse((dst / "A").exists())

    def test_move_directory_contents_only_merges_and_removes_nested_source(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "Telegram Backup" / "poo"
            parent = base.parent
            write_file(base / "poo" / "sdf", "hello\n")
            write_file(base / "keep.txt", "keep\n")
            rc, out, _ = run_copy(["--move", "poo", "..", "-c"], cwd=base, confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertTrue((parent / "sdf").exists())
            self.assertFalse((base / "poo").exists())
            self.assertTrue((base / "keep.txt").exists())

    def test_overwrite_nested_target_replaces_existing_directory(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "poo"
            dst = Path(td) / "dst" / "root" / "poo"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")
            rc, out, _ = run_copy(["--move", "-o", str(src), str(dst.parent)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertTrue((dst / "new.txt").exists())
            self.assertFalse((dst / "old.txt").exists())

    def test_overwrite_explicit_destination_with_contents_only_replaces_path(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst" / "B"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")
            rc, out, _ = run_copy(["--move", "-o", "-c", str(src), str(dst)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertTrue((dst / "new.txt").exists())
            self.assertFalse((dst / "old.txt").exists())

    def test_overwrite_preview_shows_old_new_pair(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "poo"
            dst = Path(td) / "dst" / "root" / "poo"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")
            rc, out, _ = run_copy(["--move", "-o", str(src), str(dst.parent), "-v"])
            self.assertEqual(rc, 0)
            self.assertIn("poo/ (old)", out)
            self.assertIn("poo/ (new)", out)

    def test_source_star_behaves_like_contents_only(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src"
            dst = Path(td) / "dst"
            write_file(src / "sub" / "x.txt", "x\n")
            dst.mkdir(parents=True, exist_ok=True)
            rc_star, out_star, _ = run_copy(["--move", f"{src}/*", str(dst)])
            rc_c, out_c, _ = run_copy(["--move", f"{src}/", str(dst), "-c"])
            self.assertEqual(rc_star, 0)
            self.assertEqual(rc_c, 0)
            self.assertIn("Planned transfer bytes:", out_star)
            self.assertIn("Planned transfer bytes:", out_c)
            self.assertIn("Merge", out_star)
            self.assertIn("Merge", out_c)

    def test_showall_abbreviation_format_present(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "change"
            dst = Path(td) / "dst"
            for i in range(8):
                write_file(src / f"n{i}.txt", f"{i}\n")
                write_file(dst / f"u{i}.txt", "u\n")
            rc, out, _ = run_copy(["-v", "-c", str(src), str(dst)])
            self.assertEqual(rc, 0)
            self.assertRegex(
                out,
                r"\.\.\. and (?:\d+ more (?:new|modified|unchanged|removed))(?: \d+ more (?:new|modified|unchanged|removed))*",
            )

    def test_contents_only_uppercase_alias(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "f.txt", "x\n")
            dst.mkdir(parents=True)
            rc, out, _ = run_copy(["--move", "-C", str(src), str(dst)])
            self.assertEqual(rc, 0)
            self.assertIn("Planned transfer bytes:", out)

    def test_verbose_alias_does_not_crash(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "f.txt", "x\n")
            dst.mkdir(parents=True)
            rc, out, _ = run_copy(["--move", "-v", str(src), str(dst)])
            self.assertEqual(rc, 0, out)
            self.assertIn("Planned transfer bytes:", out)

    def test_regular_files_summary_includes_removed_count(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "f.txt", "x\n")
            dst.mkdir(parents=True, exist_ok=True)

            rc_copy, out_copy, _ = run_copy([str(src), str(dst)])
            self.assertEqual(rc_copy, 0, out_copy)
            self.assertIn("Regular files:", out_copy)
            self.assertIn("removed_from_source=0", out_copy)
            self.assertIn("removed=0", out_copy)

            rc_move, out_move, _ = run_copy(["--move", str(src), str(dst)])
            self.assertEqual(rc_move, 0, out_move)
            self.assertIn("Regular files:", out_move)
            self.assertRegex(out_move, r"removed_from_source=\d+")
            self.assertRegex(out_move, r"removed=\d+")


if __name__ == "__main__":
    unittest.main(verbosity=2)
