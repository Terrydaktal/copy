#!/usr/bin/env python3
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COPY_BIN = ROOT / "copy"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
BACKUP_SUFFIX_RE = re.compile(r"^\d{8}-\d{6}(?:\.\d+)?$")


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


def find_backups(parent, base_name):
    found = []
    if not parent.exists():
        return found
    prefix = f"{base_name}."
    for child in parent.iterdir():
        name = child.name
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):]
        if BACKUP_SUFFIX_RE.match(suffix):
            found.append(child)
    return sorted(found, key=lambda p: p.name)


class CopyCliIntegrationTests(unittest.TestCase):
    def test_help_includes_expected_aliases(self):
        rc, out, _ = run_copy(["--help"])
        self.assertEqual(rc, 0)
        self.assertIn("-m, --move", out)
        self.assertIn("-s, --sudo", out)
        self.assertIn("-c, --contents-only", out)
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

    def test_dir_rename_preview_does_not_flatten_children_into_parent(self):
        with tempfile.TemporaryDirectory() as td:
            parent = Path(td) / "Telegram Backup"
            src = parent / "g"
            dst = parent / "Sensitive Information 5"
            write_file(src / "css" / "x.css", "x\n")
            write_file(src / "messages.html", "m\n")

            rc, out, _ = run_copy(["--move", str(src), str(dst)])
            self.assertEqual(rc, 0, out)
            self.assertIn(str(parent) + "/", out)
            self.assertIn("Sensitive Information 5/", out)
            self.assertNotIn("\n├── css/", out)
            self.assertNotIn("\n└── css/", out)

    def test_move_same_parent_rename_shows_removed_source(self):
        with tempfile.TemporaryDirectory() as td:
            parent = Path(td) / "Dev"
            src = parent / "f"
            dst = parent / "unearth"
            write_file(src / "a.txt", "a\n")

            rc, out, _ = run_copy(["--move", str(src), str(dst)])
            self.assertEqual(rc, 0, out)
            self.assertIn("f/ (removed)", out)
            self.assertIn("unearth/", out)

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

    def test_non_verbose_top_level_truncates_to_15_with_summary(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            for i in range(20):
                write_file(src / f"f{i:02d}.txt", f"{i}\n")
            dst.mkdir(parents=True, exist_ok=True)

            rc, out, _ = run_copy([str(src), str(dst), "-c"])
            self.assertEqual(rc, 0)
            tree_rows = [line for line in out.splitlines() if line.startswith("├── ") or line.startswith("└── ")]
            self.assertEqual(len(tree_rows), 15, msg=f"expected 15 visible rows, got {len(tree_rows)}\n{out}")
            self.assertRegex(
                out,
                r"\.\.\. and \d+ more new \d+ more modified \d+ more unchanged and \d+ more removed",
            )

    def test_contents_only_uppercase_alias_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "f.txt", "x\n")
            dst.mkdir(parents=True)
            rc, out, _ = run_copy(["--move", "-C", str(src), str(dst)])
            self.assertNotEqual(rc, 0)
            self.assertIn("unrecognized arguments: -C", out)

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

    def test_backup_merge_copy_creates_backup_dir(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst_root = Path(td) / "dst"
            dst = dst_root / "A"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")

            rc, out, _ = run_copy(["-b", str(src), str(dst_root)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("Backup saved as:", out)
            self.assertTrue((dst / "new.txt").exists())
            self.assertTrue((dst / "old.txt").exists())
            backups = find_backups(dst_root, "A")
            self.assertEqual(len(backups), 1, f"unexpected backups: {backups}")
            self.assertTrue((backups[0] / "old.txt").exists())

    def test_backup_merge_move_creates_backup_and_removes_source(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst_root = Path(td) / "dst"
            dst = dst_root / "A"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")

            rc, out, _ = run_copy(["--move", "-b", str(src), str(dst_root)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("Backup saved as:", out)
            self.assertFalse(src.exists())
            self.assertTrue((dst / "new.txt").exists())
            self.assertTrue((dst / "old.txt").exists())
            backups = find_backups(dst_root, "A")
            self.assertEqual(len(backups), 1, f"unexpected backups: {backups}")
            self.assertTrue((backups[0] / "old.txt").exists())

    def test_backup_overwrite_nested_target_move_replaces_and_backs_up_old(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "poo"
            dst_parent = Path(td) / "dst" / "root"
            dst = dst_parent / "poo"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")

            rc, out, _ = run_copy(["--move", "-o", "-b", str(src), str(dst_parent)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("Backup saved as:", out)
            self.assertTrue((dst / "new.txt").exists())
            self.assertFalse((dst / "old.txt").exists())
            backups = find_backups(dst_parent, "poo")
            self.assertEqual(len(backups), 1, f"unexpected backups: {backups}")
            self.assertTrue((backups[0] / "old.txt").exists())

    def test_backup_overwrite_explicit_contents_only_move_replaces_and_backs_up_old(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst_parent = Path(td) / "dst"
            dst = dst_parent / "B"
            write_file(src / "new.txt", "new\n")
            write_file(dst / "old.txt", "old\n")

            rc, out, _ = run_copy(["--move", "-o", "-c", "-b", str(src), str(dst)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("Backup saved as:", out)
            self.assertTrue((dst / "new.txt").exists())
            self.assertFalse((dst / "old.txt").exists())
            backups = find_backups(dst_parent, "B")
            self.assertEqual(len(backups), 1, f"unexpected backups: {backups}")
            self.assertTrue((backups[0] / "old.txt").exists())

    def test_backup_file_conflict_copy_creates_backup_file(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "f.txt"
            dst = Path(td) / "dst" / "f.txt"
            write_file(src, "newer\n")
            write_file(dst, "old\n")

            rc, out, _ = run_copy(["-b", str(src), str(dst)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("Backup saved as:", out)
            self.assertEqual(dst.read_text(encoding="utf-8"), "newer\n")
            backups = find_backups(dst.parent, "f.txt")
            self.assertEqual(len(backups), 1, f"unexpected backups: {backups}")
            self.assertTrue(backups[0].is_file())
            self.assertEqual(backups[0].read_text(encoding="utf-8"), "old\n")

    def test_backup_no_conflict_does_not_create_backup(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src" / "A"
            dst = Path(td) / "dst"
            write_file(src / "n.txt", "n\n")
            dst.mkdir(parents=True, exist_ok=True)

            rc, out, _ = run_copy(["-b", str(src), str(dst)], confirm=True)
            self.assertEqual(rc, 0, out)
            self.assertNotIn("Backup complete.", out)
            self.assertTrue((dst / "A" / "n.txt").exists())
            backups = find_backups(dst, "A")
            self.assertEqual(len(backups), 0, f"unexpected backups: {backups}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
