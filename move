#!/usr/bin/env python3
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time


class Colors:
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    DIM = "\033[90m"
    ENDC = "\033[0m"


def log(msg, level="INFO"):
    if level == "ERROR":
        print(f"{Colors.FAIL}ERROR: {msg}{Colors.ENDC}", file=sys.stderr)
    elif level == "WARN":
        print(f"{Colors.WARNING}WARNING: {msg}{Colors.ENDC}", file=sys.stderr)
    else:
        print(f"{Colors.OKBLUE}move: {msg}{Colors.ENDC}")


def _fmt_hms(total_seconds):
    s = int(max(total_seconds, 0))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _format_bytes_binary(byte_value, decimals=2):
    raw = str(byte_value or "").strip()
    if not raw or raw == "-":
        return ""
    try:
        n = int(raw, 10)
    except (TypeError, ValueError):
        return raw
    if n < 0:
        return ""
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]
    idx = 0
    value = float(n)
    while idx < len(units) - 1 and value >= 1024.0:
        value /= 1024.0
        idx += 1
    while idx < len(units) - 1 and round(value, decimals) >= 1024.0:
        value /= 1024.0
        idx += 1
    if units[idx] == "B":
        return f"{int(value)} B"
    return f"{value:.{decimals}f} {units[idx]}"


def run_command(command, check=True, sudo=False):
    if sudo:
        command = ["sudo"] + command
    try:
        return subprocess.run(
            command,
            text=True,
            check=check,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        if check:
            log(f"Command failed: {' '.join(command)}", "ERROR")
            if e.stderr:
                log(e.stderr.strip(), "ERROR")
            raise
        return e


def _to_real_path(value):
    expanded = os.path.expanduser(value)
    if os.path.isabs(expanded):
        return os.path.realpath(expanded)
    return os.path.realpath(os.path.abspath(expanded))


def _resolve_source(value):
    p = _to_real_path(value)
    if not os.path.exists(p):
        log(f"Source path does not exist: {value}", "ERROR")
        return None, None
    if os.path.isdir(p):
        return p, "dir"
    if os.path.isfile(p):
        return p, "file"
    log(f"Source path must be a file or directory: {value}", "ERROR")
    return None, None


def _resolve_destination_for_file(value):
    dst_real = _to_real_path(value)
    if os.path.exists(dst_real):
        if os.path.isdir(dst_real):
            return dst_real, "dir"
        return dst_real, "file"
    parent = os.path.dirname(dst_real) or "."
    if not os.path.isdir(parent):
        log(f"Destination parent directory does not exist: {parent}", "ERROR")
        return None, None
    return dst_real, "file"


def _resolve_destination_for_dir(value, allow_existing_file=False):
    p = _to_real_path(value)
    if os.path.exists(p):
        if not os.path.isdir(p):
            if allow_existing_file:
                return p, "file_existing_for_dir"
            log(f"Destination path must be a directory (or a new directory path): {value}", "ERROR")
            return None, None
        return p, "dir_existing"
    parent = os.path.dirname(p) or "."
    if not os.path.isdir(parent):
        log(f"Destination parent directory does not exist: {parent}", "ERROR")
        return None, None
    return p, "dir_new"


def _parse_stat_int(label, text):
    m = re.search(rf"^\s*{re.escape(label)}:\s*([0-9,]+)\b", text, re.MULTILINE)
    if not m:
        return 0
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return 0


def _node_has_explicit_changes(node):
    if (
        node.get("explicit_new_dirs")
        or node.get("explicit_new_files")
        or node.get("explicit_mod_files")
    ):
        return True
    for child in node.get("dirs", {}).values():
        if _node_has_explicit_changes(child):
            return True
    return False


def _print_preview_tree(preview_items, base_prefix=""):
    tree = {
        "dirs": {},
        "files": set(),
        "explicit_new_dirs": set(),
        "explicit_new_files": set(),
        "explicit_mod_files": set(),
    }
    for change_kind, raw in preview_items:
        p = (raw or "").strip()
        if not p:
            continue
        is_dir = p.endswith("/")
        p = p.rstrip("/")
        if not p:
            continue
        parts = p.split("/")
        node = tree
        for dname in parts[:-1]:
            node = node["dirs"].setdefault(
                dname,
                {
                    "dirs": {},
                    "files": set(),
                    "explicit_new_dirs": set(),
                    "explicit_new_files": set(),
                    "explicit_mod_files": set(),
                },
            )
        leaf = parts[-1]
        if is_dir:
            node["dirs"].setdefault(
                leaf,
                {
                    "dirs": {},
                    "files": set(),
                    "explicit_new_dirs": set(),
                    "explicit_new_files": set(),
                    "explicit_mod_files": set(),
                },
            )
            if change_kind == "new_dir":
                node["explicit_new_dirs"].add(leaf)
        else:
            node["files"].add(leaf)
            if change_kind == "mod_file":
                node["explicit_mod_files"].add(leaf)
            else:
                node["explicit_new_files"].add(leaf)

    def walk(node, prefix):
        entries = []
        for dname in sorted(node["dirs"].keys()):
            entries.append(("dir", dname))
        for fname in sorted(node["files"]):
            entries.append(("file", fname))
        for idx, (kind, name) in enumerate(entries):
            last = idx == len(entries) - 1
            branch = "└── " if last else "├── "
            suffix = "/" if kind == "dir" else ""
            if kind == "dir":
                child_node = node["dirs"][name]
                is_new_dir = name in node.get("explicit_new_dirs", set())
                is_modified_dir = (not is_new_dir) and _node_has_explicit_changes(child_node)
                if is_new_dir:
                    print(f"{prefix}{branch}{Colors.OKGREEN}{name}{suffix}{Colors.ENDC}")
                elif is_modified_dir:
                    print(f"{prefix}{branch}{Colors.WARNING}{name}{suffix}{Colors.ENDC}")
                else:
                    print(f"{prefix}{branch}{name}{suffix}")
                child_prefix = prefix + ("    " if last else "│   ")
                walk(child_node, child_prefix)
            else:
                if name in node.get("explicit_new_files", set()):
                    print(f"{prefix}{branch}{Colors.OKGREEN}{name}{Colors.ENDC}")
                elif name in node.get("explicit_mod_files", set()):
                    print(f"{prefix}{branch}{Colors.WARNING}{name}{Colors.ENDC}")
                else:
                    print(f"{prefix}{branch}{name}")

    walk(tree, base_prefix)


def _parse_progress2_bytes(line):
    m = re.match(r"^\s*([0-9][0-9,]*(?:\.[0-9]+)?)([kKmMgGtTpPeE]?)\s+[0-9]{1,3}%", line or "")
    if not m:
        return None
    num_txt = (m.group(1) or "").replace(",", "")
    unit = (m.group(2) or "").upper()
    try:
        val = float(num_txt)
    except Exception:
        return None
    mult = 1
    if unit == "K":
        mult = 1024
    elif unit == "M":
        mult = 1024 ** 2
    elif unit == "G":
        mult = 1024 ** 3
    elif unit == "T":
        mult = 1024 ** 4
    elif unit == "P":
        mult = 1024 ** 5
    elif unit == "E":
        mult = 1024 ** 6
    try:
        return int(val * mult)
    except Exception:
        return None


def _fmt_speed_bps(bps):
    try:
        b = max(0, int(float(bps)))
    except Exception:
        b = 0
    return _format_bytes_binary(str(b), decimals=2)


def _run_move(src_path, dst_path, planned_bytes, use_sudo):
    cmd = [
        "rsync",
        "-aH",
        "--remove-source-files",
        "--info=progress2,stats2,name0",
        src_path,
        dst_path,
    ]
    if use_sudo:
        cmd = ["sudo"] + cmd
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    done_bytes = 0
    progress_stop = threading.Event()
    progress_lock = threading.Lock()

    def progress_reporter():
        last_report_bytes = 0
        last_report_ts = time.time()
        while not progress_stop.wait(0.5):
            with progress_lock:
                cur_bytes = int(done_bytes)
            if cur_bytes <= last_report_bytes:
                continue
            now = time.time()
            dt = max(now - last_report_ts, 1e-6)
            speed = (cur_bytes - last_report_bytes) / dt
            if planned_bytes > 0:
                pct = min(100.0, (cur_bytes * 100.0) / planned_bytes)
                cur_disp = _format_bytes_binary(str(cur_bytes), decimals=2)
                total_disp = _format_bytes_binary(str(planned_bytes), decimals=2)
                print(f"Progress: {pct:6.2f}% ({cur_disp} / {total_disp}) | Speed: {_fmt_speed_bps(speed)}/s", flush=True)
            else:
                cur_disp = _format_bytes_binary(str(cur_bytes), decimals=2)
                print(f"Progress: ---% ({cur_disp}) | Speed: {_fmt_speed_bps(speed)}/s", flush=True)
            last_report_bytes = cur_bytes
            last_report_ts = now

    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    progress_thread.start()

    if proc.stdout is not None:
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            p2_bytes = _parse_progress2_bytes(line)
            if p2_bytes is not None:
                with progress_lock:
                    if p2_bytes > done_bytes:
                        done_bytes = p2_bytes
            elif line.strip():
                print(line)

    rc_move = proc.wait()
    progress_stop.set()
    progress_thread.join(timeout=1.0)

    with progress_lock:
        final_done = int(done_bytes)
    if planned_bytes > 0 and rc_move in (0, 24):
        final_done = max(final_done, planned_bytes)
    if planned_bytes > 0:
        pct = min(100.0, (final_done * 100.0) / planned_bytes)
        cur_disp = _format_bytes_binary(str(final_done), decimals=2)
        total_disp = _format_bytes_binary(str(planned_bytes), decimals=2)
        print(f"Progress: {pct:6.2f}% ({cur_disp} / {total_disp}) | Speed: 0 B/s", flush=True)
    else:
        cur_disp = _format_bytes_binary(str(final_done), decimals=2)
        print(f"Progress: ---% ({cur_disp}) | Speed: 0 B/s", flush=True)
    return rc_move


def _cleanup_source_dirs(src_root, remove_root, use_sudo):
    if not src_root or not os.path.isdir(src_root):
        return
    cmd = ["find", src_root]
    if not remove_root:
        cmd += ["-mindepth", "1"]
    cmd += ["-depth", "-type", "d", "-empty", "-delete"]
    res = run_command(cmd, check=False, sudo=use_sudo)
    rc = getattr(res, "returncode", 0)
    if rc != 0:
        log(f"Source cleanup failed: find exited with status {rc}.", "WARN")


def _fmt_mode_word(label, active):
    if active:
        return f"{Colors.OKGREEN}{label}{Colors.ENDC}"
    return f"{Colors.DIM}{label}{Colors.ENDC}"


def _remove_path_recursive(path, use_sudo):
    if not path:
        return True
    res = run_command(["rm", "-rf", "--", path], check=False, sudo=use_sudo)
    rc = getattr(res, "returncode", 0)
    if rc != 0:
        log(f"Failed to remove existing path: {path}", "ERROR")
        return False
    return True


def _backup_path_with_timestamp(path, use_sudo):
    if not path:
        return None
    base = _backup_base_path(path)
    if not base:
        return None
    return _backup_path_with_base(path, use_sudo, base)


def _backup_base_path(path):
    if not path:
        return None
    src = path.rstrip("/") or path
    if src == "/":
        return None
    parent = os.path.dirname(src) or "."
    name = os.path.basename(src)
    if not name:
        return None
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    return os.path.join(parent, f"{name}.{stamp}")


def _next_backup_candidate_from_base(base):
    if not base:
        return None
    for idx in range(1000):
        candidate = base if idx == 0 else f"{base}.{idx}"
        if os.path.exists(candidate):
            continue
        return candidate
    return None


def _plan_backup_path(path):
    base = _backup_base_path(path)
    return _next_backup_candidate_from_base(base)


def _backup_path_with_base(path, use_sudo, base):
    if not path:
        return None
    src = path.rstrip("/") or path
    if src == "/":
        log("Refusing to backup root path.", "ERROR")
        return None
    if not base:
        log(f"Failed to derive backup name for path: {src}", "ERROR")
        return None
    for idx in range(1000):
        candidate = base if idx == 0 else f"{base}.{idx}"
        if os.path.exists(candidate):
            continue
        res = run_command(["mv", "--", src, candidate], check=False, sudo=use_sudo)
        rc = getattr(res, "returncode", 0)
        if rc == 0:
            return candidate
        err = (getattr(res, "stderr", "") or "").strip()
        if "exists" in err.lower():
            continue
        log(f"Failed to backup existing path: {src}", "ERROR")
        if err:
            log(err, "ERROR")
        return None
    log(f"Failed to create unique backup name for: {src}", "ERROR")
    return None


def _copy_path_to_backup(path, backup_path, use_sudo):
    if not path or not backup_path:
        return None
    res = run_command(["cp", "-a", "--", path, backup_path], check=False, sudo=use_sudo)
    rc = getattr(res, "returncode", 0)
    if rc != 0:
        err = (getattr(res, "stderr", "") or "").strip()
        log(f"Failed to create backup copy: {path}", "ERROR")
        if err:
            log(err, "ERROR")
        return None
    return backup_path


def main():
    parser = argparse.ArgumentParser(
        prog="move",
        description="Standalone rsync move with preview/progress (path-only).",
    )
    parser.add_argument("source", help="Source path (file or directory)")
    parser.add_argument("destination", help="Destination path (directory, or file path when source is a file)")
    parser.add_argument("extra", nargs="*", help=argparse.SUPPRESS)
    parser.add_argument("--sudo", action="store_true", help="Run rsync with sudo")
    parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="Force overwrite (replace) instead of merge when destination target already exists.",
    )
    parser.add_argument(
        "-f",
        "-r",
        "--force-rename",
        action="store_true",
        help="Force rename and merge when destination rename target already exists (rather than nesting).",
    )
    parser.add_argument(
        "-b",
        "--backup",
        dest="backup",
        action="store_true",
        help="Create a timestamped backup when destination data will be merged or overwritten.",
    )
    args = parser.parse_args()

    if args.extra:
        log("Unexpected extra path arguments. If using '*', quote it (e.g. 'src/*').", "ERROR")
        return 2

    source_input = args.source
    source = source_input
    destination = args.destination
    use_sudo = bool(args.sudo)
    backup_requested = bool(args.backup)
    overwrite = bool(args.overwrite)
    force = bool(args.force_rename)
    source_glob_contents = False

    if source.endswith("/*"):
        source = source[:-1]
        source_glob_contents = True

    src_mnt, src_obj_kind = _resolve_source(source)
    if not src_mnt:
        return 1

    if src_obj_kind == "file":
        dst_mnt, dst_obj_kind = _resolve_destination_for_file(destination)
    else:
        dst_mnt, dst_obj_kind = _resolve_destination_for_dir(destination, allow_existing_file=overwrite)
    if not dst_mnt:
        return 1

    src_user_had_trailing = source.endswith("/") or source_glob_contents
    dest_tail_raw = os.path.basename((destination or "").rstrip("/"))
    destination_is_dir_ref = destination.endswith("/") or dest_tail_raw in ("", ".", "..")

    rename_dir_to_new_path = False
    merge_child_into_parent = False
    source_already_in_destination = False
    overwrite_parent_from_child = False
    force_parent_from_child = False
    if src_obj_kind == "dir" and dst_obj_kind in ("dir", "dir_existing"):
        dst_slot_for_src = os.path.realpath(os.path.join(dst_mnt, os.path.basename(src_mnt.rstrip("/"))))
        if dst_slot_for_src == src_mnt:
            src_base_for_slot = os.path.basename(src_mnt.rstrip("/"))
            dst_base_for_slot = os.path.basename(dst_mnt.rstrip("/"))
            if src_base_for_slot == dst_base_for_slot:
                merge_child_into_parent = True
            else:
                source_already_in_destination = True
                if overwrite and not src_user_had_trailing and not destination_is_dir_ref:
                    overwrite_parent_from_child = True
                    source_already_in_destination = False
                elif force and not src_user_had_trailing and not destination_is_dir_ref and not overwrite:
                    force_parent_from_child = True
                    source_already_in_destination = False

    rename_style_existing_dir_target = bool(
        src_obj_kind == "dir"
        and dst_obj_kind == "dir_existing"
        and not src_user_had_trailing
        and not destination_is_dir_ref
        and os.path.basename(src_mnt.rstrip("/")) != os.path.basename(dst_mnt.rstrip("/"))
    )

    target_dir_for_name = None
    target_name_for_conflict = None
    overwrite_rename_dir_target = False
    overwrite_replace_file_target = False
    if overwrite_parent_from_child:
        overwrite_rename_dir_target = True
    elif (
        overwrite
        and rename_style_existing_dir_target
    ):
        overwrite_rename_dir_target = True

    if (
        overwrite
        and src_obj_kind == "dir"
        and dst_obj_kind == "file_existing_for_dir"
        and not src_user_had_trailing
    ):
        overwrite_replace_file_target = True

    force_merge_dir_target = bool(
        force
        and rename_style_existing_dir_target
        and not source_already_in_destination
        and not overwrite_parent_from_child
    )

    overwrite_merge_child_parent = bool(overwrite and merge_child_into_parent and src_obj_kind == "dir")

    if dst_obj_kind in ("dir", "dir_existing"):
        if overwrite_rename_dir_target:
            target_dir_for_name = os.path.dirname(dst_mnt.rstrip("/"))
            target_name_for_conflict = os.path.basename(dst_mnt.rstrip("/"))
        elif force_merge_dir_target:
            target_dir_for_name = os.path.dirname(dst_mnt.rstrip("/"))
            target_name_for_conflict = os.path.basename(dst_mnt.rstrip("/"))
        elif merge_child_into_parent and src_obj_kind == "dir":
            target_dir_for_name = os.path.dirname(dst_mnt.rstrip("/"))
            target_name_for_conflict = os.path.basename(dst_mnt.rstrip("/"))
        else:
            target_dir_for_name = dst_mnt
            if src_obj_kind == "dir":
                target_name_for_conflict = os.path.basename(src_mnt.rstrip("/"))
            else:
                target_name_for_conflict = os.path.basename(src_mnt)
    elif dst_obj_kind == "dir_new" and src_obj_kind == "dir":
        target_dir_for_name = os.path.dirname(dst_mnt.rstrip("/"))
        target_name_for_conflict = os.path.basename(dst_mnt.rstrip("/"))
    elif dst_obj_kind in ("file", "file_existing_for_dir"):
        target_dir_for_name = os.path.dirname(dst_mnt)
        target_name_for_conflict = os.path.basename(dst_mnt)

    target_conflict_path = None
    existing_same_name_target = False
    existing_same_name_target_kind = None
    if target_dir_for_name and target_name_for_conflict:
        target_conflict_path = os.path.join(target_dir_for_name, target_name_for_conflict)
        existing_same_name_target = os.path.exists(target_conflict_path)
        if existing_same_name_target:
            if os.path.isdir(target_conflict_path):
                existing_same_name_target_kind = "dir"
            elif os.path.isfile(target_conflict_path):
                existing_same_name_target_kind = "file"
            else:
                existing_same_name_target_kind = "other"

    overwrite_target_path = None
    overwrite_target_kind = None
    planned_backup_path = None
    if overwrite_rename_dir_target:
        candidate_real = os.path.realpath(dst_mnt)
        if candidate_real == src_mnt:
            log("Refusing to overwrite source directory itself.", "ERROR")
            return 1
        overwrite_target_path = candidate_real
        overwrite_target_kind = "dir"
    elif overwrite_replace_file_target:
        candidate_real = os.path.realpath(dst_mnt)
        if candidate_real == src_mnt:
            log("Refusing to overwrite source directory itself.", "ERROR")
            return 1
        overwrite_target_path = candidate_real
        overwrite_target_kind = "file"
    elif (
        overwrite
        and src_obj_kind == "dir"
        and dst_obj_kind in ("dir", "dir_existing")
        and not src_user_had_trailing
        and not merge_child_into_parent
        and not source_already_in_destination
    ):
        candidate = os.path.join(dst_mnt, os.path.basename(src_mnt.rstrip("/")))
        if os.path.exists(candidate) and os.path.isdir(candidate):
            candidate_real = os.path.realpath(candidate)
            if candidate_real == src_mnt:
                log("Refusing to overwrite source directory itself.", "ERROR")
                return 1
            overwrite_target_path = candidate_real
            overwrite_target_kind = "dir"

    if src_obj_kind == "file":
        src_path = src_mnt
    else:
        # For a directory rename (src dir -> non-existent dst path), rsync needs
        # source contents mode to materialize exactly at the new destination path.
        if (overwrite_rename_dir_target or overwrite_replace_file_target) and not src_user_had_trailing:
            src_path = src_mnt.rstrip("/") + "/"
            rename_dir_to_new_path = True
        elif force_merge_dir_target and not src_user_had_trailing:
            src_path = src_mnt.rstrip("/") + "/"
        elif dst_obj_kind == "dir_new" and not src_user_had_trailing:
            src_path = src_mnt.rstrip("/") + "/"
            rename_dir_to_new_path = True
        # If destination would place the source directory back onto itself
        # (e.g. src=poo/poo, dst=poo), merge source contents into destination.
        elif merge_child_into_parent and not src_user_had_trailing:
            src_path = src_mnt.rstrip("/") + "/"
        elif src_user_had_trailing:
            src_path = src_mnt.rstrip("/") + "/"
        else:
            src_path = src_mnt.rstrip("/")
    if overwrite_rename_dir_target or overwrite_replace_file_target:
        dst_path = dst_mnt.rstrip("/")
    elif dst_obj_kind in ("dir", "dir_existing"):
        dst_path = dst_mnt.rstrip("/") + "/"
    else:
        dst_path = dst_mnt.rstrip("/")

    if src_obj_kind == "dir":
        src_base = os.path.basename(src_mnt.rstrip("/"))
        src_parent = os.path.dirname(src_mnt.rstrip("/"))
        mode_kind = "dir"
    else:
        src_base = os.path.basename(src_mnt)
        src_parent = os.path.dirname(src_mnt)
        mode_kind = "file"

    target_base = target_name_for_conflict or ""
    target_parent = target_dir_for_name or ""

    mode_move = False
    if src_parent and target_parent:
        mode_move = os.path.realpath(src_parent) != os.path.realpath(target_parent)
    if merge_child_into_parent:
        mode_move = False
    if overwrite_parent_from_child:
        mode_move = False

    mode_rename = bool(src_base and target_base and src_base != target_base)

    mode_overwrite = False
    mode_merge = False
    if src_obj_kind == "dir":
        if overwrite_target_path or (existing_same_name_target and overwrite):
            mode_overwrite = True
        elif force_merge_dir_target or existing_same_name_target:
            mode_merge = True
    else:
        if existing_same_name_target:
            mode_overwrite = True

    if source_already_in_destination:
        mode_overwrite = False
        mode_move = False
        mode_merge = False
        mode_rename = False

    backup_source_path = None
    backup_source_kind = None
    if backup_requested and not source_already_in_destination:
        if overwrite_target_path:
            backup_source_path = overwrite_target_path
            backup_source_kind = overwrite_target_kind
        elif (mode_merge or mode_overwrite) and target_conflict_path and os.path.exists(target_conflict_path):
            backup_source_path = os.path.realpath(target_conflict_path)
            backup_source_kind = "dir" if os.path.isdir(backup_source_path) else "file"
            if backup_source_path == src_mnt:
                backup_source_path = None
                backup_source_kind = None

    if backup_source_path:
        planned_backup_path = _plan_backup_path(backup_source_path)
        if not planned_backup_path:
            log(f"Failed to plan backup path for: {backup_source_path}", "ERROR")
            return 1
    mode_backup = bool(planned_backup_path)

    print(
        " ".join(
            [
                _fmt_mode_word("Overwrite", mode_overwrite),
                _fmt_mode_word("Move", mode_move),
                _fmt_mode_word("Merge", mode_merge),
                _fmt_mode_word("Rename", mode_rename),
                _fmt_mode_word("Backup", mode_backup),
                _fmt_mode_word("File", mode_kind == "file"),
                _fmt_mode_word("Dir", mode_kind == "dir"),
            ]
        )
    )
    print("")
    if use_sudo:
        run_command(["sudo", "-v"])

    if source_already_in_destination:
        planned_bytes = 0
        total_regular_files = 0
        add_files = 0
        mod_files = 0
        change_preview = []
    else:
        pre_dst_path = dst_path
        preflight_tmpdir = None
        if overwrite_replace_file_target:
            pre_parent = os.path.dirname(dst_mnt.rstrip("/")) or "."
            preflight_tmpdir = tempfile.mkdtemp(prefix=".move-preflight-", dir=pre_parent)
            pre_dst_path = os.path.join(preflight_tmpdir, "target")
        elif overwrite_parent_from_child:
            pre_parent = os.path.dirname(dst_mnt.rstrip("/")) or "."
            preflight_tmpdir = tempfile.mkdtemp(prefix=".move-preflight-", dir=pre_parent)
            pre_dst_path = os.path.join(preflight_tmpdir, "target")

        try:
            pre_cmd = [
                "rsync",
                "-anH",
                "--itemize-changes",
                "--out-format=%i\t%l\t%n",
                "--stats",
                src_path,
                pre_dst_path,
            ]
            pre_res = run_command(pre_cmd, sudo=use_sudo, check=False)
            pre_rc = getattr(pre_res, "returncode", 0)
            pre_out = (getattr(pre_res, "stdout", "") or "") + "\n" + (getattr(pre_res, "stderr", "") or "")
            if pre_rc not in (0, 24):
                log(f"Pre-scan failed: rsync exited with status {pre_rc}.", "ERROR")
                if pre_out.strip():
                    print(pre_out.rstrip())
                return 1
        finally:
            if preflight_tmpdir:
                shutil.rmtree(preflight_tmpdir, ignore_errors=True)

        planned_bytes = _parse_stat_int("Total transferred file size", pre_out)

        total_regular_files = None
        m_nf = re.search(r"^\s*Number of files:\s*[0-9,]+\s*\(([^)]*)\)\s*$", pre_out, re.MULTILINE)
        if m_nf:
            details = m_nf.group(1) or ""
            m_reg = re.search(r"\breg:\s*([0-9,]+)\b", details)
            if m_reg:
                try:
                    total_regular_files = int(m_reg.group(1).replace(",", ""))
                except Exception:
                    total_regular_files = None

        add_files = 0
        mod_files = 0
        change_preview = []
        for raw in pre_out.splitlines():
            if "\t" not in raw:
                continue
            parts = raw.split("\t", 2)
            if len(parts) < 3:
                continue
            item = (parts[0] or "").strip()
            name = (parts[2] or "").strip()
            if not item or not name:
                continue
            if item.startswith(">f+"):
                add_files += 1
                change_preview.append(("new_file", name))
            elif item.startswith(">f"):
                mod_files += 1
                change_preview.append(("mod_file", name))
            elif item.startswith("cd+"):
                change_preview.append(("new_dir", name.rstrip("/") + "/"))
            elif item.startswith("cL+"):
                add_files += 1
                change_preview.append(("new_file", name))

    if dst_obj_kind in ("dir", "dir_existing", "dir_new"):
        dst_preview_root = (dst_path.rstrip("/") or dst_path) + "/"
    else:
        dst_preview_root = os.path.dirname(dst_path.rstrip("/")) + "/"

    display_change_preview = list(change_preview)
    if force_merge_dir_target and not force_parent_from_child and target_name_for_conflict and display_change_preview:
        wrapped_preview = []
        target_prefix = target_name_for_conflict.rstrip("/")
        for ck, raw_name in display_change_preview:
            item_name = (raw_name or "").lstrip("./")
            if item_name.startswith(f"{target_prefix}/") or item_name == target_prefix:
                wrapped_preview.append((ck, item_name))
            elif item_name:
                wrapped_preview.append((ck, f"{target_prefix}/{item_name}"))
            else:
                wrapped_preview.append((ck, f"{target_prefix}/"))
        display_change_preview = wrapped_preview

    simple_rename_src = None
    simple_rename_dst = None
    simple_rename_parent = None
    rename_target_only = None
    rename_target_is_dir = False
    if src_obj_kind == "dir" and (rename_dir_to_new_path or merge_child_into_parent):
        src_base = os.path.basename(src_mnt.rstrip("/"))
        dst_base = os.path.basename(dst_mnt.rstrip("/"))
        src_parent = os.path.dirname(src_mnt.rstrip("/"))
        dst_parent = os.path.dirname(dst_mnt.rstrip("/"))
        if src_parent == dst_parent and src_base and dst_base and src_base != dst_base:
            simple_rename_src = src_base
            simple_rename_dst = dst_base
            simple_rename_parent = src_parent
        elif src_base and dst_base:
            rename_target_only = dst_base
            rename_target_is_dir = True
    elif src_obj_kind == "file" and dst_obj_kind == "file":
        src_base = os.path.basename(src_mnt)
        dst_base = os.path.basename(dst_mnt)
        src_parent = os.path.dirname(src_mnt)
        dst_parent = os.path.dirname(dst_mnt)
        if src_parent == dst_parent and src_base and dst_base and src_base != dst_base:
            simple_rename_src = src_base
            simple_rename_dst = dst_base
            simple_rename_parent = src_parent
        elif src_base and dst_base and (src_parent != dst_parent or src_base != dst_base):
            rename_target_only = dst_base

    if simple_rename_parent:
        preview_root = (simple_rename_parent.rstrip("/") or "/") + "/"
    elif rename_target_only:
        if rename_target_is_dir:
            rename_parent = os.path.dirname(dst_mnt.rstrip("/"))
        else:
            rename_parent = os.path.dirname(dst_mnt)
        preview_root = (rename_parent.rstrip("/") or "/") + "/"
    else:
        preview_root = dst_preview_root

    if planned_backup_path and not overwrite_target_path:
        backup_parent = os.path.dirname(planned_backup_path.rstrip("/")) or "/"
        current_root = preview_root.rstrip("/") or "/"
        if os.path.realpath(backup_parent) != os.path.realpath(current_root):
            current_root_name = os.path.basename(current_root.rstrip("/"))
            if current_root_name and display_change_preview:
                remapped_preview = []
                for ck, raw_name in display_change_preview:
                    item_name = (raw_name or "").lstrip("./")
                    if item_name.startswith(f"{current_root_name}/") or item_name == current_root_name:
                        remapped_preview.append((ck, item_name))
                    elif item_name:
                        remapped_preview.append((ck, f"{current_root_name}/{item_name}"))
                    else:
                        remapped_preview.append((ck, f"{current_root_name}/"))
                display_change_preview = remapped_preview
            preview_root = (backup_parent.rstrip("/") or "/") + "/"
    print(f"{Colors.WARNING}{preview_root}{Colors.ENDC}")
    if source_already_in_destination:
        entry_name = os.path.basename(src_mnt.rstrip("/")) + ("/" if src_obj_kind == "dir" else "")
        print(f"└── {entry_name}")
        print(f"{Colors.WARNING}(no changes: source already in destination directory){Colors.ENDC}")
    elif overwrite_merge_child_parent and rename_target_only:
        print(f"├── {Colors.FAIL}{rename_target_only} (old){Colors.ENDC}")
        print(f"└── {Colors.OKGREEN}{rename_target_only}/{Colors.ENDC}")
    elif overwrite_target_path:
        base_name = os.path.basename(overwrite_target_path.rstrip("/"))
        if overwrite_parent_from_child:
            old_name = base_name
            new_name = base_name + "/"
        elif overwrite_target_kind == "file":
            old_name = base_name
            new_name = base_name + "/"
        else:
            old_name = base_name + "/"
            new_name = base_name + "/"
        src_base_name = os.path.basename(src_mnt.rstrip("/")) if src_obj_kind == "dir" else os.path.basename(src_mnt)
        src_name = (src_base_name + "/") if src_obj_kind == "dir" else src_base_name
        src_parent = os.path.dirname(src_mnt.rstrip("/")) if src_obj_kind == "dir" else os.path.dirname(src_mnt)
        preview_parent = preview_root.rstrip("/") or "/"
        show_source = (
            bool(src_name)
            and bool(src_base_name)
            and src_base_name != base_name
            and os.path.realpath(src_parent) == os.path.realpath(preview_parent)
        )
        if show_source:
            if backup_requested and planned_backup_path:
                backup_base = os.path.basename(planned_backup_path.rstrip("/"))
                backup_suffix = "/" if backup_source_kind != "file" else ""
                print(f"├── {Colors.FAIL}{src_name}{Colors.ENDC}")
                print(f"├── {Colors.FAIL}{old_name} (old){Colors.ENDC}")
                print(f"├── {Colors.OKGREEN}{new_name} (new){Colors.ENDC}")
                print(f"└── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
            else:
                print(f"├── {Colors.FAIL}{src_name}{Colors.ENDC}")
                print(f"├── {Colors.FAIL}{old_name} (old){Colors.ENDC}")
                print(f"└── {Colors.OKGREEN}{new_name} (new){Colors.ENDC}")
        else:
            if backup_requested and planned_backup_path:
                backup_base = os.path.basename(planned_backup_path.rstrip("/"))
                backup_suffix = "/" if backup_source_kind != "file" else ""
                print(f"├── {Colors.FAIL}{old_name} (old){Colors.ENDC}")
                print(f"├── {Colors.OKGREEN}{new_name} (new){Colors.ENDC}")
                print(f"└── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
            else:
                print(f"├── {Colors.FAIL}{old_name} (old){Colors.ENDC}")
                print(f"└── {Colors.OKGREEN}{new_name} (new){Colors.ENDC}")
    elif simple_rename_src and simple_rename_dst:
        rename_suffix = "/" if src_obj_kind == "dir" else ""
        rename_target_color = Colors.WARNING if existing_same_name_target else Colors.OKGREEN
        if planned_backup_path and not overwrite_target_path:
            backup_base = os.path.basename(planned_backup_path.rstrip("/"))
            backup_suffix = "/" if backup_source_kind != "file" else ""
            print(f"├── {Colors.FAIL}{simple_rename_src}{rename_suffix}{Colors.ENDC}")
            print(f"├── {rename_target_color}{simple_rename_dst}{rename_suffix}{Colors.ENDC}")
            print(f"└── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
        else:
            print(f"├── {Colors.FAIL}{simple_rename_src}{rename_suffix}{Colors.ENDC}")
            print(f"└── {rename_target_color}{simple_rename_dst}{rename_suffix}{Colors.ENDC}")
    elif merge_child_into_parent and rename_target_only:
        if planned_backup_path and not overwrite_target_path:
            backup_base = os.path.basename(planned_backup_path.rstrip("/"))
            backup_suffix = "/" if backup_source_kind != "file" else ""
            print(f"├── {Colors.WARNING}{rename_target_only}/{Colors.ENDC}")
            print(f"└── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
        else:
            print(f"└── {Colors.WARNING}{rename_target_only}/{Colors.ENDC}")
        if change_preview:
            max_preview = 10
            preview_prefix = "│   " if planned_backup_path and not overwrite_target_path else "    "
            _print_preview_tree(display_change_preview[:max_preview], base_prefix=preview_prefix)
            if len(display_change_preview) > max_preview:
                more_prefix = "│   " if planned_backup_path and not overwrite_target_path else "    "
                print(f"{more_prefix}... and {len(display_change_preview) - max_preview:,} more")
    elif rename_target_only:
        suffix = "/" if rename_target_is_dir else ""
        rename_target_color = Colors.WARNING if existing_same_name_target else Colors.OKGREEN
        if planned_backup_path and not overwrite_target_path:
            backup_base = os.path.basename(planned_backup_path.rstrip("/"))
            backup_suffix = "/" if backup_source_kind != "file" else ""
            print(f"├── {rename_target_color}{rename_target_only}{suffix}{Colors.ENDC}")
            print(f"└── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
        else:
            print(f"└── {rename_target_color}{rename_target_only}{suffix}{Colors.ENDC}")
    elif change_preview:
        max_preview = 10
        if planned_backup_path and not overwrite_target_path:
            backup_base = os.path.basename(planned_backup_path.rstrip("/"))
            backup_suffix = "/" if backup_source_kind != "file" else ""
            print(f"├── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
            _print_preview_tree(display_change_preview[:max_preview])
        else:
            _print_preview_tree(display_change_preview[:max_preview])
        if len(display_change_preview) > max_preview:
            if planned_backup_path and not overwrite_target_path:
                print(f"... and {len(display_change_preview) - max_preview:,} more")
            else:
                print(f"... and {len(display_change_preview) - max_preview:,} more")
    else:
        if planned_backup_path and not overwrite_target_path:
            backup_base = os.path.basename(planned_backup_path.rstrip("/"))
            backup_suffix = "/" if backup_source_kind != "file" else ""
            print(f"└── {Colors.OKGREEN}{backup_base}{backup_suffix} (backup){Colors.ENDC}")
        else:
            print("(no new additions)")

    print("")
    if total_regular_files is not None:
        unchanged_files = max(0, int(total_regular_files) - int(add_files) - int(mod_files))
        print(f"Regular files: new={add_files:,}  modified={mod_files:,}  unchanged={unchanged_files:,}")
    print(f"Planned transfer bytes: {planned_bytes:,} ({_format_bytes_binary(str(planned_bytes), decimals=2)})")
    print("")

    no_changes_planned = bool(source_already_in_destination or (planned_bytes == 0 and not change_preview))
    if no_changes_planned:
        log("No changes detected; nothing to move.")
        return 0

    ans = (input("Proceed with move? [y/N]: ") or "").strip().lower()
    if ans not in ("y", "yes"):
        print(f"{Colors.FAIL}Cancelled.{Colors.ENDC}")
        return 0

    if source_already_in_destination:
        log("No changes: source is already in destination directory.")
        return 0

    if use_sudo:
        run_command(["sudo", "-v"])
    log(f"Starting rsync move: {source_input} -> {destination}...")
    start_ts = time.time()
    try:
        if backup_requested and backup_source_path and not overwrite_target_path:
            if backup_source_kind == "file":
                log(f"Backing up existing file: {backup_source_path}")
            else:
                log(f"Backing up existing directory: {backup_source_path}")
            backup_path = _copy_path_to_backup(backup_source_path, planned_backup_path, use_sudo=use_sudo)
            if not backup_path:
                return 1
            log(f"Backup saved as: {backup_path}")
        if overwrite_target_path:
            if overwrite_parent_from_child:
                stage_parent = os.path.dirname(dst_mnt.rstrip("/")) or "."
                stage_path = tempfile.mkdtemp(prefix=".move-stage-", dir=stage_parent)
                log(f"Staging source before overwrite: {stage_path}")
                rc_move = _run_move(src_path, stage_path, planned_bytes, use_sudo)
                if rc_move in (0, 24):
                    _cleanup_source_dirs(src_mnt, remove_root=True, use_sudo=use_sudo)
                    if backup_requested:
                        log(f"Backing up existing directory: {overwrite_target_path}")
                        backup_base = planned_backup_path or _backup_base_path(overwrite_target_path)
                        backup_path = _backup_path_with_base(overwrite_target_path, use_sudo=use_sudo, base=backup_base)
                        if not backup_path:
                            _remove_path_recursive(stage_path, use_sudo=use_sudo)
                            return 1
                        log(f"Backup saved as: {backup_path}")
                    else:
                        log(f"Overwriting existing directory: {overwrite_target_path}")
                        if not _remove_path_recursive(overwrite_target_path, use_sudo=use_sudo):
                            _remove_path_recursive(stage_path, use_sudo=use_sudo)
                            return 1
                    mv_res = run_command(["mv", "--", stage_path, overwrite_target_path], check=False, sudo=use_sudo)
                    if getattr(mv_res, "returncode", 0) != 0:
                        log("Failed to place staged directory into destination.", "ERROR")
                        _remove_path_recursive(stage_path, use_sudo=use_sudo)
                        return 1
                    if rc_move == 0:
                        log("Move complete.")
                        return 0
                    log("Move completed with warnings: some source files vanished during transfer (rsync exit 24).", "WARN")
                    log("This is expected on active trees (e.g., browser cache, temp files). Re-run move to converge.", "WARN")
                    return 0
                log(f"Move failed: rsync exited with status {rc_move}.", "ERROR")
                _remove_path_recursive(stage_path, use_sudo=use_sudo)
                return rc_move
            if backup_requested:
                if overwrite_target_kind == "file":
                    log(f"Backing up existing file: {overwrite_target_path}")
                else:
                    log(f"Backing up existing directory: {overwrite_target_path}")
                backup_base = planned_backup_path or _backup_base_path(overwrite_target_path)
                backup_path = _backup_path_with_base(overwrite_target_path, use_sudo=use_sudo, base=backup_base)
                if not backup_path:
                    return 1
                log(f"Backup saved as: {backup_path}")
            else:
                if overwrite_target_kind == "file":
                    log(f"Overwriting existing file: {overwrite_target_path}")
                else:
                    log(f"Overwriting existing directory: {overwrite_target_path}")
                if not _remove_path_recursive(overwrite_target_path, use_sudo=use_sudo):
                    return 1
        rc_move = _run_move(src_path, dst_path, planned_bytes, use_sudo)
        if rc_move in (0, 24) and src_obj_kind == "dir":
            remove_root = (not src_user_had_trailing) or rename_dir_to_new_path
            _cleanup_source_dirs(src_mnt, remove_root=remove_root, use_sudo=use_sudo)
        if rc_move == 0:
            log("Move complete.")
            return 0
        if rc_move == 24:
            log("Move completed with warnings: some source files vanished during transfer (rsync exit 24).", "WARN")
            log("This is expected on active trees (e.g., browser cache, temp files). Re-run move to converge.", "WARN")
            return 0
        log(f"Move failed: rsync exited with status {rc_move}.", "ERROR")
        return rc_move
    except Exception as e:
        log(f"Move failed: {e}", "ERROR")
        return 1
    finally:
        print(f"Duration: {_fmt_hms(time.time() - start_ts)}")


if __name__ == "__main__":
    sys.exit(main())
