# copy

Single standalone Python CLI for local filesystem transfers using `rsync`.

- `copy` mode (default): source stays in place.
- `move` mode (`--move`): source is transferred, then removed.

## Requirements

- Linux with:
  - `python3`
  - `rsync`
  - coreutils (`cp`, `mv`, `rm`, `find`)
  - `sudo` (only when using `--sudo`)

## Project Structure

```text
copy/
├── .gitignore
├── copy
└── README.md
```

## Script

### `copy`

Purpose:
- Copy or move a file/directory with preview, confirmation, progress, and optional backup.

Inputs:
- Positional:
  - `source`: file or directory path
  - `destination`: directory path, or file path when source is a file
- Flags:
  - `--move`: run in move mode (transfer then remove source data)
  - `--sudo`: run transfer/removal commands with sudo
  - `-o`, `--overwrite`: replace conflicting destination target
  - `-n`, `--no-nesting`: treat destination directory as the explicit target (merge into it instead of nesting source name under it)
  - `-b`, `--backup`: create timestamped backup when destination data would be merged/replaced
  - `--showall`: preview all destination depth-1 entries (new/changed/unchanged)

Behavior detail:
- Always performs an `rsync` dry-run preflight first (`--itemize-changes --stats`).
- Uses `--size-only` matching for transfer decisions.
- For simple operations (no merge/overwrite/backup/`source/*`), uses native backend:
  - `cp -a` in copy mode
  - `mv` in move mode
- Directory conflict handling:
  - default: may nest source directory under destination directory
  - `-n`: avoid nesting, merge source contents into destination directory path
  - `-o`: overwrite nested conflicting target
  - `-o -n`: overwrite explicit destination directory path
- For move mode, empty source directories are cleaned up after successful transfer.

Outputs:
- Terminal:
  - mode summary line
  - preview tree + top-level summary
  - planned transfer bytes
  - confirmation prompt
  - live progress and duration
  - final completion/warning/error message
- Filesystem:
  - copied or moved content at destination
  - optional timestamped backup directory/file (when requested and applicable)

## Internal Operation Pipeline

Execution flow:

1. Parse args and normalize paths.
2. Resolve source/destination kinds and choose conflict strategy.
3. Run `rsync` dry-run preflight and build preview + byte estimate.
4. Print preview and request confirmation.
5. If confirmed:
   - optionally backup destination conflict target
   - optionally remove conflict target for overwrite flows
   - execute transfer (native `cp`/`mv` or `rsync`)
   - in move mode, clean empty source directories
6. Print final status and duration.

## Usage Examples

```bash
# Copy a directory into a destination root
./copy /data/src/project /data/dst/

# Move a directory
./copy --move /data/src/project /data/archive/

# Replace nested conflicting target (overwrite)
./copy --move -o /data/src/project /data/archive/

# Replace explicit destination directory path (overwrite + no nesting)
./copy --move -o -n /data/src/project /data/archive/project_renamed

# Backup destination data before merge/overwrite
./copy -b -n /data/src/project /data/dst/project_renamed

# Use sudo for protected paths
./copy --sudo /root/input /mnt/shared/output/
```

## Notes

- Use `source/*` if you want to transfer directory contents only.
- Quote globs like `'*'` if you need literal handling by the script.
- Default preview shows only changed destination depth-1 entries plus unchanged counts.
- `--showall` includes unchanged entries in white.
