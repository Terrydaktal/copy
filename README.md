# move+copy

Two standalone Python CLI tools for local filesystem transfers using `rsync`:

- `copy`: copy files/directories with preview, progress, and optional backups.
- `move`: move files/directories with preview, progress, optional backups, and source cleanup.

## Requirements

- Linux with:
  - `python3`
  - `rsync`
  - coreutils (`mv`, `cp`, `rm`, `find`)
  - `sudo` (only if you use `--sudo`)

## Project Structure

```text
move+copy/
├── .gitignore
├── copy
├── move
└── README.md
```

## Scripts

### `copy`

Purpose:
- Copies a source file or directory to a destination using `rsync -aH`.
- Shows a dry-run preview tree before making changes.
- Prompts for confirmation before executing.

Inputs:
- Positional:
  - `source`: file or directory path
  - `destination`: directory path, or file path when source is a file
- Flags:
  - `--sudo`: run transfer commands with sudo
  - `-o`, `--overwrite`: replace existing target path when conflicts exist
  - `-f`, `-r`, `--force-rename`: force rename-style merge when rename target exists
  - `-b`, `--backup`: create a timestamped backup of destination content before merge/overwrite

Behavior detail:
- `copy` uses `rsync --size-only`.
- If destination has the same relative file name and same file size, `copy` updates metadata from source without rewriting file content.
- If file size differs, file content is overwritten from source.
- `copy` always performs an rsync dry-run preview first.
- For simple non-merge, non-overwrite, non-backup, non-`source/*` operations, execution uses native `cp -a` for speed.

Outputs:
- Terminal:
  - mode summary line (Overwrite/Copy/Merge/Backup/File/Dir)
  - preview tree of planned changes
  - planned transfer bytes
  - confirmation prompt (`Proceed with copy? [y/N]:`)
  - live progress and duration
  - final completion/warning/error message
- Filesystem:
  - copied content at destination
  - optional backup path (timestamped sibling naming)

### `move`

Purpose:
- Moves a source file or directory to destination using `rsync -aH --remove-source-files`.
- Uses the same preview/confirmation flow as `copy`.
- Cleans up empty source directories after successful transfer.

Behavior detail:
- `move` uses `rsync --size-only`.
- If destination has the same relative file name and same file size, `move` updates destination metadata from source without rewriting file content.
- If file size differs, destination file content is overwritten from source.
- Source trailing slash behaves like normal `mv` (same as no trailing slash). Use `source/*` to move directory contents only.
- `move` always performs an rsync dry-run preview first.
- For simple non-merge, non-overwrite, non-backup, non-`source/*` operations, execution uses native `mv` for speed.

Inputs:
- Positional:
  - `source`: file or directory path
  - `destination`: directory path, or file path when source is a file
- Flags:
  - `--sudo`
  - `-o`, `--overwrite`
  - `-f`, `-r`, `--force-rename`
  - `-b`, `--backup`

Outputs:
- Terminal:
  - mode summary line
  - preview tree
  - planned transfer bytes
  - confirmation prompt (`Proceed with move? [y/N]:`)
  - live progress and duration
  - final completion/warning/error message
- Filesystem:
  - moved content at destination
  - source files removed by rsync
  - empty source directories deleted after successful move
  - optional backup path when merge/overwrite would affect destination content

## Internal Operation Pipeline

Both scripts follow this order:

1. Parse arguments and normalize source/destination paths.
2. Determine operation mode (copy/move, merge, overwrite, backup, file vs directory).
3. Run `rsync` dry-run preflight (`-anH --itemize-changes --stats`) to build preview and planned byte count.
4. Print preview and prompt for confirmation.
5. If confirmed:
   - optionally create backup of conflicting destination content
   - optionally remove/replace conflicting destination content for overwrite flows
   - run live `rsync` with progress output
6. Print final status and duration.
7. For `move`, remove empty source directories after successful transfer.

## Execution Order

There is no mandatory multi-script pipeline. Choose one script per operation:

1. Use `copy` when source must remain in place.
2. Use `move` when source should be relocated.

Recommended safe workflow for important data:

1. Run `copy` first.
2. Validate destination contents.
3. Run `move` only if you then want to remove/relocate source content.

## Usage Examples

```bash
# Copy a directory into an existing destination directory
./copy /data/source_dir /data/target_root/

# Copy a single file to a new filename
./copy /data/a/report.csv /data/b/report-archive.csv

# Move a directory and overwrite conflicting destination content, with backup
./move -o -b /data/old_project /data/new_parent/

# Use sudo for protected locations
./copy --sudo /root/input /mnt/shared/output/

# Same-name + same-size files get metadata synced without rewriting file data
./copy /data/source_dir/ /data/target_dir/
```

## Notes

- The `copy` script help banner uses `diskcopy` as its program label.
- If passing shell globs such as `*`, quote them so the script can parse intent correctly.
