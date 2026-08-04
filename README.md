# copy

Rust CLI for local filesystem transfers with preview/confirm flow.

## Requirements

- Linux
- Rust toolchain (`cargo`)
- `rsync` (used when source device is rotational HDD, and for sudo transfer mode)

## Project Structure

```text
copy/
├── .gitignore
├── Cargo.toml
├── copy                  # launcher script (builds + runs Rust binary)
├── README.md
├── src/
│   └── main.rs           # full CLI implementation
└── tests/
    ├── test_cli_matrix.py
    └── test_copy_cli.py
```

## Command

```bash
./copy [OPTIONS] [--preview] [--preview-lite] SOURCE DESTINATION
```

- Default mode: copy
- Move mode: `-m`, `--move`

## Flags

- `-m`, `--move`
  - Move mode (transfer then remove source data).
- `-s`, `--sudo`
  - Run transfer/removal commands with sudo.
- `-o`, `--overwrite`
  - Replace conflicting destination target instead of merge behavior.
- `-c`, `--contents-only`
  - Merge source contents directly into destination path (no source-basename nesting).
- `-b`, `--backup`
  - Create timestamped backup when destination data would be merged/replaced.
- `--sync`
  - Make the destination tree match the source using native Rust transfer and cleanup phases for local, non-elevated operations.
  - Copy files whose type, size, or modification time differs, then delete destination-only entries after the transfer has flushed.
  - Remote and `--sudo` sync operations retain the rsync backend.
- `-v`, `--verbose`, `--showall`
  - Show hierarchical preview: up to 5 changed entries per level (modified first), expand only modified folders, and abbreviate remaining new/modified/unchanged/removed counts.
- `--preview`
  - Run only the preview phase and exit (no confirmation prompt, no transfer).
- `--preview-lite`
  - Faster preview-only mode that skips exact byte scanning when destination tree is brand-new.

## Backend Selection

- Preview is always done in Rust using `jwalk` traversal + `rayon` parallel comparison.
- Local, non-elevated copy, move, and sync operations use the Rust backend.
- The Rust backend tunes worker count, buffering, and writeback pacing for NVMe, HDD, and other media.
- Remote endpoints and `--sudo` force the rsync backend.

## Performance Build Settings

- `copy` builds and runs `target/release/copy-rs` by default.
- Release profile uses aggressive optimization (`opt-level=3`, `lto=fat`, `codegen-units=1`, `panic=abort`, stripped symbols).
- Host tuning is enabled with `-C target-cpu=native` via `.cargo/config.toml`.

## Runtime Behavior

- `SOURCE/*` is treated as contents-only mode (same as `-c` on `SOURCE/`).
- Parent/self-overlap safety is enforced.
- Local mode performs a destination free-space preflight using filesystem stats before transfer (no sudo required).
- Move mode cleans empty source directories after transferred files are removed.
- Mode line and preview output remain compatible with the previous CLI behavior.

## Build

```bash
cargo build --release
```

The launcher `./copy` auto-builds `target/release/copy-rs` when needed.

## Test

```bash
python3 -m unittest discover -s tests -v
```
