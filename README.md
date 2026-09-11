# ns-cos-sync

`ns-cos-sync` is a command-line utility for synchronizing objects between Akamai NetStorage and Tencent Cloud COS. It calls both services through their HTTP APIs and does not depend on vendor SDKs.

Copyright (c) 2026 Lionel Guo<br>
Contact: lionelliguo@gmail.com

## Features

- Bidirectional NetStorage-to-COS and COS-to-NetStorage synchronization
- Single-file and recursive directory/prefix synchronization
- Full and incremental synchronization strategies
- Change detection using ETag, or last-modified time plus file size
- Multiple NetStorage CP Code profiles
- Concurrent transfers, HTTP retries, and end-to-end file retries
- Sharded state files and directory-list caching
- Optional ZIP extraction for NetStorage-to-COS transfers
- Scheduled execution and failed-file retries
- Resume support through successful-file logs
- Configurable NetStorage metadata collection for large or unstable trees
- Zero-byte object preservation without converting files into directory markers
- File-count, byte-count, throughput, elapsed-time, and ETA progress reporting
- Per-directory failure handling so one listing error does not have to stop a large migration

## Requirements

- Python 3.10 or later
- `requests`
- The system `unzip` command when automatic ZIP extraction is enabled

Install the dependencies in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Configuration

The committed `ns-cos-config.json` contains no credentials and can be used as a template. Copy it to a local configuration file that is ignored by Git:

```bash
cp ns-cos-config.json ns-cos-config.local.json
chmod 600 ns-cos-config.local.json
```

Provide the required connection and credential fields:

```json
{
  "netstorage": {
    "profiles": [
      {
        "name": "example",
        "host": "example.akamaihd.net",
        "cp_code": "123456",
        "key_name": "YOUR_KEY_NAME",
        "key_secret": "YOUR_KEY_SECRET"
      }
    ]
  },
  "cos": {
    "secret_id": "YOUR_SECRET_ID",
    "secret_key": "YOUR_SECRET_KEY",
    "region": "ap-singapore",
    "bucket": "example-bucket",
    "appid": "1234567890"
  }
}
```

Never commit a configuration file containing real credentials. Credentials may also be supplied on the command line, but command-line arguments can appear in shell history or process listings. A local configuration file with `0600` permissions is safer on a multi-user system.

The configuration is divided into the following sections:

| Section | Purpose |
| --- | --- |
| `netstorage` | NetStorage endpoint, CP Code, and authentication settings |
| `cos` | COS authentication, region, bucket, and pagination settings |
| `sync` | Strategy, change detection, ZIP, cache, and logging options |
| `state` | Single-file or sharded incremental state settings |
| `schedule` | Built-in scheduling settings |
| `retry` | HTTP timeout and exponential-backoff settings |
| `transfer` | Concurrency, temporary files, TLS, and progress reporting |

Each item in `netstorage.profiles` may define its own host, CP Code, key name, key secret, COS destination prefix, state paths, and failure log. Profiles are processed sequentially, while files within a profile can be transferred concurrently.

Command-line options override values from the configuration file. Display all available options with:

```bash
python3 ns-cos-sync.py ns-to-cos --help
python3 ns-cos-sync.py cos-to-ns --help
python3 ns-cos-sync.py retry-failed --help
```

## Usage

### NetStorage to COS

Synchronize one file:

```bash
python3 ns-cos-sync.py --config ns-cos-config.local.json \
  ns-to-cos /source/file.txt destination/file.txt
```

Synchronize a directory recursively:

```bash
python3 ns-cos-sync.py --config ns-cos-config.local.json \
  ns-to-cos /source/ destination/ --recursive
```

A source path ending in `/` also enables recursive mode automatically.

### COS to NetStorage

Synchronize one object:

```bash
python3 ns-cos-sync.py --config ns-cos-config.local.json \
  cos-to-ns source/file.txt /destination/file.txt
```

Synchronize a prefix recursively:

```bash
python3 ns-cos-sync.py --config ns-cos-config.local.json \
  cos-to-ns source/ /destination/ --recursive
```

### Retry Failed Files

`retry-failed` reads `failed-files.log` and retries the listed NetStorage-to-COS transfers without walking the source directory again:

```bash
python3 ns-cos-sync.py --config ns-cos-config.local.json \
  retry-failed failed-files.log
```

## Path Mapping

For recursive transfers, the path relative to the source prefix is appended to the destination prefix:

```text
ns-to-cos /uat/ backup/
/uat/images/a.png  ->  COS backup/images/a.png
```

A root-to-root transfer preserves the complete relative path:

```text
ns-to-cos / /
/uat/images/a.png  ->  COS uat/images/a.png
```

When multiple CP Codes are configured, enable `sync.add_cp_code_to_cos_path` or assign a separate `cos_dest_prefix` to each profile so identical paths do not overwrite one another. With automatic CP Code prefixes enabled, `/uat/a.png` from CP Code `123456` is written to COS as `123456/uat/a.png`.

## Full and Incremental Synchronization

- `strategy=full` selects every source file on each run.
- `strategy=incremental` selects objects that are missing from the state or whose metadata has changed.
- `detect=etag` compares ETags first and falls back to last-modified time plus size.
- `detect=last_modified` compares last-modified time plus size.

The default sharded state layout is:

```text
state-meta.json
state-shards/_root.json
state-shards/<first-level-directory>.json
```

State is updated only after a successful transfer. The current implementation records objects seen during a run but does not remove stale state entries; `prune_state` should not yet be treated as an implemented cleanup feature.

### Metadata Collection

NetStorage metadata can be collected in two ways:

- `prefer_list_metadata=true` reuses ETag, size, and modification data returned by directory listings. This avoids a separate `stat` request for every file and is recommended for large trees or unstable connections.
- `prefer_list_metadata=false` performs a NetStorage `stat` request for each file.
- `stat_if_list_metadata_incomplete=true` uses directory-list metadata when possible and falls back to `stat` only when the listing does not contain a usable ETag or last-modified value.

This choice affects both performance and change-detection quality. Listing metadata is faster, while per-file `stat` requests may provide more complete information.

## ZIP Handling

Automatic ZIP extraction applies only to NetStorage-to-COS transfers:

- `extract_zip_on_ns_to_cos=true` downloads a ZIP and uploads its extracted entries.
- `zip_extract_to_folder=true` extracts `name.zip` under the COS `name/` prefix.
- `zip_extract_to_folder=false` uploads extracted entries directly under the configured destination prefix.
- `strip_redundant_zip_root=true` removes a ZIP's redundant top-level directory when it matches the archive name.
- `strip_redundant_zip_root=false` preserves the archive's internal directory structure.
- `zip_upload_original=true` uploads the original ZIP in addition to its extracted contents.
- `extract_zip_on_ns_to_cos=false` treats ZIP files as ordinary objects. COS-to-NetStorage transfers never extract ZIP files automatically.

Enable automatic extraction only for trusted ZIP files. An archive can contain a very large number of files, highly compressed data, or symbolic links. Ensure that the temporary directory has sufficient controlled storage.

## Logs and Caches

The program may create the following runtime files:

| File | Contents |
| --- | --- |
| `failed-sync.log` | Detailed synchronization errors |
| `success-files.log` | Successful file records used to resume interrupted runs |
| `failed-files.log` | Failed file records accepted by `retry-failed` |
| `failed-dirs.log` | NetStorage directory-list failures |
| `list-dir-cache-*.json` | Cached NetStorage directory listings |

Reading a list cache skips a live directory walk, so a stale cache may omit new files. Verify that the cache belongs to the current CP Code and source path before enabling `read_list_cache`.

### Resume After Interruption

When `skip_success_log=true`, the program loads `success-files.log` before selecting files. A source-to-destination pair already recorded with an `OK` status is skipped and written into incremental state. This is useful when resuming a large migration after interruption without retransmitting completed files.

Success-log matching is path based. Remove or rotate the log when the same paths must be copied again regardless of earlier results.

### Directory-list Cache

NetStorage directory walking can be expensive for a large hierarchy. The cache controls provide a reusable inventory:

- `read_list_cache=false` performs a live walk. If `write_list_cache=true`, the resulting file and directory lists are saved afterward.
- `read_list_cache=true` skips the live walk and loads the saved inventory.
- `list_cache_file` accepts `{cp_code}`, `{source}`, `{source_hash}`, and `{command}` placeholders.

The default filename includes the CP Code and a source-path hash to reduce collisions between different migrations.

## Retry and Failure Handling

Retries operate at three levels:

| Setting | Scope |
| --- | --- |
| `retry.max_attempts` | Individual HTTP requests |
| `transfer.ns_list_retries` | NetStorage directory-list requests |
| `sync.copy_file_attempts` | Complete file transfers, including download and upload |

Retries use exponential backoff with jitter. A value of `3` means three total attempts: the initial attempt plus up to two retries.

If `skip_failed_dirs=true`, a directory that still cannot be listed after all retries is written to `failed-dirs.log`, and synchronization continues with the remaining directories. If `continue_on_error=true`, individual transfer failures are also logged without stopping the entire run.

For repeated SSL EOF or connection-reset errors, reduce `transfer.workers` to `1`, increase the timeout and retry counts, and use longer backoff intervals. Lower concurrency reduces pressure on NetStorage at the cost of transfer speed.

## Transfer Behavior and Progress

During file-copy phases, progress output includes:

- Completed, copied, failed, and pending file counts
- Total bytes, processed bytes, and successfully copied bytes
- Transfer throughput and files per second
- Percentage complete, elapsed time, and estimated time remaining

Progress frequency is controlled by `copy_progress_every_files`, `select_progress_every_files`, `list_progress_every_dirs`, and `progress_interval_seconds`.

Real zero-byte source files are uploaded to COS as empty objects using their original object keys. The program does not append `/`, so a zero-byte file remains distinct from a COS directory marker.

## Scheduled Execution

Run the built-in scheduling loop with:

```bash
python3 ns-cos-sync.py --config ns-cos-config.local.json \
  ns-to-cos /source/ destination/ --schedule --schedule-interval 300
```

For production deployments, use `systemd`, a container orchestrator, or another process manager, and monitor exit status and failure logs.

## Security Recommendations

- Rotate Akamai and Tencent Cloud credentials regularly and apply least privilege.
- Set private configuration file permissions to `0600`.
- Never write authentication headers to public logs.
- Do not enable `skip_tls_verify` except for temporary diagnostics.
- Temporary directories may briefly contain complete object data; use a restricted location with sufficient capacity.

## License

This project does not currently include a license file. Add an explicit license before publishing the project for reuse.
