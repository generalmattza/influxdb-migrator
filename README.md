# InfluxDB Migration Script

This script (`influx_migrate.py`) copies time-series data from one InfluxDB v2 instance to another, with support for filtering, transforming, and injecting tags, measurements, and fields. It reads credentials from `.influx.toml` configuration files and provides options for verifying data, simulating migrations, and applying mappings.

## Prerequisites

- **Python 3.6+**
- **Required Python package**: `influxdb-client`
  ```bash
  pip install influxdb-client
  ```
- **InfluxDB v2 instances**: Source and destination InfluxDB instances with valid credentials.
- **Configuration files**: Two `.influx.toml` files for source and destination instances.

### Configuration File Format

Create `.influx.toml` files for both source and destination InfluxDB instances with the following format:

```toml
url = "http://localhost:8086"
token = "<auth-token>"
org = "my-org"
timeout = 6000                # optional (ms)
connection_pool_maxsize = 25  # optional
auth_basic = false            # optional
```

Place these files in a secure location (e.g., `~/.influx.src.toml` and `~/.influx.dst.toml`).

## Installation

1. Save the script as `influx_migrate.py`.
2. Ensure the `influxdb-client` package is installed.
3. Prepare your `.influx.toml` configuration files.

## Usage

Run the script from the command line with the following syntax:

```bash
python influx_migrate.py [arguments]
```

### Arguments

| Argument | Description | Required | Type | Notes |
|----------|-------------|----------|------|-------|
| `--src-config` | Path to the source `.influx.toml` file | Yes | String | Example: `--src-config ~/.influx.src.toml` |
| `--dst-config` | Path to the destination `.influx.toml` file | Yes | String | Example: `--dst-config ~/.influx.dst.toml` |
| `--src-bucket` | Source bucket name | Yes | String | Example: `--src-bucket my-bucket` |
| `--dst-bucket` | Destination bucket name | Yes | String | Example: `--dst-bucket my-bucket` |
| `--start` | Start time for the query | Yes | String | Formats: RFC3339 (`2025-07-22T15:00:00Z`), duration (`-4d`), or `now()` |
| `--stop` | Stop time for the query | No | String | Same formats as `--start`. Defaults to current time if omitted |
| `--measurement` | Specific measurement(s) to collect | No | String, repeatable | Example: `--measurement heaters --measurement sensors` |
| `--measurement-regex` | Regex pattern to match measurements | No | String | Mutually exclusive with `--measurement`. Example: `--measurement-regex "^(heaters|sensors)$"` |
| `--field` | Specific field(s) to collect | No | String, repeatable | Example: `--field temp --field pressure` |
| `--tag` | Filter by tag key-value pairs | No | String, repeatable | Format: `key=value` or `key=~/regex/`. Example: `--tag device=~/^prod-.*$/` |
| `--window` | Time window size for batching | No | String | Format: `<number><unit>` (e.g., `6h`, `1d`). Default: `6h` |
| `--batch-size` | Number of points per write batch | No | Integer | Default: `5000` |
| `--verify` | Count records per window without writing | No | Flag | Mutually exclusive with `--dry-run` |
| `--dry-run` | Log transformed points without writing | No | Flag | Mutually exclusive with `--verify` |
| `--tag-map` | Remap tag values | No | String, repeatable | Formats: `key=from->to`, `key=~/regex/->replacement`, `key=prefix*->replacement`. Example: `--tag-map id=heaters*->control` |
| `--measurement-map` | Remap measurement names | No | String, repeatable | Formats: `from->to`, `~/regex/->replacement`. Example: `--measurement-map heaters->control` |
| `--field-map` | Remap field names | No | String, repeatable | Formats: `from->to`, `~/regex/->replacement`. Example: `--field-map temp->temperature` |
| `--tag-inject` | Inject new tags | No | String, repeatable | Formats: `new_tag=value`, `new_tag=old_tag:from->to`, `new_tag=old_tag:~/regex/->replacement`, `new_tag=old_tag:prefix*->replacement`. Example: `--tag-inject env=production` |
| `--verbose` | Enable detailed logging of mappings and injections | No | Flag | Logs remaps and injections at `DEBUG` level |

### Notes
- **Time Formats**: Use RFC3339 (`2025-07-22T15:00:00Z`), relative durations (`-4d` for 4 days ago), or `now()`. In PowerShell, quote relative durations: `--start "-4d"`.
- **Mutually Exclusive Arguments**:
  - `--verify` and `--dry-run` cannot be used together.
  - `--measurement` and `--measurement-regex` cannot be used together.
- **Regex Syntax**: For `--measurement-regex` or regex-based mappings, use Flux-compatible regex (e.g., `^prefix.*$` or `^(value1|value2)$`). Optional `~/` delimiters are supported (e.g., `~/^prefix.*$/`).
- **Performance**: Use `--verify` for quick data volume checks. Use `--dry-run` without `--verbose` to reduce logging overhead during testing.

## Example Queries

Below are example commands demonstrating common use cases. Adjust paths, bucket names, and time ranges to match your environment.

### 1. Migrate Data for a Single Measurement
Migrate all data for the `heaters` measurement from July 22, 2025, to July 24, 2025, with tag and measurement remapping:

```bash
python influx_migrate.py \
  --src-config ~/.influx.src.toml \
  --dst-config ~/.influx.dst.toml \
  --src-bucket plungecaster \
  --dst-bucket plungecaster \
  --start 2025-07-22T15:00:00Z \
  --stop 2025-07-24T08:00:00Z \
  --measurement heaters \
  --tag-map id=heaters*->control \
  --measurement-map heaters->control \
  --field-map temp->temperature \
  --tag-inject env=production \
  --verbose
```

**What it does**:
- Collects data from the `heaters` measurement.
- Remaps `id` tags starting with `heaters` to start with `control` (e.g., `heaters/IO_Raw.K_LHT_10_QB` → `control/IO_Raw.K_LHT_10_QB`).
- Remaps the measurement `heaters` to `control`.
- Remaps the field `temp` to `temperature`.
- Injects a static tag `env=production`.
- Logs detailed mapping information due to `--verbose`.

### 2. Migrate Data for Multiple Measurements
Migrate data for both `heaters` and `sensors` measurements:

```bash
python influx_migrate.py \
  --src-config ~/.influx.src.toml \
  --dst-config ~/.influx.dst.toml \
  --src-bucket plungecaster \
  --dst-bucket plungecaster \
  --start -7d \
  --measurement heaters \
  --measurement sensors \
  --tag-inject source=plunge_caster_heater_control_CX68ABF8
```

**What it does**:
- Collects data from `heaters` and `sensors` measurements for the past 7 days.
- Injects a static tag `source=plunge_caster_heater_control_CX68ABF8` to all points.
- Writes the data to the destination bucket.

### 3. Migrate Data Using a Measurement Regex
Migrate data for measurements matching a regex pattern (e.g., `heaters` or `sensors`):

```bash
python influx_migrate.py \
  --src-config ~/.influx.src.toml \
  --dst-config ~/.influx.dst.toml \
  --src-bucket plungecaster \
  --dst-bucket plungecaster \
  --start 2025-07-22T15:00:00Z \
  --measurement-regex "^(heaters|sensors)$" \
  --tag-map device=PlungeCaster_Heater_ADSClient->CX-68ABF8 \
  --tag-inject panel_name=Plunge Caster Heater Control \
  --dry-run \
  --verbose
```

**What it does**:
- Collects data from measurements matching the regex `^(heaters|sensors)$` (i.e., `heaters` or `sensors`).
- Remaps the `device` tag from `PlungeCaster_Heater_ADSClient` to `CX-68ABF8`.
- Injects a static tag `panel_name=Plunge Caster Heater Control`.
- Simulates the migration (`--dry-run`) and logs each point and transformation (`--verbose`).

### 4. Verify Data Volume Without Writing
Check the number of records for a specific measurement without writing:

```bash
python influx_migrate.py \
  --src-config ~/.influx.src.toml \
  --dst-config ~/.influx.dst.toml \
  --src-bucket plungecaster \
  --dst-bucket plungecaster \
  --start -1d \
  --measurement heaters \
  --verify
```

**What it does**:
- Counts records in the `heaters` measurement for the past day.
- Logs the number of records per time window and the total.
- Does not apply mappings or write data.

### 5. Migrate All Measurements with Tag Filtering
Migrate all measurements with a specific tag value:

```bash
python influx_migrate.py \
  --src-config ~/.influx.src.toml \
  --dst-config ~/.influx.dst.toml \
  --src-bucket plungecaster \
  --dst-bucket plungecaster \
  --start -4h \
  --tag device=~/^prod-.*$/ \
  --batch-size 10000
```

**What it does**:
- Collects data from all measurements where the `device` tag matches the regex `^prod-.*$`.
- Processes data from the past 4 hours.
- Uses a larger batch size (`10000`) for better performance.

## Debugging

To debug the script, use the provided `debug_influx_migrate.py` script, which includes a sample `debug_argv` function with default arguments. Modify the arguments to suit your needs and run it in a Python REPL or debugger:

```python
import migrator
migrator.run(migrator.debug_argv())
```

You can also customize the arguments directly in the debug script. Example:

```python
migrator.run([
    "--src-config", r".\.influx.plungecaster-old.toml",
    "--dst-config", r".\.influx.metsys-node1.toml",
    "--src-bucket", "plungecaster",
    "--dst-bucket", "plungecaster",
    "--start", "2025-07-22T15:00:00Z",
    "--stop", "2025-07-24T08:00:00Z",
    "--measurement-regex", "^(heaters|sensors)$",
    "--tag-map", "id=heaters*->control",
    "--tag-inject", "env=production",
    "--verbose",
    "--dry-run",
])
```

## Troubleshooting

- **Excessive Logging**: If the script is slow due to logging, avoid `--verbose` unless debugging. Use `--dry-run` without `--verbose` for faster testing.
- **Invalid Time Format**: Ensure `--start` and `--stop` use valid formats (e.g., `2025-07-22T15:00:00Z` or `-4d`). Quote relative durations in PowerShell.
- **Regex Errors**: For `--measurement-regex` or tag regexes, ensure the pattern is Flux-compatible and properly escaped. Test with `--dry-run` to verify.
- **Mutual Exclusivity**: Avoid combining `--measurement` and `--measurement-regex` or `--verify` and `--dry-run`.

## Example Output

Running with `--dry-run` and `--verbose`:

```
2025-09-12 15:30:29,943 INFO [window 1] 2025-07-22T15:00:00Z → 2025-07-22T21:00:00Z
2025-09-12 15:30:29,943 DEBUG Name remap: 'heaters' -> 'control'
2025-09-12 15:30:29,943 DEBUG Tag remap: id: 'heaters/IO_Raw.K_LHT_10_QB' -> 'control/IO_Raw.K_LHT_10_QB'
2025-09-12 15:30:29,943 INFO Dry run: would write Point(measurement=control, tags=[id=control/IO_Raw.K_LHT_10_QB, env=production], field={'temperature': 0.0}, time=2025-07-22 15:19:40.240000+00:00)
2025-09-12 15:30:29,944 INFO Dry run: would write Point(measurement=sensors, tags=[id=sensors/IO_Raw.SENSOR_01, env=production], field={'SENSOR_01': 25.5}, time=2025-07-22 15:19:40.240000+00:00)
2025-09-12 15:30:29,956 INFO would write 20 records (total 20)
2025-09-12 15:30:29,956 INFO Dry run complete. Would write 20 records.
```

Running with `--verify`:

```
2025-09-12 15:30:29,943 INFO [window 1] 2025-07-22T15:00:00Z → 2025-07-22T21:00:00Z
2025-09-12 15:30:29,956 INFO found 20 records
2025-09-12 15:30:29,956 INFO found 20 records (total 20)
2025-09-12 15:30:29,956 INFO Verify complete.
```

## License

This script is provided as-is for use with InfluxDB v2. Ensure you have appropriate permissions and backups before migrating data.