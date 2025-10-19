## Network Converter (TXT/CSV → XLSX)

Convert network raw data (Wireshark-like TXT/CSV exports) into a polished Excel workbook with one sheet per protocol. Works both as a CLI and a simple Tkinter GUI.

### Features
- **GUI and CLI**: Run with a file picker (`--gui`) or from the terminal.
- **Robust CSV/TXT parsing**: Auto-detects delimiters (comma/tab/semicolon/pipe) and handles quoted CSV correctly.
- **Header handling**: Trusts the first parsed row as the header; optionally skips a leading protocol label line like `Ethernet`, `IPv4`, `IPv6`, `TCP`, `UDP`.
- **Numeric coercion**: Coerces numeric columns by name (Packets/Bytes/Port/Latitude/Longitude/ASN Number) without rewriting header titles.
- **Protocol-specific cleanup**:
  - IPv4/IPv6 sheets: drops `Country`, `City`, `Latitude`, `Longitude`, `AS Number`, `AS Organization`.
  - TCP/UDP sheets: drops `Port`.
- **Excel augmentation (new build)**:
  - If a `Packets` column is present, rows are sorted by per-row percentage of total packets (descending).
  - Adds three computed columns to the right: `Total Packets`, `Total Packets in 100% (B/D *100)`, and `Top 20 %` (Pareto sum written once).
  - Highlights the row that reaches the Pareto cutoff closest to 80% (within the 78–90% band when possible).
  - Creates real Excel Tables, freezes the header row, and auto-fits column widths.
- **Flexible input**: Convert a folder (with optional recursion) or one/more files; globs supported.
- **Built-in self-test**: `--selftest` generates sample inputs and verifies output.

### Requirements
- Python 3
- Python packages: `pandas`, `openpyxl`
- For the GUI on Linux, you may also need your distro's Tk package (e.g., `python3-tk`).

Install Python dependencies:

```bash
pip install pandas openpyxl
```

### Quick start (CLI)

```bash
# Convert all *.txt files in the current folder → network_data.xlsx
python network_converter.py

# Specify an input folder (non-recursive by default)
python network_converter.py data/ -o out.xlsx

# Match specific files (globs OK)
python network_converter.py data/*.txt -o network.xlsx

# Recurse into subdirectories
python network_converter.py data/ -r -o out.xlsx

# Preview only (no Excel writing)
python network_converter.py --list-only data/

# If no files are found from CLI, open GUI automatically
python network_converter.py --on-empty gui
```

### GUI

```bash
python network_converter.py --gui
```

Pick one or more `.txt` files or a folder, choose the output `.xlsx`, and click Convert.

### Inputs and outputs
- **Supported inputs**: Ethernet / IPv4 / IPv6 / TCP / UDP dumps that include a header row. Files can be tab-separated or CSV (with quotes). The parser auto-detects delimiters and will skip an initial line that is just the protocol name.
- **Sheet naming**: The protocol is inferred from the filename (case-insensitive). If ambiguous, a generic `Sheet` name is used. Duplicate sheet names get a numeric suffix.
- **Output**: A single `.xlsx` workbook with one sheet per protocol.
  - Each sheet is an Excel Table with a frozen header row and auto-fitted columns.
  - When a `Packets` (or `Packet`) column is present, the sheet is augmented with:
    - Sorting by per-row % of packets (descending).
    - Extra columns: `Total Packets`, `Total Packets in 100% (B/D *100)`, `Top 20 %` (Pareto value shown once).
    - The row at the Pareto cutoff is highlighted.

### Command-line options

```text
positional:
  inputs              Folder path (converts *.txt) OR one or more .txt files
                      (globs OK). Default: .

optional:
  -o, --output        Output .xlsx path (default: network_data.xlsx)
  -r, --recursive     Recurse into subdirectories when a folder is provided
  -p, --pattern       Glob pattern for folder inputs (default: *.txt)
  --list-only         List what would be parsed and exit (no Excel writing)
  --selftest          Run built-in tests and exit
  --on-empty {gui,recursive,error,selftest,noop}
                      What to do if no files are found (default: gui)
  --on-empty {gui,recursive,error,selftest,noop}
                      What to do if no files are found (default: gui)
  --gui               Launch the Tkinter GUI instead of CLI
```

Behavior when no files are found (CLI): by default it opens the GUI (`--on-empty gui`). You can change this to `recursive`, `error`, `selftest`, or `noop`.

### Examples

```bash
# Convert current folder; default name network_data.xlsx
python network_converter.py

# Convert a nested dataset
python network_converter.py samples/ -r -o reports/network.xlsx

# Convert explicit files
python network_converter.py ethernet.txt ipv4.csv tcp.txt -o out.xlsx
```

### Programmatic usage

```python
from network_converter import parse_folder, write_sheets_to_excel

sheets = parse_folder("data", recursive=False, pattern="*.txt")
write_sheets_to_excel("network_data.xlsx", sheets)
```

### Troubleshooting
- "No module named pandas" or "openpyxl": install dependencies with `pip install pandas openpyxl`.
- GUI fails on Linux due to Tk: install your distro's Tk package (e.g., `sudo apt-get install -y python3-tk`). Otherwise use the CLI.
- Nothing is parsed: ensure your files include a header row. For folder inputs, adjust `--pattern` (default `*.txt`) or use file globs.

### Self-test

```bash
python network_converter.py --selftest
```

Creates temporary sample inputs (tabs and CSV with quotes), writes an Excel workbook, and verifies key behaviors:
- CSV parsing with quotes and delimiter detection
- IPv4/IPv6: geo/ASN columns are dropped from output
- TCP/UDP: `Port` column is dropped
- Excel augmentation: added computed columns, Pareto sum, highlighted cutoff row

---

If you have questions or run into issues, please open an issue with a sample input and the command you ran.
