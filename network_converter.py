"""GUI tool to convert network raw data text files to XLSX tables."""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import Dict, List, Sequence

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from tkinter import filedialog, messagebox, Tk, ttk, StringVar


@dataclass(frozen=True)
class SectionDefinition:
    """Describes the expected headers for a section in the raw data."""

    name: str
    headers: Sequence[str]

    @property
    def normalized_name(self) -> str:
        """Return a sanitized sheet name for use in Excel workbooks."""

        sanitized = re.sub(r"[^0-9A-Za-z ]", "", self.name)
        sanitized = sanitized.strip() or "Sheet"
        # Excel sheet names are limited to 31 characters
        return sanitized[:31]


SECTION_DEFINITIONS: Dict[str, SectionDefinition] = {
    "Ethernet": SectionDefinition(
        name="Ethernet",
        headers=(
            "Address",
            "Port",
            "Packets",
            "Bytes",
            "Tx Packets",
            "Tx Bytes",
            "Rx Packets",
            "Rx Bytes",
        ),
    ),
    "IPv4": SectionDefinition(
        name="IPv4",
        headers=(
            "Address",
            "Packets",
            "Bytes",
            "Tx Packets",
            "Tx Bytes",
            "Rx Packets",
            "Rx Bytes",
        ),
    ),
    "IPv6": SectionDefinition(
        name="IPv6",
        headers=(
            "Address",
            "Packets",
            "Bytes",
            "Tx Packets",
            "Tx Bytes",
            "Rx Packets",
            "Rx Bytes",
        ),
    ),
    "TCP": SectionDefinition(
        name="TCP",
        headers=(
            "Address",
            "Port",
            "Packets",
            "Bytes",
            "Tx Packets",
            "Tx Bytes",
            "Rx Packets",
            "Rx Bytes",
        ),
    ),
    "UDP": SectionDefinition(
        name="UDP",
        headers=(
            "Address",
            "Port",
            "Packets",
            "Bytes",
            "Tx Packets",
            "Tx Bytes",
            "Rx Packets",
            "Rx Bytes",
        ),
    ),
}


class RawNetworkDataParser:
    """Parse raw network telemetry text files into structured tables."""

    def __init__(self, section_definitions: Dict[str, SectionDefinition] | None = None) -> None:
        self._definitions = section_definitions or SECTION_DEFINITIONS

    @property
    def definitions(self) -> Dict[str, SectionDefinition]:
        return self._definitions

    def parse(self, text: str) -> Dict[str, List[List[str]]]:
        """Parse raw text into a mapping of section name to table rows."""

        tables: Dict[str, List[List[str]]] = {}
        current_section: SectionDefinition | None = None

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            normalized = line.rstrip(":")
            definition = self._definitions.get(normalized)
            if definition is not None:
                current_section = definition
                tables.setdefault(current_section.name, [])
                continue

            if current_section is None:
                # Line does not belong to a known section; ignore it for now.
                continue

            if self._is_header_line(line, current_section.headers):
                continue

            row = self._tokenize_row(line, len(current_section.headers))
            tables.setdefault(current_section.name, []).append(row)

        return tables

    @staticmethod
    def _is_header_line(line: str, headers: Sequence[str]) -> bool:
        """Check whether the line matches the expected header names."""

        normalized_line = re.sub(r"\s+", " ", line.strip()).lower()
        normalized_headers = " ".join(headers).lower()
        return normalized_line == normalized_headers

    @staticmethod
    def _tokenize_row(line: str, expected_columns: int) -> List[str]:
        tokens = line.split()
        if len(tokens) < expected_columns:
            tokens.extend([""] * (expected_columns - len(tokens)))
        elif len(tokens) > expected_columns:
            # Merge trailing tokens into the last column.
            tokens = tokens[: expected_columns - 1] + [" ".join(tokens[expected_columns - 1 :])]
        return tokens


def export_tables_to_workbook(
    tables: Dict[str, List[List[str]]],
    output_path: pathlib.Path,
    section_definitions: Dict[str, SectionDefinition] | None = None,
) -> None:
    """Create an XLSX workbook from parsed tables."""

    definitions = section_definitions or SECTION_DEFINITIONS
    workbook = Workbook()
    # Remove default sheet created by openpyxl
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    for section_name, rows in tables.items():
        definition = definitions.get(section_name)
        if definition is None:
            continue

        sheet = workbook.create_sheet(definition.normalized_name)
        sheet.append(list(definition.headers))

        for row in rows:
            sheet.append(row)

        _autosize_columns(sheet)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)


def _autosize_columns(sheet) -> None:
    """Adjust column widths based on the longest cell in each column."""

    for column_cells in sheet.columns:
        max_length = 0
        column_index = column_cells[0].column
        for cell in column_cells:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = max_length + 2
        sheet.column_dimensions[get_column_letter(column_index)].width = adjusted_width


class ConverterGUI:
    """Simple Tk GUI around the raw data parser and XLSX exporter."""

    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("Network Data Converter")

        self.parser = RawNetworkDataParser()

        self.input_path_var = StringVar()
        self.output_path_var = StringVar()

        self._build_layout()

    def _build_layout(self) -> None:
        padding = {"padx": 10, "pady": 10}

        frame = ttk.Frame(self.root)
        frame.grid(column=0, row=0, sticky="nsew")

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="Input TXT file:").grid(column=0, row=0, sticky="w", **padding)
        input_entry = ttk.Entry(frame, textvariable=self.input_path_var, width=50)
        input_entry.grid(column=1, row=0, sticky="ew", **padding)
        ttk.Button(frame, text="Browse", command=self._browse_input).grid(column=2, row=0, **padding)

        ttk.Label(frame, text="Output XLSX file:").grid(column=0, row=1, sticky="w", **padding)
        output_entry = ttk.Entry(frame, textvariable=self.output_path_var, width=50)
        output_entry.grid(column=1, row=1, sticky="ew", **padding)
        ttk.Button(frame, text="Browse", command=self._browse_output).grid(column=2, row=1, **padding)

        convert_button = ttk.Button(frame, text="Convert", command=self._convert)
        convert_button.grid(column=0, row=2, columnspan=3, pady=(20, 10))

    def _browse_input(self) -> None:
        initial_dir = pathlib.Path(self.input_path_var.get() or ".").expanduser()
        file_path = filedialog.askopenfilename(
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialdir=str(initial_dir),
        )
        if file_path:
            self.input_path_var.set(file_path)
            if not self.output_path_var.get():
                suggested = pathlib.Path(file_path).with_suffix(".xlsx")
                self.output_path_var.set(str(suggested))

    def _browse_output(self) -> None:
        initial_file = self.output_path_var.get() or "converted.xlsx"
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")],
            initialfile=initial_file,
        )
        if file_path:
            self.output_path_var.set(file_path)

    def _convert(self) -> None:
        input_path = pathlib.Path(self.input_path_var.get())
        output_path = pathlib.Path(self.output_path_var.get())

        if not input_path.exists():
            messagebox.showerror("Conversion Failed", f"Input file not found: {input_path}")
            return

        if not output_path.suffix:
            output_path = output_path.with_suffix(".xlsx")

        text = input_path.read_text(encoding="utf-8")
        tables = self.parser.parse(text)
        if not tables:
            messagebox.showwarning(
                "No Data Found",
                "The input file did not contain any recognized sections to convert.",
            )
            return

        export_tables_to_workbook(tables, output_path, self.parser.definitions)
        messagebox.showinfo("Conversion Complete", f"Saved workbook to {output_path}")

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    ConverterGUI().run()


if __name__ == "__main__":
    main()
