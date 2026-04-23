#!/usr/bin/env python3
import json
import math
import os
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET


BASE_DIR = Path("/Users/nguyencan/Library/CloudStorage/OneDrive-TARA/Order Haravan")
OUTPUT_FILE = BASE_DIR / "order_report.html"
PRODUCT_MAP_PATH = Path("/Users/nguyencan/Downloads/Copy of list-sp-hien-website.xlsx")
NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def col_to_idx(col: str) -> int:
    value = 0
    for ch in col:
        value = value * 26 + (ord(ch.upper()) - 64)
    return value - 1


def parse_cell_value(cell) -> str:
    value = cell.find("x:v", NS)
    return "" if value is None or value.text is None else value.text


def read_shared_strings(zf):
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    shared = []
    for item in root.findall("x:si", NS):
        shared.append("".join(node.text or "" for node in item.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")))
    return shared


def parse_row(row, shared=None):
    sparse = {}
    for cell in row.findall("x:c", NS):
        ref = cell.attrib.get("r", "")
        match = re.match(r"([A-Z]+)", ref)
        idx = col_to_idx(match.group(1)) if match else len(sparse)
        value = ""
        cell_type = cell.attrib.get("t")
        if cell_type == "s" and shared is not None:
            raw = parse_cell_value(cell)
            value = shared[int(raw)] if raw.isdigit() and int(raw) < len(shared) else raw
        elif cell_type == "inlineStr":
            inline = cell.find("x:is", NS)
            if inline is not None:
                value = "".join(node.text or "" for node in inline.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"))
        else:
            value = parse_cell_value(cell)
        sparse[idx] = value
    if not sparse:
        return []
    max_idx = max(sparse)
    return [sparse.get(i, "") for i in range(max_idx + 1)]


def safe_float(value: str) -> float:
    if value is None:
        return 0.0
    value = str(value).strip().replace(",", "")
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def parse_datetime(value: str):
    if not value:
        return None
    text = value.strip()
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass
    return None


def normalize_channel(value: str) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "unknown"
    replacements = {
        "thu_1_doi_1": "promo",
        "harasocial": "social",
    }
    return replacements.get(text, text)


def derive_group(product_name: str) -> str:
    name = (product_name or "").strip()
    if not name:
        return "Khác"
    for token in (" BlueStone ", " Bluestone ", " BLUESTONE "):
        if token in name:
            prefix = name.split(token, 1)[0].strip()
            return prefix or "Khác"
    match = re.split(r"\s+[A-Z0-9-]{4,}\b", name, maxsplit=1)
    prefix = match[0].strip() if match else name
    words = prefix.split()
    if len(words) >= 5:
        prefix = " ".join(words[:5])
    return prefix or "Khác"


def choose_order_datetime(ordered_at: str, paid_at: str, delivered_at: str):
    return parse_datetime(ordered_at) or parse_datetime(paid_at) or parse_datetime(delivered_at)


def read_product_mapping():
    if not PRODUCT_MAP_PATH.exists():
        return {}

    with zipfile.ZipFile(PRODUCT_MAP_PATH) as zf:
        shared = read_shared_strings(zf)
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        sheet_targets = []
        for sheet in workbook.find("x:sheets", NS):
            rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = rel_map.get(rid, "")
            if target and not target.startswith("xl/"):
                target = "xl/" + target.lstrip("/")
            sheet_targets.append(target)

        if not sheet_targets:
            return {}

        sheet1_root = ET.fromstring(zf.read(sheet_targets[0]))
        sheet1_rows = sheet1_root.findall(".//x:sheetData/x:row", NS)
        product_header = parse_row(sheet1_rows[0], shared)
        product_index = {name: idx for idx, name in enumerate(product_header)}

        def product_get(values, column_name: str) -> str:
            idx = product_index.get(column_name, -1)
            return values[idx] if 0 <= idx < len(values) else ""

        product_map = {}
        for row in sheet1_rows[1:]:
            values = parse_row(row, shared)
            variant_id = product_get(values, "Mã phiên bản sản phẩm").strip()
            if not variant_id:
                continue
            product_map[variant_id] = {
                "variant_id": variant_id,
                "barcode": (product_get(values, "Barcode") or variant_id).strip(),
                "image": (product_get(values, "Link hình") or product_get(values, "Ảnh biến thể")).strip(),
                "website_group": product_get(values, "Loại sản phẩm").strip(),
                "product_url": product_get(values, "Url").strip(),
                "key_summer": product_get(values, "Key Group Summer").strip(),
                "classify": product_get(values, "Classify").strip(),
            }

        sheet2_target = None
        if len(sheet_targets) >= 2:
            sheet2_target = sheet_targets[1]
        elif "xl/worksheets/sheet2.xml" in zf.namelist():
            sheet2_target = "xl/worksheets/sheet2.xml"

        if sheet2_target:
            sheet2_root = ET.fromstring(zf.read(sheet2_target))
            sheet2_rows = sheet2_root.findall(".//x:sheetData/x:row", NS)
        else:
            sheet2_rows = []

        if len(sheet2_rows) >= 3:
            class_header = parse_row(sheet2_rows[2], shared)
            class_index = {name: idx for idx, name in enumerate(class_header)}

            def class_get(values, names):
                for name in names:
                    idx = class_index.get(name, -1)
                    if 0 <= idx < len(values) and values[idx]:
                        return values[idx]
                return ""

            classify_map = {}
            for row in sheet2_rows[4:]:
                values = parse_row(row, shared)
                model = class_get(values, ["MODEL NAME"]).strip()
                if not model:
                    continue
                classify_map[model] = {
                    "key_summer": class_get(values, ["Key Group Summer"]).strip(),
                    "classify": class_get(values, ["BCG Classify", "Classify"]).strip(),
                }

            for meta in product_map.values():
                if meta.get("key_summer") and meta.get("classify"):
                    continue
                classify_meta = classify_map.get(meta["barcode"], {})
                meta["key_summer"] = meta.get("key_summer") or classify_meta.get("key_summer", "")
                meta["classify"] = meta.get("classify") or classify_meta.get("classify", "")

        return product_map


def read_xlsx_records(path: Path, product_map):
    with zipfile.ZipFile(path) as zf:
        root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        rows = root.findall(".//x:sheetData/x:row", NS)
        if not rows:
            return []
        header = parse_row(rows[0])
        index = {name: idx for idx, name in enumerate(header)}
        records = []
        for row in rows[1:]:
            values = parse_row(row)
            def get(column_name: str) -> str:
                idx = index.get(column_name, -1)
                return values[idx] if 0 <= idx < len(values) else ""

            ordered_at = get("Ngày đặt hàng")
            paid_at = get("Thời gian thanh toán")
            delivered_at = get("Thời gian giao hàng")
            dt = choose_order_datetime(ordered_at, paid_at, delivered_at)
            if not dt:
                continue

            quantity = safe_float(get("Số lượng sản phẩm")) or 1.0
            unit_price = safe_float(get("Giá sản phẩm"))
            compare_price = safe_float(get("Giá so sánh sản phẩm"))
            discount = safe_float(get("Số tiền giảm"))
            order_total = safe_float(get("Tổng cộng"))
            shipping_fee = safe_float(get("Phí vận chuyển"))

            line_revenue = unit_price * quantity
            if line_revenue <= 0 and compare_price > 0:
                line_revenue = compare_price * quantity
            if line_revenue <= 0 and order_total > 0:
                line_revenue = order_total
            if line_revenue > 0 and discount > 0:
                line_revenue = max(0.0, line_revenue - discount)

            variant_id = (get("Mã sản phẩm") or get("Id") or "").strip()
            product_name = (get("Tên sản phẩm") or "Không rõ sản phẩm").strip()
            channel = normalize_channel(get("Kênh bán hàng"))
            cancel_status = (get("Trạng thái hủy") or "No").strip()
            product_meta = product_map.get(variant_id, {})
            display_barcode = product_meta.get("barcode") or variant_id or product_name
            group_name = product_meta.get("website_group") or derive_group(product_name)
            key_summer = product_meta.get("key_summer") or "Khác"
            classify = product_meta.get("classify") or "Chưa phân loại"

            records.append(
                {
                    "d": dt.strftime("%Y-%m-%d"),
                    "m": dt.strftime("%Y-%m"),
                    "c": channel,
                    "x": cancel_status,
                    "q": quantity,
                    "r": round(line_revenue, 2),
                    "s": display_barcode,
                    "v": variant_id,
                    "p": product_name,
                    "g": group_name,
                    "i": product_meta.get("image", ""),
                    "ks": key_summer,
                    "cl": classify,
                    "o": (get("Mã đơn hàng") or "").strip(),
                    "pm": (get("Phương thức thanh toán") or "").strip(),
                    "ship": shipping_fee,
                }
            )
        return records


def build_dataset():
    product_map = read_product_mapping()
    records = []
    for path in sorted(BASE_DIR.glob("Orders_T*_20*.xlsx")):
        records.extend(read_xlsx_records(path, product_map))

    if not records:
        raise SystemExit("No order files found.")

    dates = sorted({item["d"] for item in records})
    channels = sorted({item["c"] for item in records})
    groups = sorted({item["g"] for item in records})
    skus = sorted({item["s"] for item in records})
    key_summers = sorted({item["ks"] for item in records})
    classifies = sorted({item["cl"] for item in records})

    max_date = datetime.fromisoformat(dates[-1]).date()
    default_from = max(datetime.fromisoformat(dates[0]).date(), max_date - timedelta(days=59))

    return {
        "records": records,
        "meta": {
            "generatedAt": datetime.now().isoformat(timespec="seconds"),
            "minDate": dates[0],
            "maxDate": dates[-1],
            "defaultFrom": default_from.isoformat(),
            "defaultTo": dates[-1],
            "channels": channels,
            "groups": groups,
            "skus": skus,
            "keySummers": key_summers,
            "classifies": classifies,
            "recordCount": len(records),
            "hasProductMap": bool(product_map),
        },
    }


HTML_TEMPLATE = r"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Order Haravan Report</title>
  <style>
    :root {
      --bg: #f3f7fb;
      --card: #ffffff;
      --ink: #16324f;
      --muted: #60758d;
      --line: #dbe6f0;
      --blue: #4d89e8;
      --blue-soft: #d9e9ff;
      --orange: #f39a3f;
      --teal: #2aa6b8;
      --green: #1a9b5f;
      --red: #db4d4d;
      --shadow: 0 10px 30px rgba(15, 55, 95, 0.08);
      --radius: 18px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(77,137,232,0.16), transparent 22%),
        radial-gradient(circle at top right, rgba(243,154,63,0.12), transparent 18%),
        linear-gradient(180deg, #f8fbff 0%, #f2f6fb 100%);
    }
    .page {
      max-width: 1500px;
      margin: 0 auto;
      padding: 18px 18px 60px;
    }
    .hero {
      background: linear-gradient(135deg, #ffffff, #edf5ff);
      border: 1px solid rgba(77,137,232,0.15);
      border-radius: 26px;
      box-shadow: var(--shadow);
      padding: 22px;
      margin-bottom: 18px;
    }
    .hero h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
    }
    .hero p {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .filters {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .filter-card, .panel, .metric {
      background: var(--card);
      border: 1px solid rgba(23, 71, 115, 0.08);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }
    .filter-card {
      padding: 12px 14px;
      min-height: 80px;
    }
    .filter-card label {
      display: block;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .04em;
      color: var(--muted);
      margin-bottom: 8px;
    }
    .filter-card input, .filter-card select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 14px;
      color: var(--ink);
      background: #f9fbfd;
    }
    .filter-hint {
      margin-top: 7px;
      font-size: 11px;
      color: var(--muted);
    }
    .multi-select {
      position: relative;
    }
    .multi-select-trigger {
      width: 100%;
      min-height: 44px;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 14px;
      color: var(--ink);
      background: #f9fbfd;
      text-align: left;
      cursor: pointer;
    }
    .multi-select-menu {
      position: absolute;
      top: calc(100% + 8px);
      left: 0;
      right: 0;
      z-index: 20;
      display: none;
      background: white;
      border: 1px solid #dbe6f0;
      border-radius: 16px;
      box-shadow: 0 18px 40px rgba(15, 55, 95, 0.16);
      padding: 12px;
    }
    .multi-select.open .multi-select-menu {
      display: block;
    }
    .multi-select-search {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 9px 10px;
      font-size: 13px;
      margin-bottom: 8px;
      background: #fbfdff;
    }
    .multi-select-actions {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 8px;
    }
    .multi-select-actions button {
      border: none;
      background: transparent;
      color: var(--blue);
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      padding: 0;
    }
    .multi-select-options {
      max-height: 220px;
      overflow: auto;
      display: grid;
      gap: 6px;
    }
    .multi-select-option {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 8px 10px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 13px;
    }
    .multi-select-option:hover {
      background: #f4f8fd;
    }
    .multi-select-option input {
      width: auto;
      margin: 0;
    }
    .selected-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 8px;
      min-height: 24px;
    }
    .selected-chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 8px;
      border-radius: 999px;
      background: #eef5fd;
      color: var(--ink);
      font-size: 11px;
      font-weight: 700;
    }
    .selected-chip button {
      border: none;
      background: transparent;
      color: var(--muted);
      cursor: pointer;
      padding: 0;
      font-size: 12px;
      line-height: 1;
    }
    .summary-stat {
      background: #f8fbff;
      border: 1px solid #edf3f9;
      border-radius: 12px;
      padding: 10px 12px;
    }
    .summary-stat-label {
      font-size: 11px;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: .03em;
    }
    .summary-stat-value {
      margin-top: 4px;
      font-size: 20px;
      font-weight: 800;
      line-height: 1.05;
    }
    .summary-total {
      margin-top: 12px;
      padding: 14px;
      border-radius: 16px;
      background: linear-gradient(180deg, #edf5ff, #f8fbff);
      border: 1px solid #dde9f5;
    }
    .summary-total-grid {
      display: grid;
      grid-template-columns: 1.4fr repeat(3, minmax(0, 1fr));
      gap: 12px;
      align-items: center;
    }
    .summary-total-label {
      font-size: 16px;
      font-weight: 800;
    }
    .breakdown-grid {
      display: grid;
      grid-template-columns: minmax(250px, 0.8fr) minmax(0, 1.7fr);
      gap: 16px;
      margin-bottom: 18px;
    }
    .section-title {
      margin: 18px 0 12px;
      font-size: 18px;
      font-weight: 800;
      text-decoration: underline;
      text-underline-offset: 4px;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }
    .metric {
      padding: 14px 16px;
      background: linear-gradient(180deg, #eef6ff, #dcecff);
      border-color: rgba(77,137,232,0.25);
    }
    .metric .label {
      color: var(--muted);
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: .03em;
    }
    .metric .value {
      margin-top: 4px;
      font-size: 34px;
      font-weight: 800;
      line-height: 1;
    }
    .metric .delta {
      margin-top: 8px;
      font-size: 14px;
      font-weight: 700;
    }
    .delta.up { color: var(--green); }
    .delta.down { color: var(--red); }
    .delta.flat { color: var(--muted); }
    .grid-2 {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 16px;
      margin-bottom: 18px;
    }
    .grid-3 {
      display: grid;
      grid-template-columns: 1.25fr 1fr 1fr;
      gap: 16px;
      margin-bottom: 18px;
    }
    .panel {
      padding: 16px;
      overflow: hidden;
    }
    .panel h3 {
      margin: 0 0 14px;
      font-size: 20px;
    }
    .panel-subtitle {
      font-size: 13px;
      color: var(--muted);
      margin: -6px 0 14px;
    }
    .panel-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: flex-start;
      justify-content: space-between;
      margin-bottom: 14px;
    }
    .toolbar-group {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: flex-start;
      flex: 1 1 auto;
    }
    .toolbar-item {
      min-width: 220px;
      flex: 1 1 220px;
    }
    .toolbar-label {
      display: block;
      font-size: 11px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .03em;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .download-btn, .lang-select {
      border: 1px solid #dbe6f0;
      background: #f8fbff;
      color: var(--ink);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 13px;
      font-weight: 800;
      cursor: pointer;
    }
    .download-btn {
      white-space: nowrap;
    }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .panel-head h3 {
      margin: 0;
    }
    .viz-list {
      display: grid;
      gap: 12px;
    }
    .viz-row {
      display: grid;
      grid-template-columns: minmax(120px, 1.2fr) 3fr auto;
      gap: 12px;
      align-items: center;
    }
    .viz-name {
      font-size: 14px;
      font-weight: 700;
      line-height: 1.2;
    }
    .viz-bar {
      position: relative;
      height: 14px;
      border-radius: 999px;
      overflow: hidden;
      background: #edf3f9;
    }
    .viz-fill {
      position: absolute;
      inset: 0 auto 0 0;
      border-radius: inherit;
    }
    .viz-meta {
      text-align: right;
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
      white-space: nowrap;
    }
    .chart-shell {
      width: 100%;
      overflow-x: auto;
    }
    svg {
      display: block;
      width: 100%;
      height: auto;
    }
    .legend {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin-bottom: 10px;
      font-size: 14px;
      font-weight: 700;
    }
    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .legend i {
      display: inline-block;
      width: 14px;
      height: 14px;
      border-radius: 4px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      padding: 10px 10px;
      border-bottom: 1px solid #edf2f7;
      text-align: left;
      vertical-align: middle;
    }
    thead th {
      background: #eef5fd;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .03em;
    }
    .table-scroll {
      max-height: 720px;
      overflow: auto;
      border: 1px solid #edf2f7;
      border-radius: 14px;
      background: white;
    }
    .table-scroll table {
      margin: 0;
    }
    .table-scroll thead th {
      position: sticky;
      top: 0;
      z-index: 2;
      box-shadow: 0 1px 0 #e5edf6;
    }
    tbody tr:nth-child(even) {
      background: #fafcff;
    }
    .compact-table {
      width: 100%;
      table-layout: fixed;
    }
    .compact-table td, .compact-table th {
      padding: 10px 8px;
    }
    .sort-btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: none;
      background: transparent;
      color: inherit;
      font: inherit;
      font-weight: 800;
      cursor: pointer;
      padding: 0;
      text-transform: inherit;
      letter-spacing: inherit;
    }
    .sort-indicator {
      font-size: 11px;
      color: var(--muted);
    }
    .metric-cell {
      display: grid;
      gap: 3px;
    }
    .metric-main {
      font-size: 15px;
      font-weight: 800;
      line-height: 1.1;
    }
    .share-badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      background: #eef5fd;
      font-size: 11px;
      font-weight: 800;
      color: var(--ink);
    }
    .rank {
      color: var(--muted);
      width: 42px;
    }
    .sku-pill {
      display: inline-block;
      padding: 5px 8px;
      border-radius: 999px;
      background: #eef5fd;
      color: var(--ink);
      font-size: 12px;
      font-weight: 700;
    }
    .sku-thumb {
      width: 64px;
      height: 64px;
      object-fit: contain;
      border-radius: 12px;
      border: 1px solid #e5edf6;
      background: white;
      padding: 4px;
    }
    .sku-meta {
      display: grid;
      gap: 4px;
    }
    .sku-code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
      font-weight: 800;
      word-break: break-all;
    }
    .subtle {
      color: var(--muted);
      font-size: 11px;
    }
    .note {
      margin-top: 14px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }
    .empty {
      padding: 30px 0;
      text-align: center;
      color: var(--muted);
    }
    @media (max-width: 1200px) {
      .filters, .metrics, .grid-2, .grid-3, .breakdown-grid {
        grid-template-columns: 1fr 1fr;
      }
    }
    @media (max-width: 760px) {
      .filters, .metrics, .grid-2, .grid-3, .breakdown-grid, .summary-total-grid {
        grid-template-columns: 1fr;
      }
      .hero h1 { font-size: 24px; }
      .metric .value { font-size: 28px; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1 id="heroTitle">Order Haravan Report</h1>
      <p id="heroMeta"></p>
    </section>

    <section class="filters">
      <div class="filter-card">
        <label for="fromDate" data-i18n="fromDate">Từ ngày</label>
        <input id="fromDate" type="date" />
      </div>
      <div class="filter-card">
        <label for="toDate" data-i18n="toDate">Đến ngày</label>
        <input id="toDate" type="date" />
      </div>
      <div class="filter-card">
        <label for="channelFilter" data-i18n="channel">Kênh bán hàng</label>
        <div class="multi-select" id="channelFilter">
          <button type="button" class="multi-select-trigger" id="channelTrigger">Tất cả kênh</button>
          <div class="multi-select-menu">
            <input class="multi-select-search" id="channelSearch" type="text" placeholder="Tìm kênh..." />
            <div class="multi-select-actions">
              <button type="button" data-filter="channel" data-action="all">Chọn tất cả</button>
              <button type="button" data-filter="channel" data-action="clear">Bỏ chọn</button>
            </div>
            <div class="multi-select-options" id="channelOptions"></div>
          </div>
        </div>
        <div class="selected-chips" id="channelChips"></div>
        <div class="filter-hint" data-i18n="channelHint">Bấm chọn nhiều kênh cùng lúc</div>
      </div>
      <div class="filter-card">
        <label for="groupFilter" data-i18n="group">Group</label>
        <div class="multi-select" id="groupFilter">
          <button type="button" class="multi-select-trigger" id="groupTrigger">Tất cả group</button>
          <div class="multi-select-menu">
            <input class="multi-select-search" id="groupSearch" type="text" placeholder="Tìm group..." />
            <div class="multi-select-actions">
              <button type="button" data-filter="group" data-action="all">Chọn tất cả</button>
              <button type="button" data-filter="group" data-action="clear">Bỏ chọn</button>
            </div>
            <div class="multi-select-options" id="groupOptions"></div>
          </div>
        </div>
        <div class="selected-chips" id="groupChips"></div>
        <div class="filter-hint" data-i18n="groupHint">Bấm chọn nhiều group cùng lúc</div>
      </div>
      <div class="filter-card">
        <label for="skuFilter" data-i18n="barcodeSku">Barcode / SKU</label>
        <div class="multi-select" id="skuFilter">
          <button type="button" class="multi-select-trigger" id="skuTrigger">Tất cả SKU</button>
          <div class="multi-select-menu">
            <input class="multi-select-search" id="skuSearch" type="text" placeholder="Tìm SKU / barcode..." />
            <div class="multi-select-actions">
              <button type="button" data-filter="sku" data-action="all">Chọn tất cả</button>
              <button type="button" data-filter="sku" data-action="clear">Bỏ chọn</button>
            </div>
            <div class="multi-select-options" id="skuOptions"></div>
          </div>
        </div>
        <div class="selected-chips" id="skuChips"></div>
        <div class="filter-hint" data-i18n="skuHint">Tìm và tick nhiều SKU</div>
      </div>
      <div class="filter-card">
        <label for="keySummerFilter" data-i18n="keySummer">Key Summer</label>
        <div class="multi-select" id="keySummerFilter">
          <button type="button" class="multi-select-trigger" id="keySummerTrigger">Tất cả key summer</button>
          <div class="multi-select-menu">
            <input class="multi-select-search" id="keySummerSearch" type="text" placeholder="Tìm key summer..." />
            <div class="multi-select-actions">
              <button type="button" data-filter="keySummer" data-action="all">Chọn tất cả</button>
              <button type="button" data-filter="keySummer" data-action="clear">Bỏ chọn</button>
            </div>
            <div class="multi-select-options" id="keySummerOptions"></div>
          </div>
        </div>
        <div class="selected-chips" id="keySummerChips"></div>
        <div class="filter-hint" data-i18n="keySummerHint">Bấm chọn nhiều nhóm key summer</div>
      </div>
      <div class="filter-card">
        <label for="classifyFilter" data-i18n="classify">Classify</label>
        <div class="multi-select" id="classifyFilter">
          <button type="button" class="multi-select-trigger" id="classifyTrigger">Tất cả classify</button>
          <div class="multi-select-menu">
            <input class="multi-select-search" id="classifySearch" type="text" placeholder="Tìm classify..." />
            <div class="multi-select-actions">
              <button type="button" data-filter="classify" data-action="all">Chọn tất cả</button>
              <button type="button" data-filter="classify" data-action="clear">Bỏ chọn</button>
            </div>
            <div class="multi-select-options" id="classifyOptions"></div>
          </div>
        </div>
        <div class="selected-chips" id="classifyChips"></div>
        <div class="filter-hint" data-i18n="classifyHint">Bấm chọn nhiều classify</div>
      </div>
      <div class="filter-card">
        <label for="cancelFilter" data-i18n="cancelStatus">Trạng thái hủy</label>
        <select id="cancelFilter">
          <option value="all">Tất cả</option>
          <option value="No">No</option>
          <option value="Yes">Yes</option>
        </select>
      </div>
      <div class="filter-card">
        <label for="langToggle" data-i18n="language">Ngôn ngữ</label>
        <select id="langToggle" class="lang-select">
          <option value="vi">Tiếng Việt</option>
          <option value="en">English</option>
        </select>
      </div>
    </section>

    <h2 class="section-title" data-i18n="sectionTotal">1. Total DT / Volume</h2>
    <section class="metrics">
      <article class="metric">
        <div class="label">DT</div>
        <div class="value" id="metricRevenue">-</div>
        <div class="delta" id="metricRevenueDelta">-</div>
      </article>
      <article class="metric">
        <div class="label">Volume</div>
        <div class="value" id="metricVolume">-</div>
        <div class="delta" id="metricVolumeDelta">-</div>
      </article>
      <article class="metric">
        <div class="label">ASP</div>
        <div class="value" id="metricAsp">-</div>
        <div class="delta" id="metricAspDelta">-</div>
      </article>
      <article class="metric">
        <div class="label">% Hủy</div>
        <div class="value" id="metricCancel">-</div>
        <div class="delta" id="metricCancelDelta">-</div>
      </article>
    </section>

    <section class="panel">
      <h3 data-i18n="dailyTrend">DT và Volume theo ngày</h3>
      <div class="legend">
        <span><i style="background:#4d89e8"></i> DT</span>
        <span><i style="background:#f39a3f"></i> Số lượng sản phẩm</span>
      </div>
      <div class="chart-shell" id="trendChart"></div>
    </section>

    <h2 class="section-title" data-i18n="sectionChannel">2. Performance by Channel</h2>
    <section class="breakdown-grid">
      <article class="panel">
        <h3 data-i18n="channelShare">Tỷ trọng doanh thu theo kênh</h3>
        <div class="panel-subtitle" data-i18n="channelShareSub">Top kênh đóng góp doanh thu trong kỳ đang chọn</div>
        <div id="channelViz"></div>
      </article>
      <article class="panel">
        <h3 data-i18n="channelSummary">Tổng hợp kênh bán</h3>
        <div id="channelTable"></div>
      </article>
    </section>

    <h2 class="section-title" data-i18n="sectionGroup">3. Performance by Group</h2>
    <section class="breakdown-grid">
      <article class="panel">
        <h3 data-i18n="groupShare">Tỷ trọng doanh thu theo group</h3>
        <div class="panel-subtitle" data-i18n="groupShareSub">Top group suy ra từ tên sản phẩm</div>
        <div id="groupViz"></div>
      </article>
      <article class="panel">
        <div class="panel-head">
          <h3 data-i18n="groupSummary">Tổng hợp group</h3>
          <button id="downloadGroupData" class="download-btn" data-i18n="downloadGroup">Tải group data</button>
        </div>
        <div id="groupTable"></div>
      </article>
    </section>

    <h2 class="section-title" data-i18n="sectionKeyClassify">4. Performance by Key Summer / Classify</h2>
    <section class="breakdown-grid">
      <article class="panel">
        <h3 data-i18n="keySummerTitle">Key Group Summer</h3>
        <div class="panel-subtitle" data-i18n="keySummerSub">Phân tích theo mapping từ file website</div>
        <div id="keySummerViz"></div>
      </article>
      <article class="panel">
        <h3 data-i18n="keySummerSummary">Tổng hợp Key Group Summer</h3>
        <div id="keySummerTable"></div>
      </article>
    </section>

    <section class="breakdown-grid">
      <article class="panel">
        <h3 data-i18n="classifyTitle">Classify</h3>
        <div class="panel-subtitle" data-i18n="classifySub">Phân tích theo BCG/Classify</div>
        <div id="classifyViz"></div>
      </article>
      <article class="panel">
        <h3 data-i18n="classifySummary">Tổng hợp Classify</h3>
        <div id="classifyTable"></div>
      </article>
    </section>

    <h2 class="section-title" data-i18n="sectionSku">5. Performance by SKU</h2>
    <section class="panel">
      <div class="panel-toolbar">
        <div class="toolbar-group">
          <div class="toolbar-item">
            <label class="toolbar-label" data-i18n="detailChannel">Detail filter - Kênh bán</label>
            <div class="multi-select" id="skuDetailChannelFilter">
              <button type="button" class="multi-select-trigger" id="skuDetailChannelTrigger">Tất cả kênh</button>
              <div class="multi-select-menu">
                <input class="multi-select-search" id="skuDetailChannelSearch" type="text" placeholder="Tìm kênh..." />
                <div class="multi-select-actions">
                  <button type="button" data-filter="skuDetailChannel" data-action="all">Chọn tất cả</button>
                  <button type="button" data-filter="skuDetailChannel" data-action="clear">Bỏ chọn</button>
                </div>
                <div class="multi-select-options" id="skuDetailChannelOptions"></div>
              </div>
            </div>
          </div>
          <div class="toolbar-item">
            <label class="toolbar-label" data-i18n="detailGroup">Detail filter - Group</label>
            <div class="multi-select" id="skuDetailGroupFilter">
              <button type="button" class="multi-select-trigger" id="skuDetailGroupTrigger">Tất cả group</button>
              <div class="multi-select-menu">
                <input class="multi-select-search" id="skuDetailGroupSearch" type="text" placeholder="Tìm group..." />
                <div class="multi-select-actions">
                  <button type="button" data-filter="skuDetailGroup" data-action="all">Chọn tất cả</button>
                  <button type="button" data-filter="skuDetailGroup" data-action="clear">Bỏ chọn</button>
                </div>
                <div class="multi-select-options" id="skuDetailGroupOptions"></div>
              </div>
            </div>
          </div>
        </div>
        <button id="downloadSkuData" class="download-btn" data-i18n="downloadData">Tải data</button>
      </div>
      <div class="selected-chips" id="skuDetailChannelChips"></div>
      <div class="selected-chips" id="skuDetailGroupChips"></div>
      <h3 data-i18n="topSkuRevenue">Top SKU theo DT</h3>
      <div id="skuTable"></div>
      <div class="note">
        Nguồn dữ liệu: export Haravan trong thư mục local + mapping từ file website. `DT` đang được tính theo doanh thu line-item (`Giá sản phẩm x Số lượng`), để tránh nhân đôi `Tổng cộng` ở các đơn có nhiều sản phẩm.
        `Barcode`, `Link hình`, `Key Group Summer`, `Classify` được join từ file `/Users/nguyencan/Downloads/Copy of list-sp-hien-website.xlsx`. So sánh `% Δ` dùng kỳ trước có cùng số ngày với kỳ đang chọn.
      </div>
    </section>
  </div>

  <script>
    const REPORT_DATA = __DATA__;

    const PALETTE = [
      "#4d89e8", "#f39a3f", "#9d73db", "#a9bf52", "#2aa6b8",
      "#de72aa", "#dcb774", "#59b0db", "#f0c635", "#7d8940",
      "#a67a63", "#9aa9c9"
    ];

    const state = {
      from: REPORT_DATA.meta.defaultFrom,
      to: REPORT_DATA.meta.defaultTo,
      channel: [],
      group: [],
      sku: [],
      keySummer: [],
      classify: [],
      skuDetailChannel: [],
      skuDetailGroup: [],
      cancel: "all",
      lang: "vi",
      summarySort: {
        channel: { key: "revenue", dir: "desc" },
        group: { key: "revenue", dir: "desc" },
        keySummer: { key: "revenue", dir: "desc" },
        classify: { key: "revenue", dir: "desc" },
      },
      skuSort: { key: "revenue", dir: "desc" },
    };

    const multiSelectConfig = {
      channel: { values: REPORT_DATA.meta.channels, label: "kênh", allLabel: "Tất cả kênh", enLabel: "channels", enAllLabel: "All channels" },
      group: { values: REPORT_DATA.meta.groups, label: "group", allLabel: "Tất cả group", enLabel: "groups", enAllLabel: "All groups" },
      sku: { values: REPORT_DATA.meta.skus, label: "SKU", allLabel: "Tất cả SKU", enLabel: "SKUs", enAllLabel: "All SKUs" },
      keySummer: { values: REPORT_DATA.meta.keySummers, label: "key summer", allLabel: "Tất cả key summer", enLabel: "key summer", enAllLabel: "All key summer" },
      classify: { values: REPORT_DATA.meta.classifies, label: "classify", allLabel: "Tất cả classify", enLabel: "classify", enAllLabel: "All classify" },
      skuDetailChannel: { values: REPORT_DATA.meta.channels, label: "kênh", allLabel: "Tất cả kênh", enLabel: "channels", enAllLabel: "All channels" },
      skuDetailGroup: { values: REPORT_DATA.meta.groups, label: "group", allLabel: "Tất cả group", enLabel: "groups", enAllLabel: "All groups" },
    };

    const I18N = {
      vi: {
        heroTitle: "Order Haravan Report",
        fromDate: "Từ ngày",
        toDate: "Đến ngày",
        channel: "Kênh bán hàng",
        group: "Group",
        barcodeSku: "Barcode / SKU",
        keySummer: "Key Summer",
        classify: "Classify",
        cancelStatus: "Trạng thái hủy",
        language: "Ngôn ngữ",
        channelHint: "Bấm chọn nhiều kênh cùng lúc",
        groupHint: "Bấm chọn nhiều group cùng lúc",
        skuHint: "Tìm và tick nhiều SKU",
        keySummerHint: "Bấm chọn nhiều nhóm key summer",
        classifyHint: "Bấm chọn nhiều classify",
        sectionTotal: "1. Total DT / Volume",
        dailyTrend: "DT và Volume theo ngày",
        sectionChannel: "2. Performance by Channel",
        channelShare: "Tỷ trọng doanh thu theo kênh",
        channelShareSub: "Top kênh đóng góp doanh thu trong kỳ đang chọn",
        channelSummary: "Tổng hợp kênh bán",
        sectionGroup: "3. Performance by Group",
        groupShare: "Tỷ trọng doanh thu theo group",
        groupShareSub: "Top group suy ra từ tên sản phẩm",
        groupSummary: "Tổng hợp group",
        sectionKeyClassify: "4. Performance by Key Summer / Classify",
        keySummerTitle: "Key Group Summer",
        keySummerSub: "Phân tích theo mapping từ file website",
        keySummerSummary: "Tổng hợp Key Group Summer",
        classifyTitle: "Classify",
        classifySub: "Phân tích theo BCG/Classify",
        classifySummary: "Tổng hợp Classify",
        sectionSku: "5. Performance by SKU",
        detailChannel: "Detail filter - Kênh bán",
        detailGroup: "Detail filter - Group",
        downloadData: "Tải data",
        downloadGroup: "Tải group data",
        topSkuRevenue: "Top SKU theo DT",
        revenue: "DT",
        volume: "Volume",
        asp: "ASP",
        total: "Tổng cộng",
        noData: "Không có dữ liệu.",
        image: "Ảnh",
        productName: "Tên sản phẩm",
        variant: "Variant",
        downloadFileNameSku: "sku_detail_export",
        downloadFileNameGroup: "group_performance_export"
      },
      en: {
        heroTitle: "Order Haravan Report",
        fromDate: "From date",
        toDate: "To date",
        channel: "Sales channel",
        group: "Group",
        barcodeSku: "Barcode / SKU",
        keySummer: "Key Summer",
        classify: "Classify",
        cancelStatus: "Cancel status",
        language: "Language",
        channelHint: "Select multiple channels",
        groupHint: "Select multiple groups",
        skuHint: "Search and tick multiple SKUs",
        keySummerHint: "Select multiple key summer groups",
        classifyHint: "Select multiple classify values",
        sectionTotal: "1. Total Revenue / Volume",
        dailyTrend: "Revenue and Volume by day",
        sectionChannel: "2. Performance by Channel",
        channelShare: "Revenue share by channel",
        channelShareSub: "Top revenue-contributing channels in current period",
        channelSummary: "Channel summary",
        sectionGroup: "3. Performance by Group",
        groupShare: "Revenue share by group",
        groupShareSub: "Top groups derived from product names",
        groupSummary: "Group summary",
        sectionKeyClassify: "4. Performance by Key Summer / Classify",
        keySummerTitle: "Key Group Summer",
        keySummerSub: "Analysis based on website mapping",
        keySummerSummary: "Key Group Summer summary",
        classifyTitle: "Classify",
        classifySub: "Analysis by BCG/Classify",
        classifySummary: "Classify summary",
        sectionSku: "5. Performance by SKU",
        detailChannel: "Detail filter - Channel",
        detailGroup: "Detail filter - Group",
        downloadData: "Download data",
        downloadGroup: "Download group data",
        topSkuRevenue: "Top SKU by revenue",
        revenue: "Revenue",
        volume: "Volume",
        asp: "ASP",
        total: "Total",
        noData: "No data.",
        image: "Image",
        productName: "Product name",
        variant: "Variant",
        downloadFileNameSku: "sku_detail_export",
        downloadFileNameGroup: "group_performance_export"
      }
    };

    function setup() {
      document.getElementById("heroMeta").textContent =
        `Generated ${formatDateTime(REPORT_DATA.meta.generatedAt)} | ${REPORT_DATA.meta.recordCount.toLocaleString("en-US")} line items | ${REPORT_DATA.meta.minDate} -> ${REPORT_DATA.meta.maxDate}`;

      bindFilter("fromDate", REPORT_DATA.meta.defaultFrom);
      bindFilter("toDate", REPORT_DATA.meta.defaultTo);
      setupMultiSelect("channel");
      setupMultiSelect("group");
      setupMultiSelect("sku");
      setupMultiSelect("keySummer");
      setupMultiSelect("classify");
      setupMultiSelect("skuDetailChannel");
      setupMultiSelect("skuDetailGroup");

      document.getElementById("fromDate").addEventListener("change", (e) => { state.from = e.target.value; render(); });
      document.getElementById("toDate").addEventListener("change", (e) => { state.to = e.target.value; render(); });
      document.getElementById("cancelFilter").addEventListener("change", (e) => { state.cancel = e.target.value; render(); });
      document.getElementById("langToggle").addEventListener("change", (e) => { state.lang = e.target.value; applyI18n(); render(); });
      document.getElementById("downloadSkuData").addEventListener("click", downloadSkuData);
      document.getElementById("downloadGroupData").addEventListener("click", downloadGroupData);
      document.addEventListener("click", handleOutsideClick);

      applyI18n();
      render();
    }

    function applyI18n() {
      const dict = I18N[state.lang] || I18N.vi;
      document.getElementById("heroTitle").textContent = dict.heroTitle;
      document.querySelectorAll("[data-i18n]").forEach(el => {
        const key = el.dataset.i18n;
        if (dict[key]) el.textContent = dict[key];
      });
      document.getElementById("downloadSkuData").textContent = dict.downloadData;
      const groupDownload = document.getElementById("downloadGroupData");
      if (groupDownload) groupDownload.textContent = dict.downloadGroup;
      const cancel = document.getElementById("cancelFilter");
      if (cancel) {
        cancel.options[0].text = state.lang === "en" ? "All" : "Tất cả";
        cancel.options[1].text = "No";
        cancel.options[2].text = "Yes";
      }
      Object.keys(multiSelectConfig).forEach(name => renderMultiSelect(name));
    }

    function t(key) {
      const dict = I18N[state.lang] || I18N.vi;
      return dict[key] || key;
    }

    function bindFilter(id, value) {
      const el = document.getElementById(id);
      el.value = value;
    }

    function setupMultiSelect(name) {
      const cfg = multiSelectConfig[name];
      const root = document.getElementById(`${name}Filter`);
      const trigger = document.getElementById(`${name}Trigger`);
      const search = document.getElementById(`${name}Search`);

      trigger.addEventListener("click", () => {
        document.querySelectorAll(".multi-select.open").forEach(el => {
          if (el !== root) el.classList.remove("open");
        });
        root.classList.toggle("open");
        if (root.classList.contains("open")) search.focus();
      });

      search.addEventListener("input", () => renderMultiSelectOptions(name));
      root.querySelectorAll("[data-action]").forEach(btn => {
        btn.addEventListener("click", () => {
          if (btn.dataset.action === "all") {
            state[name] = [...cfg.values];
          } else {
            state[name] = [];
          }
          renderMultiSelect(name);
          render();
        });
      });

      renderMultiSelect(name);
    }

    function handleOutsideClick(event) {
      document.querySelectorAll(".multi-select.open").forEach(root => {
        if (!root.contains(event.target)) {
          root.classList.remove("open");
        }
      });
    }

    function renderMultiSelect(name) {
      renderMultiSelectOptions(name);
      const cfg = multiSelectConfig[name];
      const selected = state[name];
      const trigger = document.getElementById(`${name}Trigger`);
      const chips = document.getElementById(`${name}Chips`);
      if (!selected.length) {
        trigger.textContent = state.lang === "en" ? cfg.enAllLabel : cfg.allLabel;
        chips.innerHTML = "";
        return;
      }
      if (selected.length <= 2) {
        trigger.textContent = selected.join(", ");
      } else {
        trigger.textContent = state.lang === "en"
          ? `${selected.length} ${cfg.enLabel} selected`
          : `${selected.length} ${cfg.label} đã chọn`;
      }
      chips.innerHTML = selected.slice(0, 6).map(value =>
        `<span class="selected-chip">${escapeHtml(value)}<button type="button" data-filter="${name}" data-value="${escapeAttr(value)}">×</button></span>`
      ).join("");
      chips.querySelectorAll("button").forEach(btn => {
        btn.addEventListener("click", () => {
          state[name] = state[name].filter(item => item !== btn.dataset.value);
          renderMultiSelect(name);
          render();
        });
      });
    }

    function renderMultiSelectOptions(name) {
      const cfg = multiSelectConfig[name];
      const search = document.getElementById(`${name}Search`);
      const holder = document.getElementById(`${name}Options`);
      const keyword = normalizeText(search.value);
      const filtered = cfg.values.filter(value => !keyword || normalizeText(value).includes(keyword)).slice(0, 200);
      holder.innerHTML = filtered.map(value => `
        <label class="multi-select-option">
          <input type="checkbox" ${state[name].includes(value) ? "checked" : ""} data-filter="${name}" data-value="${escapeAttr(value)}" />
          <span>${escapeHtml(value)}</span>
        </label>
      `).join("") || `<div class="empty">${t("noData")}</div>`;
      holder.querySelectorAll("input[type='checkbox']").forEach(input => {
        input.addEventListener("change", () => {
          toggleMultiValue(name, input.dataset.value, input.checked);
        });
      });
    }

    function toggleMultiValue(name, value, checked) {
      const current = new Set(state[name]);
      if (checked) current.add(value);
      else current.delete(value);
      state[name] = [...current];
      renderMultiSelect(name);
      render();
    }

    function formatDateTime(iso) {
      return new Date(iso).toLocaleString("vi-VN");
    }

    function escapeHtml(text) {
      return String(text)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function escapeAttr(text) {
      return escapeHtml(text).replaceAll("'", "&#39;");
    }

    function formatCompactCurrency(num) {
      const value = Number(num || 0);
      if (Math.abs(value) >= 1e9) return `${formatNumber(value / 1e9)} T`;
      if (Math.abs(value) >= 1e6) return `${formatNumber(value / 1e6)} Tr`;
      if (Math.abs(value) >= 1e3) return `${formatNumber(value / 1e3)} N`;
      return formatNumber(value);
    }

    function formatCompactUnits(num) {
      const value = Number(num || 0);
      if (Math.abs(value) >= 1e6) return `${formatNumber(value / 1e6)} Tr`;
      if (Math.abs(value) >= 1e3) return `${formatNumber(value / 1e3)} N`;
      return formatNumber(value, 0);
    }

    function formatNumber(num, fixed = 1) {
      const value = Number(num || 0);
      const digits = Math.abs(value) >= 100 ? 0 : fixed;
      return value.toLocaleString("vi-VN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }

    function formatPercent(value) {
      return `${formatNumber(value, 1)}%`;
    }

    function pctDelta(current, previous) {
      if (!previous && !current) return 0;
      if (!previous) return 100;
      return ((current - previous) / previous) * 100;
    }

    function deltaHtml(value, inverse = false) {
      const cls = Math.abs(value) < 0.05 ? "flat" : (value > 0 ? (inverse ? "down" : "up") : (inverse ? "up" : "down"));
      const arrow = Math.abs(value) < 0.05 ? "•" : (value > 0 ? "↑" : "↓");
      return `<span class="delta ${cls}">${arrow} ${formatPercent(Math.abs(value))}</span>`;
    }

    function dayDiff(from, to) {
      const ms = new Date(to).getTime() - new Date(from).getTime();
      return Math.floor(ms / 86400000) + 1;
    }

    function addDays(isoDate, days) {
      const dt = new Date(isoDate + "T00:00:00");
      dt.setDate(dt.getDate() + days);
      return dt.toISOString().slice(0, 10);
    }

    function inRange(date, from, to) {
      return date >= from && date <= to;
    }

    function normalizeText(value) {
      return String(value || "").trim().toLowerCase();
    }

    function matchesSelectedFilter(candidate, selectedValues) {
      if (!selectedValues.length) return true;
      const target = normalizeText(candidate);
      return selectedValues.some(token => target === normalizeText(token));
    }

    function filterRecords(source, from, to) {
      return source.filter(item => {
        if (!inRange(item.d, from, to)) return false;
        if (!matchesSelectedFilter(item.c, state.channel)) return false;
        if (!matchesSelectedFilter(item.g, state.group)) return false;
        if (!matchesSelectedFilter(item.s, state.sku)) return false;
        if (!matchesSelectedFilter(item.ks, state.keySummer)) return false;
        if (!matchesSelectedFilter(item.cl, state.classify)) return false;
        if (state.cancel !== "all" && item.x !== state.cancel) return false;
        return true;
      });
    }

    function summarize(records) {
      let revenue = 0;
      let volume = 0;
      let canceled = 0;
      for (const item of records) {
        revenue += item.r;
        volume += item.q;
        if (String(item.x).toLowerCase() === "yes") canceled += 1;
      }
      return {
        revenue,
        volume,
        asp: volume ? revenue / volume : 0,
        cancelRate: records.length ? (canceled / records.length) * 100 : 0,
      };
    }

    function aggregateBy(records, keyField) {
      const map = new Map();
      for (const item of records) {
        const key = item[keyField];
        const row = map.get(key) || { key, revenue: 0, volume: 0, count: 0 };
        row.revenue += item.r;
        row.volume += item.q;
        row.count += 1;
        map.set(key, row);
      }
      return Array.from(map.values());
    }

    function aggregateSku(records) {
      const map = new Map();
      for (const item of records) {
        const key = item.s;
        const row = map.get(key) || { sku: key, variant: item.v, product: item.p, group: item.g, keySummer: item.ks, classify: item.cl, image: item.i, revenue: 0, volume: 0, count: 0 };
        row.revenue += item.r;
        row.volume += item.q;
        row.count += 1;
        if (!row.product || row.product.length < item.p.length) row.product = item.p;
        if (!row.image && item.i) row.image = item.i;
        map.set(key, row);
      }
      return Array.from(map.values());
    }

    function sortByRevenue(items) {
      return items.slice().sort((a, b) => b.revenue - a.revenue);
    }

    function makeMonthlyShare(records, keyField, topN = 6) {
      const monthMap = new Map();
      const totalByKey = new Map();
      for (const item of records) {
        const month = item.m;
        const key = item[keyField];
        totalByKey.set(key, (totalByKey.get(key) || 0) + item.r);
        if (!monthMap.has(month)) monthMap.set(month, new Map());
        const bucket = monthMap.get(month);
        bucket.set(key, (bucket.get(key) || 0) + item.r);
      }
      const topKeys = Array.from(totalByKey.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, topN)
        .map(([key]) => key);
      const months = Array.from(monthMap.keys()).sort();
      const rows = months.map(month => {
        const values = Object.fromEntries(topKeys.map(key => [key, 0]));
        let other = 0;
        let total = 0;
        for (const [key, value] of monthMap.get(month).entries()) {
          total += value;
          if (topKeys.includes(key)) values[key] += value;
          else other += value;
        }
        values["Khác"] = other;
        return { month, total, values };
      });
      return { months, keys: topKeys.concat(["Khác"]), rows };
    }

    function render() {
      const current = filterRecords(REPORT_DATA.records, state.from, state.to);
      const spanDays = dayDiff(state.from, state.to);
      const previousTo = addDays(state.from, -1);
      const previousFrom = addDays(previousTo, -(spanDays - 1));
      const previous = filterRecords(REPORT_DATA.records, previousFrom, previousTo);

      const summary = summarize(current);
      const prevSummary = summarize(previous);

      setMetric("metricRevenue", "metricRevenueDelta", summary.revenue, prevSummary.revenue, formatCompactCurrency, false);
      setMetric("metricVolume", "metricVolumeDelta", summary.volume, prevSummary.volume, formatCompactUnits, false);
      setMetric("metricAsp", "metricAspDelta", summary.asp, prevSummary.asp, formatCompactCurrency, false);
      setMetric("metricCancel", "metricCancelDelta", summary.cancelRate, prevSummary.cancelRate, formatPercent, true);

      renderTrendChart(current);
      renderBreakdown("channel", "c", current, previous);
      renderBreakdown("group", "g", current, previous);
      renderBreakdown("keySummer", "ks", current, previous);
      renderBreakdown("classify", "cl", current, previous);
      renderSkuTable(current, previous);
    }

    function setMetric(valueId, deltaId, current, previous, formatter, inverse) {
      document.getElementById(valueId).textContent = formatter(current);
      const delta = pctDelta(current, previous);
      const el = document.getElementById(deltaId);
      el.className = `delta ${Math.abs(delta) < 0.05 ? "flat" : (delta > 0 ? (inverse ? "down" : "up") : (inverse ? "up" : "down"))}`;
      el.innerHTML = `${Math.abs(delta) < 0.05 ? "•" : (delta > 0 ? "↑" : "↓")} ${formatPercent(Math.abs(delta))} vs kỳ trước`;
    }

    function renderTrendChart(records) {
      const map = new Map();
      for (const item of records) {
        const row = map.get(item.d) || { date: item.d, revenue: 0, volume: 0 };
        row.revenue += item.r;
        row.volume += item.q;
        map.set(item.d, row);
      }
      const rows = Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date));
      if (!rows.length) {
        document.getElementById("trendChart").innerHTML = `<div class="empty">${t("noData")}</div>`;
        return;
      }
      const width = Math.max(960, rows.length * 34);
      const height = 420;
      const pad = { top: 18, right: 56, bottom: 72, left: 60 };
      const chartW = width - pad.left - pad.right;
      const chartH = height - pad.top - pad.bottom;
      const maxRevenue = Math.max(...rows.map(r => r.revenue), 1);
      const maxVolume = Math.max(...rows.map(r => r.volume), 1);
      const step = chartW / rows.length;
      const barW = Math.max(10, step * 0.72);
      const labelCandidates = rows.length <= 31
        ? new Set(rows.map(row => row.date))
        : new Set(rows.slice().sort((a, b) => b.revenue - a.revenue).slice(0, 8).map(row => row.date).concat(rows.slice().sort((a, b) => b.volume - a.volume).slice(0, 8).map(row => row.date)));

      let bars = "";
      let labels = "";
      let volumePath = "";
      let volumeDots = "";
      let grid = "";

      for (let i = 0; i <= 5; i++) {
        const y = pad.top + (chartH * i / 5);
        grid += `<line x1="${pad.left}" y1="${y}" x2="${width - pad.right}" y2="${y}" stroke="#dbe6f0" stroke-width="1" />`;
      }

      rows.forEach((row, idx) => {
        const cx = pad.left + idx * step + step / 2;
        const barHeight = (row.revenue / maxRevenue) * chartH;
        const x = cx - barW / 2;
        const y = pad.top + chartH - barHeight;
        bars += `<rect x="${x}" y="${y}" width="${barW}" height="${barHeight}" rx="4" fill="#4d89e8" opacity="0.95"><title>${row.date} | DT ${formatCompactCurrency(row.revenue)} | Volume ${formatCompactUnits(row.volume)}</title></rect>`;

        const vy = pad.top + chartH - (row.volume / maxVolume) * chartH;
        volumePath += `${idx === 0 ? "M" : "L"} ${cx} ${vy} `;
        volumeDots += `<circle cx="${cx}" cy="${vy}" r="4.5" fill="#f39a3f"><title>${row.date} | DT ${formatCompactCurrency(row.revenue)} | Volume ${formatCompactUnits(row.volume)}</title></circle>`;

        if (labelCandidates.has(row.date)) {
          bars += `<text x="${cx}" y="${Math.max(18, y - 8)}" text-anchor="middle" font-size="11" font-weight="700" fill="#4d89e8">${formatCompactCurrency(row.revenue)}</text>`;
          volumeDots += `<text x="${cx}" y="${Math.min(pad.top + chartH - 12, Math.max(16, vy - 12))}" text-anchor="middle" font-size="11" font-weight="700" fill="#f39a3f">${formatCompactUnits(row.volume)}</text>`;
        }

        if (idx % Math.ceil(rows.length / 16) === 0 || rows.length <= 16) {
          labels += `<text x="${cx}" y="${height - 34}" text-anchor="end" transform="rotate(-35 ${cx} ${height - 34})" font-size="11" fill="#60758d">${formatShortDate(row.date)}</text>`;
        }
      });

      const svg = `
        <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="DT và volume theo ngày">
          ${grid}
          <line x1="${pad.left}" y1="${pad.top + chartH}" x2="${width - pad.right}" y2="${pad.top + chartH}" stroke="#9fb3c8" stroke-width="1.2" />
          <path d="${volumePath}" fill="none" stroke="#f39a3f" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"></path>
          ${bars}
          ${volumeDots}
          ${labels}
          <text x="${pad.left - 18}" y="${pad.top + 8}" font-size="12" fill="#60758d">${t("revenue")}</text>
          <text x="${width - pad.right + 16}" y="${pad.top + 8}" font-size="12" fill="#60758d">${t("volume")}</text>
        </svg>`;
      document.getElementById("trendChart").innerHTML = svg;
    }

    function formatShortDate(iso) {
      const dt = new Date(iso + "T00:00:00");
      return dt.toLocaleDateString("vi-VN", { day: "numeric", month: "short" });
    }

    function renderBreakdown(prefix, keyField, current, previous) {
      const currentRows = sortByRevenue(aggregateBy(current, keyField));
      const previousRows = aggregateBy(previous, keyField);
      const previousMap = new Map(previousRows.map(row => [row.key, row]));

      document.getElementById(prefix + "Viz").innerHTML = renderSummaryViz(currentRows);
      document.getElementById(prefix + "Table").innerHTML = renderSummaryTable(currentRows, previousMap, prefix);
      attachSummarySortHandlers(prefix, currentRows, previousMap);
    }

    function sortSummaryRows(rows, previousMap, type) {
      const sort = state.summarySort[type] || { key: "revenue", dir: "desc" };
      const factor = sort.dir === "asc" ? 1 : -1;
      return rows.slice().sort((a, b) => {
        let av = 0;
        let bv = 0;
        if (sort.key === "label") {
          av = translateDataValue(a.key);
          bv = translateDataValue(b.key);
          return String(av).localeCompare(String(bv)) * factor;
        }
        if (sort.key === "asp") {
          av = a.volume ? a.revenue / a.volume : 0;
          bv = b.volume ? b.revenue / b.volume : 0;
        } else {
          av = a[sort.key] || 0;
          bv = b[sort.key] || 0;
        }
        return (av - bv) * factor;
      });
    }

    function attachSummarySortHandlers(type, rows, previousMap) {
      document.querySelectorAll(`[data-summary-sort="${type}"]`).forEach(btn => {
        btn.addEventListener("click", () => {
          const key = btn.dataset.sortKey;
          const current = state.summarySort[type] || { key: "revenue", dir: "desc" };
          state.summarySort[type] = {
            key,
            dir: current.key === key && current.dir === "desc" ? "asc" : "desc",
          };
          document.getElementById(type + "Table").innerHTML = renderSummaryTable(rows, previousMap, type);
          attachSummarySortHandlers(type, rows, previousMap);
        });
      });
    }

    function sortSkuRows(rows) {
      const sort = state.skuSort || { key: "revenue", dir: "desc" };
      const factor = sort.dir === "asc" ? 1 : -1;
      return rows.slice().sort((a, b) => {
        let av;
        let bv;
        switch (sort.key) {
          case "barcode":
            av = a.sku || "";
            bv = b.sku || "";
            return String(av).localeCompare(String(bv)) * factor;
          case "product":
            av = a.product || "";
            bv = b.product || "";
            return String(av).localeCompare(String(bv)) * factor;
          case "group":
            av = a.group || "";
            bv = b.group || "";
            return String(av).localeCompare(String(bv)) * factor;
          case "asp":
            av = a.volume ? a.revenue / a.volume : 0;
            bv = b.volume ? b.revenue / b.volume : 0;
            break;
          case "volume":
            av = a.volume || 0;
            bv = b.volume || 0;
            break;
          case "revenue":
          default:
            av = a.revenue || 0;
            bv = b.revenue || 0;
            break;
        }
        return (av - bv) * factor;
      });
    }

    function attachSkuSortHandlers(rows, previousMap) {
      document.querySelectorAll("[data-sku-sort]").forEach(btn => {
        btn.addEventListener("click", () => {
          const key = btn.dataset.sortKey;
          const current = state.skuSort || { key: "revenue", dir: "desc" };
          state.skuSort = {
            key,
            dir: current.key === key && current.dir === "desc" ? "asc" : "desc",
          };
          document.getElementById("skuTable").innerHTML = renderSkuTableMarkup(rows, previousMap);
          attachSkuSortHandlers(rows, previousMap);
        });
      });
    }

    function renderSummaryViz(rows) {
      if (!rows.length) {
        return `<div class="empty">${t("noData")}</div>`;
      }
      const total = rows.reduce((sum, row) => sum + row.revenue, 0);
      const topRows = rows.slice(0, 8);
      return `<div class="viz-list">${topRows.map((row, idx) => {
        const share = total ? (row.revenue / total) * 100 : 0;
        return `<div class="viz-row">
          <div class="viz-name">${escapeHtml(translateDataValue(row.key))}</div>
          <div class="viz-bar">
            <div class="viz-fill" style="width:${share}%; background:${PALETTE[idx % PALETTE.length]}"></div>
          </div>
          <div class="viz-meta">${formatCompactCurrency(row.revenue)} · ${formatPercent(share)}</div>
        </div>`;
      }).join("")}</div>`;
    }

    function renderSummaryTable(rows, previousMap, type) {
      if (!rows.length) return `<div class="empty">${t("noData")}</div>`;
      const sortedRows = sortSummaryRows(rows, previousMap, type);
      const totalRevenue = rows.reduce((sum, row) => sum + row.revenue, 0);
      const totalVolume = rows.reduce((sum, row) => sum + row.volume, 0);
      const titleMap = {
        channel: "Kênh bán hàng",
        group: "Group",
        keySummer: "Key Summer",
        classify: "Classify",
      };
      const visibleRows = type === "group" ? sortedRows : sortedRows.slice(0, 8);
      const sort = state.summarySort[type] || { key: "revenue", dir: "desc" };
      const arrow = (key) => sort.key === key ? (sort.dir === "desc" ? "▼" : "▲") : "↕";
      const body = visibleRows.map((row, idx) => {
        const prev = previousMap.get(row.key) || { revenue: 0, volume: 0 };
        const asp = row.volume ? row.revenue / row.volume : 0;
        const prevAsp = prev.volume ? prev.revenue / prev.volume : 0;
        const share = totalRevenue ? (row.revenue / totalRevenue) * 100 : 0;
        return `<tr>
          <td class="rank">${idx + 1}.</td>
          <td>${escapeHtml(translateDataValue(row.key))}<div style="margin-top:4px"><span class="share-badge">${formatPercent(share)}</span></div></td>
          <td><div class="metric-cell"><div class="metric-main">${formatCompactCurrency(row.revenue)}</div>${deltaHtml(pctDelta(row.revenue, prev.revenue))}</div></td>
          <td><div class="metric-cell"><div class="metric-main">${formatCompactUnits(row.volume)}</div>${deltaHtml(pctDelta(row.volume, prev.volume))}</div></td>
          <td><div class="metric-cell"><div class="metric-main">${formatCompactCurrency(asp)}</div>${deltaHtml(pctDelta(asp, prevAsp))}</div></td>
        </tr>`;
      }).join("");
      return `<div class="table-scroll"><table class="compact-table">
        <thead>
          <tr>
            <th style="width:40px"></th>
            <th><button class="sort-btn" data-summary-sort="${type}" data-sort-key="label">${titleMap[type] || "Nhóm"} <span class="sort-indicator">${arrow("label")}</span></button></th>
            <th><button class="sort-btn" data-summary-sort="${type}" data-sort-key="revenue">${t("revenue")} <span class="sort-indicator">${arrow("revenue")}</span></button></th>
            <th><button class="sort-btn" data-summary-sort="${type}" data-sort-key="volume">${t("volume")} <span class="sort-indicator">${arrow("volume")}</span></button></th>
            <th><button class="sort-btn" data-summary-sort="${type}" data-sort-key="asp">${t("asp")} <span class="sort-indicator">${arrow("asp")}</span></button></th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table></div>
      <div class="summary-total">
        <div class="summary-total-grid">
          <div class="summary-total-label">${t("total")}</div>
          <div class="summary-stat">
            <div class="summary-stat-label">${t("revenue")}</div>
            <div class="summary-stat-value">${formatCompactCurrency(totalRevenue)}</div>
          </div>
          <div class="summary-stat">
            <div class="summary-stat-label">${t("volume")}</div>
            <div class="summary-stat-value">${formatCompactUnits(totalVolume)}</div>
          </div>
          <div class="summary-stat">
            <div class="summary-stat-label">${t("asp")}</div>
            <div class="summary-stat-value">${formatCompactCurrency(totalVolume ? totalRevenue / totalVolume : 0)}</div>
          </div>
        </div>
      </div>`;
    }

    function renderSkuTable(current, previous) {
      const currentFiltered = current.filter(item =>
        matchesSelectedFilter(item.c, state.skuDetailChannel) &&
        matchesSelectedFilter(item.g, state.skuDetailGroup)
      );
      const previousFiltered = previous.filter(item =>
        matchesSelectedFilter(item.c, state.skuDetailChannel) &&
        matchesSelectedFilter(item.g, state.skuDetailGroup)
      );
      const currentRows = aggregateSku(currentFiltered);
      const prevMap = new Map(aggregateSku(previousFiltered).map(row => [row.sku, row]));
      if (!currentRows.length) {
        document.getElementById("skuTable").innerHTML = `<div class="empty">${t("noData")}</div>`;
        return;
      }
      document.getElementById("skuTable").innerHTML = renderSkuTableMarkup(currentRows, prevMap);
      attachSkuSortHandlers(currentRows, prevMap);
    }

    function renderSkuTableMarkup(rows, prevMap) {
      const sortedRows = sortSkuRows(rows).slice(0, 100);
      const sort = state.skuSort || { key: "revenue", dir: "desc" };
      const arrow = (key) => sort.key === key ? (sort.dir === "desc" ? "▼" : "▲") : "↕";
      const body = sortedRows.map((row, idx) => {
        const prev = prevMap.get(row.sku) || { revenue: 0, volume: 0 };
        const asp = row.volume ? row.revenue / row.volume : 0;
        const prevAsp = prev.volume ? prev.revenue / prev.volume : 0;
        return `<tr>
          <td class="rank">${idx + 1}.</td>
          <td>${row.image ? `<img class="sku-thumb" src="${escapeAttr(row.image)}" alt="${escapeAttr(row.product)}" loading="lazy" />` : ''}</td>
          <td><div class="sku-meta"><span class="sku-code">${escapeHtml(row.sku)}</span><span class="subtle">${t("variant")}: ${escapeHtml(row.variant || "-")}</span></div></td>
          <td><div class="sku-meta"><span>${escapeHtml(row.product)}</span><span class="subtle">${escapeHtml(translateDataValue(row.keySummer || "Khác"))} · ${escapeHtml(translateDataValue(row.classify || "Chưa phân loại"))}</span></div></td>
          <td>${escapeHtml(translateDataValue(row.group))}</td>
          <td>${formatCompactCurrency(row.revenue)}</td>
          <td>${deltaHtml(pctDelta(row.revenue, prev.revenue))}</td>
          <td>${formatCompactUnits(row.volume)}</td>
          <td>${deltaHtml(pctDelta(row.volume, prev.volume))}</td>
          <td>${formatCompactCurrency(asp)}</td>
          <td>${deltaHtml(pctDelta(asp, prevAsp))}</td>
        </tr>`;
      }).join("");
      return `<div class="table-scroll"><table>
        <thead>
          <tr>
            <th></th>
            <th>${t("image")}</th>
            <th><button class="sort-btn" data-sku-sort="true" data-sort-key="barcode">Barcode <span class="sort-indicator">${arrow("barcode")}</span></button></th>
            <th><button class="sort-btn" data-sku-sort="true" data-sort-key="product">${t("productName")} <span class="sort-indicator">${arrow("product")}</span></button></th>
            <th><button class="sort-btn" data-sku-sort="true" data-sort-key="group">Group <span class="sort-indicator">${arrow("group")}</span></button></th>
            <th><button class="sort-btn" data-sku-sort="true" data-sort-key="revenue">${t("revenue")} <span class="sort-indicator">${arrow("revenue")}</span></button></th>
            <th>% Δ</th>
            <th><button class="sort-btn" data-sku-sort="true" data-sort-key="volume">${t("volume")} <span class="sort-indicator">${arrow("volume")}</span></button></th>
            <th>% Δ</th>
            <th><button class="sort-btn" data-sku-sort="true" data-sort-key="asp">${t("asp")} <span class="sort-indicator">${arrow("asp")}</span></button></th>
            <th>% Δ</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table></div>`;
    }

    function translateDataValue(value) {
      if (state.lang !== "en") return value;
      const map = {
        "Khác": "Other",
        "Chưa phân loại": "Unclassified",
      };
      return map[value] || value;
    }

    function buildCsv(header, rows) {
      return "\uFEFF" + [header].concat(rows).map(cols =>
        cols.map(value => `"${String(value ?? "").replaceAll('"', '""')}"`).join(",")
      ).join("\r\n");
    }

    function downloadSkuData() {
      const filtered = filterRecords(REPORT_DATA.records, state.from, state.to).filter(item =>
        matchesSelectedFilter(item.c, state.skuDetailChannel) &&
        matchesSelectedFilter(item.g, state.skuDetailGroup)
      );
      const rows = sortByRevenue(aggregateSku(filtered));
      const header = ["image","barcode","variant_id","product_name","group","key_summer","classify","revenue","volume","asp"];
      const csv = buildCsv(header, rows.map(row => {
        const asp = row.volume ? row.revenue / row.volume : 0;
        return [
          row.image || "", row.sku, row.variant || "", row.product, row.group, row.keySummer || "", row.classify || "",
          row.revenue, row.volume, asp
        ];
      }));
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${t("downloadFileNameSku")}.csv`;
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    }

    function downloadGroupData() {
      const filtered = filterRecords(REPORT_DATA.records, state.from, state.to);
      const rows = sortByRevenue(aggregateBy(filtered, "g"));
      const totalRevenue = rows.reduce((sum, row) => sum + row.revenue, 0);
      const csv = buildCsv(
        ["group","revenue","volume","asp","share"],
        rows.map(row => {
          const asp = row.volume ? row.revenue / row.volume : 0;
          const share = totalRevenue ? (row.revenue / totalRevenue) * 100 : 0;
          return [row.key, row.revenue, row.volume, asp, share];
        })
      );
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${t("downloadFileNameGroup")}.csv`;
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    }

    setup();
  </script>
</body>
</html>
"""


def main():
    dataset = build_dataset()
    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(dataset, ensure_ascii=False, separators=(",", ":")))
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"Generated: {OUTPUT_FILE}")
    print(f"Records: {dataset['meta']['recordCount']}")
    print(f"Date range: {dataset['meta']['minDate']} -> {dataset['meta']['maxDate']}")


if __name__ == "__main__":
    main()
