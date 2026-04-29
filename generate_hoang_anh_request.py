#!/usr/bin/env python3
import json
import math
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import generate_report as base


BASE_DIR = Path("/Users/nguyencan/Library/CloudStorage/OneDrive-TARA/Order Haravan")
OUTPUT_FILE = BASE_DIR / "Hoang Anh Request.html"
PUBLISHED_DIR = BASE_DIR / "hoang-anh-request"
PUBLISHED_FILE = PUBLISHED_DIR / "index.html"
NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def classify_channel(raw_channel: str):
    channel = (raw_channel or "").strip().lower()
    mapping = {
        "shopee": ("Shopee", "Shopee", True),
        "lazada": ("Lazada", "Lazada", True),
        "tiktokshop": ("TTS", "TikTok Shop", True),
        "tiki": ("Tiki", "Tiki", True),
        "web": ("Web", "Web D2C", True),
        "website - carez": ("Web", "Web D2C", True),
        "phone": ("Offline", "Offline", False),
        "zalo": ("Offline", "Offline", False),
        "haravan_draft_order": ("Offline", "Offline", False),
    }
    if channel in mapping:
        return mapping[channel]
    return ("Khác", "Khác", True)


def is_cancelled(status: str) -> bool:
    text = (status or "").strip().lower()
    return text in {"yes", "y", "true", "1", "cancelled", "canceled"}


def choose_gross_and_net(quantity: float, unit_price: float, compare_price: float, discount: float, order_total: float):
    gross = 0.0
    if unit_price > 0:
        gross = unit_price * quantity
    if compare_price > 0:
        gross = max(gross, compare_price * quantity)
    if gross <= 0 and order_total > 0:
        gross = order_total

    net = unit_price * quantity if unit_price > 0 else gross
    if net <= 0 and order_total > 0:
        net = order_total
    if discount > 0:
        net = max(0.0, net - discount)
    if net <= 0 and gross > 0:
        net = gross
    return round(gross, 2), round(net, 2)


def is_ignored_product(product_name: str, sku_code: str, variant_id: str) -> bool:
    haystack = " ".join([product_name or "", sku_code or "", variant_id or ""]).strip().lower()
    return "thank you card bluestone" in haystack


def read_rich_records(path: Path, product_map):
    with zipfile.ZipFile(path) as zf:
        shared = base.read_shared_strings(zf)
        root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        rows = root.findall(".//x:sheetData/x:row", NS)
        if not rows:
            return []

        header = base.parse_row(rows[0], shared)
        index = {name: idx for idx, name in enumerate(header)}

        def get(values, name: str) -> str:
            idx = index.get(name, -1)
            return values[idx] if 0 <= idx < len(values) else ""

        records = []
        for row in rows[1:]:
            values = base.parse_row(row, shared)
            if not values:
                continue

            ordered_at = get(values, "Ngày đặt hàng")
            paid_at = get(values, "Thời gian thanh toán")
            delivered_at = get(values, "Thời gian giao hàng")
            dt = base.choose_order_datetime(ordered_at, paid_at, delivered_at)
            if not dt:
                continue

            quantity = base.safe_float(get(values, "Số lượng sản phẩm")) or 1.0
            unit_price = base.safe_float(get(values, "Giá sản phẩm"))
            compare_price = base.safe_float(get(values, "Giá so sánh sản phẩm"))
            discount = base.safe_float(get(values, "Số tiền giảm"))
            order_total = base.safe_float(get(values, "Tổng cộng"))
            cancel_status = get(values, "Trạng thái hủy")

            gross, net = choose_gross_and_net(quantity, unit_price, compare_price, discount, order_total)

            variant_id = (get(values, "Mã sản phẩm") or get(values, "Id") or "").strip()
            product_name = (get(values, "Tên sản phẩm") or "Không rõ sản phẩm").strip()
            product_meta = product_map.get(variant_id, {})
            sku_code = product_meta.get("barcode") or variant_id or product_name
            group_name = product_meta.get("website_group") or base.derive_group(product_name)
            if is_ignored_product(product_name, sku_code, variant_id):
                continue

            raw_channel = base.normalize_channel(get(values, "Kênh bán hàng"))
            channel_bucket, marketplace, is_online = classify_channel(raw_channel)

            records.append(
                {
                    "date": dt.strftime("%Y-%m-%d"),
                    "month": dt.strftime("%Y-%m"),
                    "raw_channel": raw_channel,
                    "channel": channel_bucket,
                    "marketplace": marketplace,
                    "is_online": is_online,
                    "cancelled": is_cancelled(cancel_status),
                    "quantity": quantity,
                    "gmv": gross,
                    "nmv": net,
                    "sku": sku_code,
                    "variant_id": variant_id,
                    "product_name": product_name,
                    "group": group_name,
                    "image": product_meta.get("image", ""),
                    "key_summer": product_meta.get("key_summer") or "Khác",
                    "classify": product_meta.get("classify") or "Chưa phân loại",
                    "order_id": (get(values, "Mã đơn hàng") or "").strip(),
                }
            )
        return records


def build_records():
    product_map = base.read_product_mapping()
    records = []
    for path in sorted(BASE_DIR.glob("Orders_T*_20*.xlsx")):
        records.extend(read_rich_records(path, product_map))
    if not records:
        raise SystemExit("No order data found.")
    return records, product_map


def order_count(order_set):
    return len([item for item in order_set if item])


def aov_from(nmv: float, success_orders: int, total_orders: int):
    denom = success_orders or total_orders
    return round(nmv / denom, 2) if denom else 0.0


def pct(value: float, total: float):
    return round((value / total) * 100, 2) if total else 0.0


def aggregate(records):
    months = sorted({r["month"] for r in records})
    max_month = months[-1]
    last_12_months = set(months[-12:])
    min_date = min(r["date"] for r in records)
    max_date = max(r["date"] for r in records)

    sec1 = {}
    sec2 = {}
    sec4 = {}
    sec5 = {}
    sec3_platform_sku = defaultdict(dict)
    online_gmv_by_month = defaultdict(float)
    month_totals = defaultdict(float)

    for r in records:
        order_id = r["order_id"] or f"{r['date']}::{r['sku']}::{r['variant_id']}"

        key1 = (r["sku"], r["month"], r["channel"])
        row1 = sec1.setdefault(
            key1,
            {
                "ma_sku": r["sku"],
                "ten_sp": r["product_name"],
                "nhom_sp": r["group"],
                "thang": r["month"],
                "kenh": r["channel"],
                "so_luong_ban": 0.0,
                "GMV": 0.0,
                "NMV": 0.0,
                "_success_orders": set(),
                "_cancel_orders": set(),
            },
        )
        row1["so_luong_ban"] += r["quantity"]
        row1["GMV"] += r["gmv"]
        row1["NMV"] += r["nmv"]
        (row1["_cancel_orders"] if r["cancelled"] else row1["_success_orders"]).add(order_id)

        if r["marketplace"] != "Offline":
            key2 = (r["marketplace"], r["month"])
            row2 = sec2.setdefault(
                key2,
                {
                    "san": r["marketplace"],
                    "thang": r["month"],
                    "GMV": 0.0,
                    "NMV": 0.0,
                    "_orders": set(),
                    "_success_orders": set(),
                    "_active_skus": set(),
                },
            )
            row2["GMV"] += r["gmv"]
            row2["NMV"] += r["nmv"]
            row2["_orders"].add(order_id)
            if not r["cancelled"]:
                row2["_success_orders"].add(order_id)
                row2["_active_skus"].add(r["sku"])
            if r["is_online"]:
                online_gmv_by_month[r["month"]] += r["gmv"]

        if r["marketplace"] in {"Shopee", "Lazada", "TikTok Shop", "Tiki", "Web D2C"}:
            key4 = (r["date"], r["marketplace"])
            row4 = sec4.setdefault(
                key4,
                {
                    "ngay": r["date"],
                    "san": r["marketplace"],
                    "GMV": 0.0,
                    "NMV": 0.0,
                    "_success_orders": set(),
                    "_cancel_orders": set(),
                    "_all_orders": set(),
                    "_skus": set(),
                },
            )
            row4["GMV"] += r["gmv"]
            row4["NMV"] += r["nmv"]
            row4["_all_orders"].add(order_id)
            row4["_skus"].add(r["sku"])
            (row4["_cancel_orders"] if r["cancelled"] else row4["_success_orders"]).add(order_id)

        key5 = (r["month"], r["sku"], r["channel"])
        row5 = sec5.setdefault(
            key5,
            {
                "thang": r["month"],
                "ma_sku": r["sku"],
                "ten_sp": r["product_name"],
                "nhom_sp": r["group"],
                "kenh": r["channel"],
                "GMV": 0.0,
                "NMV": 0.0,
                "so_luong_ban": 0.0,
                "_success_orders": set(),
                "_cancel_orders": set(),
                "_all_orders": set(),
            },
        )
        row5["GMV"] += r["gmv"]
        row5["NMV"] += r["nmv"]
        row5["so_luong_ban"] += r["quantity"]
        row5["_all_orders"].add(order_id)
        (row5["_cancel_orders"] if r["cancelled"] else row5["_success_orders"]).add(order_id)
        month_totals[r["month"]] += r["gmv"]

        if r["month"] in last_12_months and r["marketplace"] in {"Shopee", "Lazada", "TikTok Shop", "Tiki"}:
            platform_stats = sec3_platform_sku[r["marketplace"]]
            item = platform_stats.setdefault(
                r["sku"],
                {
                    "ma_sku": r["sku"],
                    "ten_sp": r["product_name"],
                    "nhom_sp": r["group"],
                    "GMV": 0.0,
                    "NMV": 0.0,
                    "so_luong_ban": 0.0,
                    "_all_orders": set(),
                    "_success_orders": set(),
                    "_cancel_orders": set(),
                },
            )
            item["GMV"] += r["gmv"]
            item["NMV"] += r["nmv"]
            item["so_luong_ban"] += r["quantity"]
            item["_all_orders"].add(order_id)
            (item["_cancel_orders"] if r["cancelled"] else item["_success_orders"]).add(order_id)

    section1 = []
    for row in sec1.values():
        row["so_luong_ban"] = round(row["so_luong_ban"], 2)
        row["GMV"] = round(row["GMV"], 2)
        row["NMV"] = round(row["NMV"], 2)
        row["don_hang_thanh_cong"] = order_count(row.pop("_success_orders"))
        row["don_huy"] = order_count(row.pop("_cancel_orders"))
        section1.append(row)
    section1.sort(key=lambda x: (x["thang"], x["GMV"], x["so_luong_ban"]), reverse=True)

    section2 = []
    for row in sec2.values():
        total_orders = order_count(row.pop("_orders"))
        success_orders = order_count(row.pop("_success_orders"))
        active_skus = len(row.pop("_active_skus"))
        row["GMV"] = round(row["GMV"], 2)
        row["NMV"] = round(row["NMV"], 2)
        row["so_don"] = total_orders
        row["AOV"] = aov_from(row["NMV"], success_orders, total_orders)
        row["so_SKU_active_co_don"] = active_skus
        row["pct_thi_phan_online"] = pct(row["GMV"], online_gmv_by_month[row["thang"]])
        section2.append(row)
    section2.sort(key=lambda x: (x["thang"], x["GMV"]), reverse=True)

    section4 = []
    for row in sec4.values():
        success_orders = order_count(row.pop("_success_orders"))
        cancel_orders = order_count(row.pop("_cancel_orders"))
        total_orders = order_count(row.pop("_all_orders"))
        distinct_skus = len(row.pop("_skus"))
        row["GMV"] = round(row["GMV"], 2)
        row["NMV"] = round(row["NMV"], 2)
        row["so_don_thanh_cong"] = success_orders
        row["so_don_huy"] = cancel_orders
        row["AOV"] = aov_from(row["NMV"], success_orders, total_orders)
        row["so_SKU_distinct"] = distinct_skus
        row["ty_le_huy_hoan"] = pct(cancel_orders, total_orders)
        section4.append(row)
    section4.sort(key=lambda x: (x["ngay"], x["san"]), reverse=True)

    sec5_rows_by_month = defaultdict(list)
    for row in sec5.values():
        success_orders = order_count(row.pop("_success_orders"))
        cancel_orders = order_count(row.pop("_cancel_orders"))
        total_orders = order_count(row.pop("_all_orders"))
        row["GMV"] = round(row["GMV"], 2)
        row["NMV"] = round(row["NMV"], 2)
        row["so_luong_ban"] = round(row["so_luong_ban"], 2)
        row["AOV_sku"] = aov_from(row["NMV"], success_orders, total_orders)
        row["pct_GMV_tren_thang"] = pct(row["GMV"], month_totals[row["thang"]])
        row["ty_le_huy_hoan"] = pct(cancel_orders, total_orders)
        sec5_rows_by_month[row["thang"]].append(row)

    section5 = []
    for month in sorted(sec5_rows_by_month.keys(), reverse=True):
        ranked = sorted(sec5_rows_by_month[month], key=lambda x: (x["GMV"], x["so_luong_ban"]), reverse=True)
        for idx, row in enumerate(ranked, start=1):
            row["rank_GMV"] = idx
            section5.append(row)

    section3 = {}
    for platform in ("Shopee", "Lazada", "TikTok Shop", "Tiki"):
        rows = []
        for row in sec3_platform_sku.get(platform, {}).values():
            success_orders = order_count(row.pop("_success_orders"))
            cancel_orders = order_count(row.pop("_cancel_orders"))
            total_orders = order_count(row.pop("_all_orders"))
            row["GMV"] = round(row["GMV"], 2)
            row["NMV"] = round(row["NMV"], 2)
            row["so_luong_ban"] = round(row["so_luong_ban"], 2)
            row["so_don"] = total_orders
            row["AOV"] = aov_from(row["NMV"], success_orders, total_orders)
            row["ty_le_huy_hoan"] = pct(cancel_orders, total_orders)
            rows.append(row)

        rows_by_gmv = sorted(rows, key=lambda x: (x["GMV"], x["so_luong_ban"]), reverse=True)
        rows_by_qty = sorted(rows, key=lambda x: (x["so_luong_ban"], x["GMV"]), reverse=True)
        rows_bottom = sorted(
            [row for row in rows if row["so_don"] > 0],
            key=lambda x: (x["so_don"], x["GMV"], x["ma_sku"])
        )
        total_gmv = sum(row["GMV"] for row in rows)
        section3[platform] = {
            "top_gmv": rows_by_gmv[:10],
            "top_qty": rows_by_qty[:10],
            "bottom": rows_bottom[:5],
            "summary": {
                "total_gmv": round(total_gmv, 2),
                "active_skus": len(rows),
                "concentration_top10_pct": pct(sum(row["GMV"] for row in rows_by_gmv[:10]), total_gmv),
            },
        }

    return {
        "meta": {
            "generatedAt": datetime.now().isoformat(timespec="seconds"),
            "minDate": min_date,
            "maxDate": max_date,
            "monthsAvailable": months,
            "monthCount": len(months),
            "recordCount": len(records),
            "notes": [
                "Chỉ hiển thị field nào có thể suy ra từ Haravan export + file mapping hiện có.",
                "Số_SKU_đang_listing theo từng sàn không có trong nguồn hiện tại nên được bỏ qua.",
                "Bottom underperform trong phần Top SKU đang dựa trên SKU có phát sinh đơn trong 12 tháng gần nhất, không phản ánh trạng thái listing.",
                "GMV/NMV được suy ra từ giá sản phẩm, giá so sánh và số tiền giảm hiện có trong file gốc.",
            ],
        },
        "filters": {
            "months": sorted({item["thang"] for item in section1}, reverse=True),
            "channels": sorted({item["kenh"] for item in section1}),
            "marketplaces": sorted({item["san"] for item in section2}),
            "dailyMarketplaces": sorted({item["san"] for item in section4}),
        },
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "section5": section5,
    }


HTML_TEMPLATE = r"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Hoang Anh Request</title>
  <style>
    :root {
      --bg: #f3f7fb;
      --card: #ffffff;
      --ink: #17324a;
      --muted: #6a7f96;
      --line: #d8e4ef;
      --blue: #2f73da;
      --blue-soft: #eef5ff;
      --green: #159864;
      --orange: #f0a14a;
      --red: #d35656;
      --shadow: 0 12px 30px rgba(23, 50, 74, 0.08);
      --radius: 18px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(47,115,218,0.13), transparent 22%),
        linear-gradient(180deg, #fbfdff 0%, #f2f6fb 100%);
    }
    .page { max-width: 1600px; margin: 0 auto; padding: 18px 18px 60px; }
    .hero, .panel, .card { background: var(--card); border: 1px solid rgba(23,50,74,0.08); border-radius: var(--radius); box-shadow: var(--shadow); }
    .hero { padding: 22px; margin-bottom: 18px; }
    .hero h1 { margin: 0; font-size: 30px; }
    .hero p { margin: 8px 0 0; color: var(--muted); }
    .hero ul { margin: 12px 0 0 18px; color: var(--muted); }
    .cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card { padding: 14px 16px; }
    .card .label { font-size: 12px; text-transform: uppercase; color: var(--muted); font-weight: 800; letter-spacing: .04em; }
    .card .value { margin-top: 6px; font-size: 28px; font-weight: 800; }
    .section-title { margin: 20px 0 12px; font-size: 21px; font-weight: 800; text-decoration: underline; text-underline-offset: 4px; }
    .panel { padding: 16px; margin-bottom: 18px; }
    .panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 14px; }
    .panel-head h2, .panel-head h3 { margin: 0; }
    .subtitle { margin-top: 6px; color: var(--muted); font-size: 13px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 14px; }
    .toolbar label { font-size: 12px; color: var(--muted); font-weight: 700; text-transform: uppercase; }
    .toolbar input, .toolbar select, .toolbar button {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: white;
      color: var(--ink);
      padding: 10px 12px;
      font-size: 14px;
    }
    .toolbar button, .download-btn, .tab-btn { cursor: pointer; font-weight: 700; }
    .download-btn, .tab-btn {
      border: none;
      border-radius: 12px;
      padding: 10px 14px;
      background: var(--blue-soft);
      color: var(--blue);
    }
    .tab-btn.active { background: var(--blue); color: white; }
    .table-wrap { overflow: auto; max-height: 620px; border: 1px solid #edf3f8; border-radius: 14px; }
    table { width: 100%; border-collapse: collapse; min-width: 980px; }
    th, td { padding: 12px 14px; border-bottom: 1px solid #edf3f8; vertical-align: top; }
    th {
      position: sticky; top: 0; z-index: 1; background: #eaf3ff;
      text-align: left; font-size: 12px; text-transform: uppercase; letter-spacing: .04em;
    }
    td.num { text-align: right; font-variant-numeric: tabular-nums; }
    tr:nth-child(even) td { background: #fbfdff; }
    .grid-2 { display: grid; grid-template-columns: 1.2fr .8fr; gap: 16px; }
    .stack { display: grid; gap: 16px; }
    .summary-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 14px; }
    .summary-box { background: #f8fbff; border: 1px solid #e5eef8; border-radius: 14px; padding: 14px; }
    .summary-box .label { font-size: 11px; text-transform: uppercase; color: var(--muted); font-weight: 800; }
    .summary-box .value { margin-top: 6px; font-size: 26px; font-weight: 800; }
    .viz-list { display: grid; gap: 10px; }
    .pie-layout { display: grid; grid-template-columns: 320px 1fr; gap: 18px; align-items: start; }
    .pie-wrap { display: grid; justify-items: center; gap: 12px; }
    .pie-chart {
      width: 240px; height: 240px; border-radius: 50%;
      background: conic-gradient(#dfe9f5 0deg 360deg);
      box-shadow: inset 0 0 0 18px #fff;
      position: relative;
    }
    .pie-chart::after {
      content: "";
      position: absolute;
      inset: 54px;
      background: white;
      border-radius: 50%;
      box-shadow: inset 0 0 0 1px #eef3f8;
    }
    .pie-center {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      z-index: 1;
      text-align: center;
      font-weight: 800;
      color: var(--ink);
    }
    .pie-center .small { font-size: 12px; color: var(--muted); font-weight: 700; }
    .legend-list { display: grid; gap: 10px; }
    .legend-row { display: grid; grid-template-columns: 18px 1fr auto; gap: 10px; align-items: center; }
    .legend-dot { width: 12px; height: 12px; border-radius: 50%; }
    .accordion { margin-bottom: 18px; width: 100%; }
    .accordion > summary {
      list-style: none;
      cursor: pointer;
      padding: 18px 20px;
      border-radius: var(--radius);
      background: var(--card);
      border: 1px solid rgba(23,50,74,0.08);
      box-shadow: var(--shadow);
      font-size: 20px;
      font-weight: 800;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .accordion > summary::-webkit-details-marker { display: none; }
    .accordion > summary::after {
      content: "+";
      font-size: 28px;
      line-height: 1;
      color: var(--blue);
    }
    .accordion > summary:focus,
    .accordion > summary:focus-visible {
      outline: none;
      box-shadow: var(--shadow);
    }
    .accordion[open] > summary::after { content: "−"; }
    .accordion-body { padding-top: 12px; }
    .muted { color: var(--muted); }
    .small { font-size: 12px; }
    .no-data { padding: 24px; color: var(--muted); text-align: center; }
    @media (max-width: 1100px) {
      .cards, .summary-grid, .grid-2, .pie-layout { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Hoang Anh Request</h1>
      <p id="metaLine"></p>
      <ul id="noteList"></ul>
    </section>

    <section class="cards">
      <div class="card"><div class="label">Records</div><div class="value" id="cardRecords"></div></div>
      <div class="card"><div class="label">Months Available</div><div class="value" id="cardMonths"></div></div>
      <div class="card"><div class="label">Date Range</div><div class="value" id="cardRange"></div></div>
      <div class="card"><div class="label">Generated</div><div class="value" id="cardGenerated"></div></div>
    </section>

    <details class="accordion" open>
      <summary>1. Doanh số SKU × kênh × tháng</summary>
      <div class="accordion-body">
    <section class="panel">
      <div class="panel-head">
        <div>
          <h3>24 tháng gần nhất trong data hiện có</h3>
          <div class="subtitle">Các cột khả dụng: SKU, tháng, kênh, số lượng, GMV, NMV, đơn thành công, đơn huỷ.</div>
        </div>
        <button class="download-btn" id="downloadSection1">Tải data</button>
      </div>
      <div class="toolbar">
        <div><label>Tháng</label><br><select id="s1Month"></select></div>
        <div><label>Kênh</label><br><select id="s1Channel"></select></div>
        <div style="min-width:260px"><label>Tìm SKU / tên SP</label><br><input id="s1Search" placeholder="Nhập mã SKU hoặc tên sản phẩm" /></div>
      </div>
      <div class="table-wrap"><table id="table1"></table></div>
    </section>
      </div>
    </details>

    <details class="accordion">
      <summary>2. Doanh số phân bổ theo từng sàn</summary>
      <div class="accordion-body">
    <section class="panel">
      <div class="panel-head">
        <div>
          <h3>GMV / số đơn / AOV / share online</h3>
          <div class="subtitle">Không có dữ liệu listing theo sàn trong nguồn hiện tại nên cột đó được bỏ qua.</div>
        </div>
        <button class="download-btn" id="downloadSection2">Tải data</button>
      </div>
      <div class="grid-2">
        <div class="table-wrap"><table id="table2"></table></div>
        <div class="panel" style="margin-bottom:0">
          <h3 style="margin-top:0">Revenue share snapshot</h3>
          <div class="subtitle">Theo tháng đang chọn</div>
          <div class="toolbar">
            <div><label>Tháng</label><br><select id="s2Month"></select></div>
          </div>
          <div class="pie-layout">
            <div class="pie-wrap">
              <div id="sharePie" class="pie-chart">
                <div class="pie-center">
                  <div class="small">Tổng GMV</div>
                  <div id="sharePieTotal"></div>
                </div>
              </div>
            </div>
            <div id="shareViz" class="legend-list"></div>
          </div>
        </div>
      </div>
    </section>
      </div>
    </details>

    <details class="accordion">
      <summary>3. Top SKU GMV + Top SKU số lượng × sàn</summary>
      <div class="accordion-body">
    <section class="panel">
      <div class="panel-head">
        <div>
          <h3>12 tháng gần nhất</h3>
          <div class="subtitle">Bottom 5 đang dùng các SKU có phát sinh đơn trong 12 tháng gần nhất.</div>
        </div>
        <button class="download-btn" id="downloadSection3">Tải data</button>
      </div>
      <div class="toolbar" id="s3Tabs"></div>
      <div class="summary-grid">
        <div class="summary-box"><div class="label">Total GMV</div><div class="value" id="s3TotalGmv"></div></div>
        <div class="summary-box"><div class="label">Active SKU</div><div class="value" id="s3ActiveSku"></div></div>
        <div class="summary-box"><div class="label">Top 10 / Total GMV</div><div class="value" id="s3Concentration"></div></div>
      </div>
      <div class="stack">
        <div class="panel" style="margin-bottom:0">
          <div class="panel-head"><h3>Top 10 SKU theo GMV</h3></div>
          <div class="table-wrap"><table id="table3a"></table></div>
        </div>
        <div class="panel" style="margin-bottom:0">
          <div class="panel-head"><h3>Top 10 SKU theo số lượng</h3></div>
          <div class="table-wrap"><table id="table3b"></table></div>
        </div>
      </div>
        <div class="panel" style="margin-top:16px; margin-bottom:0">
          <div class="panel-head"><h3>Bottom 5 SKU orders thấp nhất</h3></div>
          <div class="table-wrap"><table id="table3c"></table></div>
        </div>
    </section>
      </div>
    </details>

    <details class="accordion">
      <summary>4. Doanh thu Daily × sàn</summary>
      <div class="accordion-body">
    <section class="panel">
      <div class="panel-head">
        <div>
          <h3>Daily x marketplace</h3>
          <div class="subtitle">Chỉ hiển thị các sàn có dữ liệu: Shopee, Lazada, TikTok Shop, Tiki, Web D2C.</div>
        </div>
        <button class="download-btn" id="downloadSection4">Tải data</button>
      </div>
      <div class="toolbar">
        <div><label>Sàn</label><br><select id="s4Marketplace"></select></div>
        <div><label>Số dòng hiển thị</label><br><select id="s4Limit"><option value="60">60</option><option value="180">180</option><option value="365">365</option><option value="999999">Tất cả</option></select></div>
      </div>
      <div class="table-wrap"><table id="table4"></table></div>
    </section>
      </div>
    </details>

    <details class="accordion">
      <summary>5. Cơ cấu doanh thu theo SKU × tháng</summary>
      <div class="accordion-body">
    <section class="panel">
      <div class="panel-head">
        <div>
          <h3>Monthly SKU mix</h3>
          <div class="subtitle">Có đủ: tháng, SKU, tên SP, nhóm, kênh, GMV, NMV, số lượng, AOV, share tháng, rank GMV, tỷ lệ huỷ hoàn.</div>
        </div>
        <button class="download-btn" id="downloadSection5">Tải data</button>
      </div>
      <div class="toolbar">
        <div><label>Tháng</label><br><select id="s5Month"></select></div>
        <div><label>Kênh</label><br><select id="s5Channel"></select></div>
        <div style="min-width:260px"><label>Tìm SKU / tên SP</label><br><input id="s5Search" placeholder="Nhập mã SKU hoặc tên sản phẩm" /></div>
      </div>
      <div class="table-wrap"><table id="table5"></table></div>
    </section>
      </div>
    </details>
  </div>

  <script id="payload" type="application/json">__DATA__</script>
  <script>
    const payload = JSON.parse(document.getElementById("payload").textContent);
    let currentPlatform = "Shopee";

    function formatInt(value) {
      return new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 0 }).format(value || 0);
    }
    function formatQty(value) {
      return new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 2 }).format(value || 0);
    }
    function formatMoney(value) {
      if (!value) return "0";
      const abs = Math.abs(value);
      if (abs >= 1e9) return `${(value / 1e9).toLocaleString("vi-VN", { maximumFractionDigits: 1 })} T`;
      if (abs >= 1e6) return `${(value / 1e6).toLocaleString("vi-VN", { maximumFractionDigits: 1 })} Tr`;
      return new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 0 }).format(value);
    }
    function formatPct(value) {
      return `${(value || 0).toLocaleString("vi-VN", { maximumFractionDigits: 2 })}%`;
    }
    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]));
    }
    function setOptions(select, values, allLabel) {
      select.innerHTML = [`<option value="">${allLabel}</option>`].concat(values.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`)).join("");
    }
    function buildCsv(rows, columns) {
      const esc = (value) => `"${String(value ?? "").replace(/"/g, '""')}"`;
      return [columns.join(","), ...rows.map(row => columns.map(col => esc(row[col])).join(","))].join("\n");
    }
    function downloadCsv(rows, columns, fileName) {
      const blob = new Blob([buildCsv(rows, columns)], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;
      a.click();
      URL.revokeObjectURL(url);
    }
    function renderTable(tableId, columns, rows, formatters = {}) {
      const table = document.getElementById(tableId);
      if (!rows.length) {
        table.innerHTML = `<tbody><tr><td class="no-data" colspan="${columns.length}">Không có dữ liệu cho bộ lọc hiện tại.</td></tr></tbody>`;
        return;
      }
      const head = `<thead><tr>${columns.map(col => `<th>${escapeHtml(col.label)}</th>`).join("")}</tr></thead>`;
      const body = `<tbody>${rows.map(row => `<tr>${columns.map(col => {
        const raw = row[col.key];
        const val = formatters[col.key] ? formatters[col.key](raw, row) : raw;
        return `<td class="${col.numeric ? "num" : ""}">${escapeHtml(val)}</td>`;
      }).join("")}</tr>`).join("")}</tbody>`;
      table.innerHTML = head + body;
    }

    function initMeta() {
      const meta = payload.meta;
      document.getElementById("metaLine").textContent = `Generated ${meta.generatedAt} | ${formatInt(meta.recordCount)} line items | ${meta.minDate} -> ${meta.maxDate}`;
      document.getElementById("cardRecords").textContent = formatInt(meta.recordCount);
      document.getElementById("cardMonths").textContent = formatInt(meta.monthCount);
      document.getElementById("cardRange").textContent = `${meta.minDate} -> ${meta.maxDate}`;
      document.getElementById("cardGenerated").textContent = meta.generatedAt.slice(11, 19);
      document.getElementById("noteList").innerHTML = meta.notes.map(note => `<li>${escapeHtml(note)}</li>`).join("");
    }

    function initSection1() {
      setOptions(document.getElementById("s1Month"), payload.filters.months, "Tất cả tháng");
      setOptions(document.getElementById("s1Channel"), payload.filters.channels, "Tất cả kênh");
      const rerender = () => {
        const month = document.getElementById("s1Month").value;
        const channel = document.getElementById("s1Channel").value;
        const search = document.getElementById("s1Search").value.trim().toLowerCase();
        const rows = payload.section1.filter(row =>
          (!month || row.thang === month) &&
          (!channel || row.kenh === channel) &&
          (!search || row.ma_sku.toLowerCase().includes(search) || row.ten_sp.toLowerCase().includes(search))
        );
        renderTable("table1", [
          { key: "ma_sku", label: "Mã SKU" },
          { key: "ten_sp", label: "Tên SP" },
          { key: "nhom_sp", label: "Nhóm / dòng SP" },
          { key: "thang", label: "Tháng" },
          { key: "kenh", label: "Kênh" },
          { key: "so_luong_ban", label: "Số lượng", numeric: true },
          { key: "GMV", label: "GMV", numeric: true },
          { key: "NMV", label: "NMV", numeric: true },
          { key: "don_hang_thanh_cong", label: "Đơn thành công", numeric: true },
          { key: "don_huy", label: "Đơn huỷ", numeric: true }
        ], rows.slice(0, 300), {
          so_luong_ban: formatQty, GMV: formatMoney, NMV: formatMoney, don_hang_thanh_cong: formatInt, don_huy: formatInt
        });
        document.getElementById("downloadSection1").onclick = () => downloadCsv(rows, [
          "ma_sku","ten_sp","nhom_sp","thang","kenh","so_luong_ban","GMV","NMV","don_hang_thanh_cong","don_huy"
        ], "hoang_anh_request_sku_channel_month.csv");
      };
      ["s1Month","s1Channel","s1Search"].forEach(id => document.getElementById(id).addEventListener("input", rerender));
      rerender();
    }

    function initSection2() {
      setOptions(document.getElementById("s2Month"), payload.filters.months, "Tất cả tháng");
      const render = () => {
        const rows = payload.section2.slice();
        renderTable("table2", [
          { key: "san", label: "Sàn" },
          { key: "thang", label: "Tháng" },
          { key: "GMV", label: "GMV", numeric: true },
          { key: "NMV", label: "NMV", numeric: true },
          { key: "so_don", label: "Số đơn", numeric: true },
          { key: "AOV", label: "AOV", numeric: true },
          { key: "so_SKU_active_co_don", label: "SKU active có đơn", numeric: true },
          { key: "pct_thi_phan_online", label: "% thị phần online", numeric: true }
        ], rows, {
          GMV: formatMoney, NMV: formatMoney, so_don: formatInt, AOV: formatMoney,
          so_SKU_active_co_don: formatInt, pct_thi_phan_online: formatPct
        });

        const month = document.getElementById("s2Month").value || payload.filters.months[0];
        const monthRows = payload.section2.filter(row => row.thang === month).sort((a, b) => b.GMV - a.GMV);
        const total = monthRows.reduce((sum, row) => sum + row.GMV, 0);
        const palette = ["#4d89e8", "#7ba9f4", "#f0a14a", "#48b3a8", "#a67be8", "#f06c87", "#8cbf3f", "#95a3b8"];
        const usableRows = monthRows.filter(row => row.GMV > 0);
        if (!usableRows.length) {
          document.getElementById("shareViz").innerHTML = `<div class="no-data">Không có dữ liệu cho tháng này.</div>`;
          document.getElementById("sharePie").style.background = "conic-gradient(#dfe9f5 0deg 360deg)";
          document.getElementById("sharePieTotal").textContent = "0";
          return;
        }
        let start = 0;
        const segments = usableRows.map((row, idx) => {
          const angle = total ? (row.GMV / total) * 360 : 0;
          const color = palette[idx % palette.length];
          const seg = `${color} ${start}deg ${start + angle}deg`;
          start += angle;
          return seg;
        });
        document.getElementById("sharePie").style.background = `conic-gradient(${segments.join(", ")})`;
        document.getElementById("sharePieTotal").textContent = formatMoney(total);
        document.getElementById("shareViz").innerHTML = usableRows.map((row, idx) => `
          <div class="legend-row">
            <div class="legend-dot" style="background:${palette[idx % palette.length]}"></div>
            <div><strong>${escapeHtml(row.san)}</strong><div class="small muted">${escapeHtml(formatMoney(row.GMV))}</div></div>
            <div>${escapeHtml(formatPct(row.pct_thi_phan_online))}</div>
          </div>`).join("");
      };
      document.getElementById("downloadSection2").onclick = () => downloadCsv(payload.section2, [
        "san","thang","GMV","NMV","so_don","AOV","so_SKU_active_co_don","pct_thi_phan_online"
      ], "hoang_anh_request_marketplace_month.csv");
      document.getElementById("s2Month").addEventListener("input", render);
      render();
    }

    function initSection3() {
      const tabs = document.getElementById("s3Tabs");
      tabs.innerHTML = ["Shopee","Lazada","TikTok Shop","Tiki"].map(platform => `<button class="tab-btn ${platform === currentPlatform ? "active" : ""}" data-platform="${platform}">${platform}</button>`).join("");
      tabs.addEventListener("click", (event) => {
        const btn = event.target.closest("[data-platform]");
        if (!btn) return;
        currentPlatform = btn.dataset.platform;
        initSection3();
      });
      const data = payload.section3[currentPlatform] || { top_gmv: [], top_qty: [], bottom: [], summary: { total_gmv: 0, active_skus: 0, concentration_top10_pct: 0 } };
      document.getElementById("s3TotalGmv").textContent = formatMoney(data.summary.total_gmv);
      document.getElementById("s3ActiveSku").textContent = formatInt(data.summary.active_skus);
      document.getElementById("s3Concentration").textContent = formatPct(data.summary.concentration_top10_pct);
      const cols = [
        { key: "ma_sku", label: "Mã SKU" },
        { key: "ten_sp", label: "Tên SP" },
        { key: "nhom_sp", label: "Nhóm SP" },
        { key: "GMV", label: "GMV", numeric: true },
        { key: "NMV", label: "NMV", numeric: true },
        { key: "so_luong_ban", label: "Số lượng", numeric: true },
        { key: "so_don", label: "Số đơn", numeric: true },
        { key: "AOV", label: "AOV", numeric: true },
        { key: "ty_le_huy_hoan", label: "% huỷ hoàn", numeric: true }
      ];
      const fmts = { GMV: formatMoney, NMV: formatMoney, so_luong_ban: formatQty, so_don: formatInt, AOV: formatMoney, ty_le_huy_hoan: formatPct };
      renderTable("table3a", cols, data.top_gmv, fmts);
      renderTable("table3b", cols, data.top_qty, fmts);
      renderTable("table3c", cols, data.bottom, fmts);
      document.getElementById("downloadSection3").onclick = () => {
        const rows = []
          .concat(data.top_gmv.map(row => ({ list_type: "top_gmv", platform: currentPlatform, ...row })))
          .concat(data.top_qty.map(row => ({ list_type: "top_qty", platform: currentPlatform, ...row })))
          .concat(data.bottom.map(row => ({ list_type: "bottom_orders", platform: currentPlatform, ...row })));
        downloadCsv(rows, ["list_type","platform","ma_sku","ten_sp","nhom_sp","GMV","NMV","so_luong_ban","so_don","AOV","ty_le_huy_hoan"], `hoang_anh_request_top_sku_${currentPlatform.toLowerCase().replace(/\s+/g, "_")}.csv`);
      };
    }

    function initSection4() {
      setOptions(document.getElementById("s4Marketplace"), payload.filters.dailyMarketplaces, "Tất cả sàn");
      const rerender = () => {
        const market = document.getElementById("s4Marketplace").value;
        const limit = Number(document.getElementById("s4Limit").value || 60);
        const rows = payload.section4.filter(row => !market || row.san === market).slice(0, limit);
        renderTable("table4", [
          { key: "ngay", label: "Ngày" },
          { key: "san", label: "Sàn" },
          { key: "GMV", label: "GMV", numeric: true },
          { key: "NMV", label: "NMV", numeric: true },
          { key: "so_don_thanh_cong", label: "Đơn thành công", numeric: true },
          { key: "so_don_huy", label: "Đơn huỷ", numeric: true },
          { key: "AOV", label: "AOV", numeric: true },
          { key: "so_SKU_distinct", label: "SKU distinct", numeric: true },
          { key: "ty_le_huy_hoan", label: "% huỷ hoàn", numeric: true }
        ], rows, {
          GMV: formatMoney, NMV: formatMoney, so_don_thanh_cong: formatInt, so_don_huy: formatInt,
          AOV: formatMoney, so_SKU_distinct: formatInt, ty_le_huy_hoan: formatPct
        });
        document.getElementById("downloadSection4").onclick = () => downloadCsv(
          payload.section4.filter(row => !market || row.san === market),
          ["ngay","san","GMV","NMV","so_don_thanh_cong","so_don_huy","AOV","so_SKU_distinct","ty_le_huy_hoan"],
          "hoang_anh_request_daily_marketplace.csv"
        );
      };
      ["s4Marketplace","s4Limit"].forEach(id => document.getElementById(id).addEventListener("input", rerender));
      rerender();
    }

    function initSection5() {
      setOptions(document.getElementById("s5Month"), payload.filters.months, "Tất cả tháng");
      setOptions(document.getElementById("s5Channel"), payload.filters.channels, "Tất cả kênh");
      const rerender = () => {
        const month = document.getElementById("s5Month").value;
        const channel = document.getElementById("s5Channel").value;
        const search = document.getElementById("s5Search").value.trim().toLowerCase();
        const rows = payload.section5.filter(row =>
          (!month || row.thang === month) &&
          (!channel || row.kenh === channel) &&
          (!search || row.ma_sku.toLowerCase().includes(search) || row.ten_sp.toLowerCase().includes(search))
        );
        renderTable("table5", [
          { key: "thang", label: "Tháng" },
          { key: "ma_sku", label: "Mã SKU" },
          { key: "ten_sp", label: "Tên SP" },
          { key: "nhom_sp", label: "Nhóm / dòng SP" },
          { key: "kenh", label: "Kênh" },
          { key: "GMV", label: "GMV", numeric: true },
          { key: "NMV", label: "NMV", numeric: true },
          { key: "so_luong_ban", label: "Số lượng", numeric: true },
          { key: "AOV_sku", label: "AOV SKU", numeric: true },
          { key: "pct_GMV_tren_thang", label: "% GMV / tháng", numeric: true },
          { key: "rank_GMV", label: "Rank GMV", numeric: true },
          { key: "ty_le_huy_hoan", label: "% huỷ hoàn", numeric: true }
        ], rows.slice(0, 300), {
          GMV: formatMoney, NMV: formatMoney, so_luong_ban: formatQty, AOV_sku: formatMoney,
          pct_GMV_tren_thang: formatPct, rank_GMV: formatInt, ty_le_huy_hoan: formatPct
        });
        document.getElementById("downloadSection5").onclick = () => downloadCsv(rows, [
          "thang","ma_sku","ten_sp","nhom_sp","kenh","GMV","NMV","so_luong_ban","AOV_sku","pct_GMV_tren_thang","rank_GMV","ty_le_huy_hoan"
        ], "hoang_anh_request_monthly_sku_mix.csv");
      };
      ["s5Month","s5Channel","s5Search"].forEach(id => document.getElementById(id).addEventListener("input", rerender));
      rerender();
    }

    initMeta();
    initSection1();
    initSection2();
    initSection3();
    initSection4();
    initSection5();
  </script>
</body>
</html>
"""


def main():
    records, _ = build_records()
    dataset = aggregate(records)
    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(dataset, ensure_ascii=False))
    PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    PUBLISHED_FILE.write_text(html, encoding="utf-8")
    print(f"Generated: {OUTPUT_FILE}")
    print(f"Published: {PUBLISHED_FILE}")
    print(f"Records: {dataset['meta']['recordCount']}")
    print(f"Date range: {dataset['meta']['minDate']} -> {dataset['meta']['maxDate']}")


if __name__ == "__main__":
    main()
