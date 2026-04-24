import csv
import json
import re
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

dumps_dir = Path("dumps")
out_dir = Path("out")
out_dir.mkdir(parents=True, exist_ok=True)

def classify_kind(name: str):
    lower = name.lower()
    if lower.startswith("stores") or lower.startswith("store"):
        return "store"
    if "pricefull" in lower:
        return "price_full"
    if "promofull" in lower:
        return "promo_full"
    if lower.startswith("promo") or "promo" in lower:
        return "promo"
    return "other"

def retailer_from_path(path: Path):
    parent = path.parent.name.lower()
    if "rami" in parent:
        return "ramilevy"
    if "shuf" in parent:
        return "shufersal"
    if "yoh" in parent:
        return "yohananof"
    return parent or "unknown"

def parse_identity(filename: str):
    shufersal_full_patterns = [
        (r"(?i)pricefull(\d+)-(\d+)-(\d+)-(\d{8})-(\d{6})", "price_full"),
        (r"(?i)promofull(\d+)-(\d+)-(\d+)-(\d{8})-(\d{6})", "promo_full"),
        (r"(?i)promo(\d+)-(\d+)-(\d+)-(\d{8})-(\d{6})", "promo"),
    ]
    for pattern, kind in shufersal_full_patterns:
        m = re.search(pattern, filename)
        if m:
            chain_id = m.group(1)
            store_id = m.group(3)
            source_ts = f"{m.group(4)}{m.group(5)}"
            return kind, chain_id, store_id, source_ts

    patterns = [
        (r"(?i)pricefull(\d+)-(\d+)-(\d+)", "price_full"),
        (r"(?i)promofull(\d+)-(\d+)-(\d+)", "promo_full"),
        (r"(?i)promo(\d+)-(\d+)-(\d+)", "promo"),
        (r"(?i)stores(\d+)-(\d+)-(\d+)", "store"),
        (r"(?i)stores(\d+)", "store"),
    ]
    for pattern, kind in patterns:
        m = re.search(pattern, filename)
        if m:
            chain_id = m.group(1)
            store_id = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
            source_ts = m.group(3) if m.lastindex and m.lastindex >= 3 else ""
            return kind, chain_id, store_id, source_ts
    return classify_kind(filename), "", "", ""

inventory_rows = []
summary = defaultdict(lambda: Counter())
observed_store_ids = defaultdict(lambda: defaultdict(set))
download_retailer_status_rows = []
status_csv_path = out_dir / "download_retailer_status.csv"
if status_csv_path.exists():
    with status_csv_path.open(encoding="utf-8", newline="") as handle:
        download_retailer_status_rows = list(csv.DictReader(handle))
for path in sorted(dumps_dir.rglob("*")):
    if not path.is_file():
        continue
    retailer = retailer_from_path(path)
    kind, chain_id, store_id, source_ts = parse_identity(path.name)
    size_bytes = path.stat().st_size
    inventory_rows.append({
        "retailer": retailer,
        "kind": kind,
        "chain_id": chain_id,
        "store_id": store_id,
        "source_ts": source_ts,
        "relative_path": str(path.as_posix()),
        "filename": path.name,
        "size_bytes": size_bytes,
    })
    summary[retailer][f"{kind}_files"] += 1
    summary[retailer]["total_files"] += 1
    summary[retailer]["total_bytes"] += size_bytes
    if store_id:
        observed_store_ids[retailer][kind].add(store_id)

with (out_dir / "downloaded_dump_inventory.csv").open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["retailer", "kind", "chain_id", "store_id", "source_ts", "relative_path", "filename", "size_bytes"],
    )
    writer.writeheader()
    for row in inventory_rows:
        writer.writerow(row)

json.dump(
    {
        "retailers": ["shufersal", "ramilevy", "yohananof"],
        "downloaded_file_count": len(inventory_rows),
        "downloaded_file_summary_by_retailer": {
            retailer: dict(counts) for retailer, counts in sorted(summary.items())
        },
        "downloaded_store_ids_by_retailer_and_kind": {
            retailer: {
                kind: sorted(store_ids)
                for kind, store_ids in sorted(kind_map.items())
                if store_ids
            }
            for retailer, kind_map in sorted(observed_store_ids.items())
        },
        "per_retailer_download_status": download_retailer_status_rows,
        "failed_retailer_downloads": [
            row["retailer"]
            for row in download_retailer_status_rows
            if row.get("status") not in ("", "success")
        ],
        "timed_out_retailer_downloads": [
            row["retailer"]
            for row in download_retailer_status_rows
            if row.get("status") == "timeout"
        ],
    },
    (out_dir / "download_stage_summary.json").open("w", encoding="utf-8"),
    ensure_ascii=False,
    indent=2,
)
