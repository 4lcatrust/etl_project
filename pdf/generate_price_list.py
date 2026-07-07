"""Generate the demo price-list PDF (pdf/price_list.pdf) parsed by the Phase G docling job.

A clean 1-page digital PDF with a bordered product-price table so docling's table-structure
model detects it reliably. Header row uses the canonical column names the vld expects
(sku, product_name, unit_price, category). Row SKU-0014 has a negative unit_price to
exercise the quarantine path (positive_unit_price rule), like the API source's bad rows.

Run:  python pdf/generate_price_list.py   (needs reportlab)
"""
import os

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "price_list.pdf")

HEADER = ["sku", "product_name", "unit_price", "category"]
ROWS = [
    ["SKU-0001", "Wireless Mouse",       "18.50",  "peripherals"],
    ["SKU-0002", "Mechanical Keyboard",  "72.00",  "peripherals"],
    ["SKU-0003", "USB-C Hub",            "34.99",  "accessories"],
    ["SKU-0004", "1080p Webcam",         "45.00",  "peripherals"],
    ["SKU-0005", "Noise-Cancel Headset", "129.90", "audio"],
    ["SKU-0006", "Laptop Stand",         "27.25",  "accessories"],
    ["SKU-0007", "27in Monitor",         "219.00", "displays"],
    ["SKU-0008", "Portable SSD 1TB",     "98.49",  "storage"],
    ["SKU-0009", "HDMI Cable 2m",        "8.99",   "cables"],
    ["SKU-0010", "Bluetooth Speaker",    "54.00",  "audio"],
    ["SKU-0011", "LED Desk Lamp",        "39.90",  "lighting"],
    ["SKU-0014", "Broken Price Item",    "-5.00",  "misc"],  # bad row -> quarantine
]


def main() -> None:
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(OUT, pagesize=A4, title="Supplier Price List")
    elements = [
        Paragraph("Supplier Price List — Q3 2026", styles["Title"]),
        Spacer(1, 0.5 * cm),
    ]
    table = Table([HEADER] + ROWS, repeatRows=1)
    table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.75, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dddddd")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (2, 1), (2, -1), "RIGHT"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(table)
    doc.build(elements)
    print(f"wrote {OUT} ({len(ROWS)} rows)")


if __name__ == "__main__":
    main()
