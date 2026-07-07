"""Parse a PDF's first table with docling and land it as NDJSON on MinIO (Phase G).

Runs in the isolated /opt/docling-venv (invoked by dag_bronze_pdf via BashOperator), so
docling/torch never touch the Airflow env. OCR is disabled and thread counts are capped
to keep the memory/CPU footprint small (~1-1.5g for a 1-page PDF). Output mirrors the API
connector: one JSON object per line, uploaded via boto3, then read by the shared
BronzeExtract (--input_path) which validates + writes Iceberg.

Args:  --pdf_path --bucket --key
Env:   MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY  (set by the BashOperator)
"""
import argparse
import json
import os
import sys

# Cap CPU threads before importing torch/docling so the models honor it.
os.environ.setdefault("OMP_NUM_THREADS", "2")
os.environ.setdefault("MKL_NUM_THREADS", "2")


def _clean(value):
    """Normalize a docling table cell to a JSON-safe string or None."""
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf_path", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--key", required=True)
    # Canonical column names (the vld schema order). We control the PDF column order, so we
    # assign these positionally rather than trust docling's header detection.
    ap.add_argument("--columns", required=True, help="comma-separated canonical column names")
    args = ap.parse_args()
    canonical = [c.strip() for c in args.columns.split(",")]

    import torch
    torch.set_num_threads(int(os.environ.get("OMP_NUM_THREADS", "2")))

    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    # Digital PDF: skip OCR (no EasyOCR model / big memory save); keep table structure.
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False
    pipeline_options.do_table_structure = True

    converter = DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )
    result = converter.convert(args.pdf_path)
    doc = result.document

    if not doc.tables:
        print("[parse_pdf] ERROR: no tables detected in the PDF", file=sys.stderr)
        sys.exit(1)

    df = doc.tables[0].export_to_dataframe()
    if len(df.columns) != len(canonical):
        print(f"[parse_pdf] ERROR: detected {len(df.columns)} columns, expected "
              f"{len(canonical)} ({canonical})", file=sys.stderr)
        sys.exit(1)
    # Assign canonical names positionally (the PDF column order is fixed), then drop a row
    # if docling emitted the header as data (values equal the canonical names).
    df.columns = canonical
    header_leak = df.apply(
        lambda r: [str(x).strip().lower() for x in r.tolist()] == [c.lower() for c in canonical],
        axis=1,
    )
    df = df[~header_leak]
    records = df.to_dict(orient="records")

    body = "\n".join(
        json.dumps({k: _clean(v) for k, v in rec.items()}) for rec in records
    ).encode("utf-8")

    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    s3.put_object(Bucket=args.bucket, Key=args.key, Body=body)
    print(f"[parse_pdf] landed {len(records)} rows -> s3://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
