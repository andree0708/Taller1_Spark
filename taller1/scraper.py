"""
scraper.py — Descarga el Online Retail Dataset desde UCI ML Repository.

Estrategia:
  1. Descarga el ZIP directamente desde la URL estática del repositorio UCI.
  2. Extrae el archivo .xlsx del ZIP.
  3. Convierte el .xlsx a CSV usando openpyxl (sin pandas) para que PySpark
     pueda leerlo directamente con spark.read.csv().

Fuente: https://archive.ics.uci.edu/dataset/352/online+retail
"""

import os
import io
import zipfile
import csv
import requests
import openpyxl

# ── Configuración ─────────────────────────────────────────────────────────────
DATASET_URL  = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
ZIP_PATH     = os.path.join(DATA_DIR, "online_retail.zip")
XLSX_NAME    = "Online Retail.xlsx"          # nombre dentro del ZIP
XLSX_PATH    = os.path.join(DATA_DIR, XLSX_NAME)
CSV_PATH     = os.path.join(DATA_DIR, "online_retail.csv")
# ──────────────────────────────────────────────────────────────────────────────


def download_zip(url: str, dest: str) -> None:
    """Descarga el ZIP con barra de progreso básica."""
    print(f"[1/3] Descargando dataset desde:\n      {url}")
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        chunk_size = 1024 * 64  # 64 KB

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(f"\r   {pct:5.1f}%  ({downloaded // 1024} KB / {total // 1024} KB)", end="")
    print(f"\n   Guardado en: {dest}")


def extract_xlsx(zip_path: str, xlsx_name: str, out_dir: str) -> str:
    """Extrae el archivo .xlsx del ZIP."""
    print(f"[2/3] Extrayendo '{xlsx_name}' del ZIP...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Busca el xlsx sin importar la capitalización
        matches = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
        if not matches:
            raise FileNotFoundError(f"No se encontró ningún .xlsx dentro de {zip_path}")
        target = matches[0]
        zf.extract(target, out_dir)
        extracted_path = os.path.join(out_dir, target)
    print(f"   Extraído en: {extracted_path}")
    return extracted_path


def xlsx_to_csv(xlsx_path: str, csv_path: str) -> None:
    """Convierte el .xlsx a CSV plano usando openpyxl."""
    print(f"[3/3] Convirtiendo XLSX → CSV...")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    row_count = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        for row in ws.iter_rows(values_only=True):
            writer.writerow(["" if v is None else str(v) for v in row])
            row_count += 1
            if row_count % 50_000 == 0:
                print(f"   ... {row_count:,} filas procesadas")

    wb.close()
    size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"   CSV guardado en: {csv_path}")
    print(f"   Total filas: {row_count - 1:,} (sin cabecera)  |  Tamaño: {size_mb:.1f} MB")


def main():
    print("=" * 60)
    print("  Online Retail Dataset — Descargador")
    print("=" * 60)

    # Paso 1: Descargar ZIP (omitir si ya existe)
    if os.path.exists(ZIP_PATH):
        print(f"[1/3] ZIP ya descargado, omitiendo: {ZIP_PATH}")
    else:
        download_zip(DATASET_URL, ZIP_PATH)

    # Paso 2: Extraer XLSX
    if os.path.exists(XLSX_PATH):
        print(f"[2/3] XLSX ya extraído, omitiendo: {XLSX_PATH}")
    else:
        extract_xlsx(ZIP_PATH, XLSX_NAME, DATA_DIR)

    # Paso 3: Convertir a CSV
    if os.path.exists(CSV_PATH):
        print(f"[3/3] CSV ya existe, omitiendo: {CSV_PATH}")
    else:
        xlsx_to_csv(XLSX_PATH, CSV_PATH)

    print("\n✓ Dataset listo para PySpark:", CSV_PATH)
    print("=" * 60)


if __name__ == "__main__":
    main()
