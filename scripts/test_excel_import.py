import os
import glob
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.app.excel_import import import_5_files_to_payload


def main() -> None:
    base = r"C:\Users\wsana\Downloads\Base de Dados"
    paths = sorted(glob.glob(os.path.join(base, "*.xls*")))
    files: list[tuple[str, bytes]] = []
    for p in paths:
        with open(p, "rb") as f:
            files.append((os.path.basename(p), f.read()))

    res = import_5_files_to_payload(files)
    vend = res.payload.get("vendedores") or []
    print("files:", [n for n, _ in files])
    print("count:", len(vend))
    print("names:", [v.get("nome") for v in vend])
    print("warnings:", res.warnings)


if __name__ == "__main__":
    main()

