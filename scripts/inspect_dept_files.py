import pandas as pd
from pathlib import Path


def inspect(path: str) -> None:
    p = Path(path)
    print("\n==", p.name, "==")
    xls = pd.ExcelFile(path, engine="openpyxl")
    print("sheets:", xls.sheet_names)
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh, engine="openpyxl")
        print("-", sh, "rows", len(df))
        print("  cols:", list(df.columns))
        print("  head:")
        print(df.head(5).to_string(index=False))
        break


def main() -> None:
    files = [
        r"C:\Users\wsana\Downloads\Base de Dados\Performance Departamento.xlsx",
        r"C:\Users\wsana\Downloads\Base de Dados\Faturamento e Atendidos.xlsx",
    ]
    for f in files:
        inspect(f)


if __name__ == "__main__":
    main()

