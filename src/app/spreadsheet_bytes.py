"""Validação leve de bytes antes de `read_excel` — evita erro críptico do xlrd (BOF) em CSV/.env/TOML."""

from __future__ import annotations


def looks_like_html_table_export(b: bytes) -> bool:
    head = (b or b"")[:4096].lstrip().lower()
    return head.startswith(b"<") or b"<html" in head or b"<table" in head or b"<style" in head


def _stripped(b: bytes) -> bytes:
    return (b or b"").lstrip(b"\xef\xbb\xbf")


def is_ooxml_zip(b: bytes) -> bool:
    s = _stripped(b)
    return len(s) >= 4 and s[:2] == b"PK"


def is_ole_xls(b: bytes) -> bool:
    s = _stripped(b)
    return s.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")


def assert_excel_or_html_bytes(file_name: str, b: bytes) -> None:
    """
    Garante que o conteúdo parece .xlsx (ZIP OOXML), .xls (OLE) ou HTML exportado.
    Caso contrário, levanta mensagem clara (ex.: CSV ou secrets renomeados).
    """
    if b is None or len(b) == 0:
        raise ValueError(f"Arquivo '{file_name}' está vazio.")
    if looks_like_html_table_export(b):
        return
    raw = _stripped(b)
    if is_ooxml_zip(raw) or is_ole_xls(raw):
        return
    snippet = raw[:64].decode("utf-8", errors="replace").replace("\r", " ").replace("\n", " ").strip()
    if len(snippet) > 48:
        snippet = snippet[:45] + "…"
    raise ValueError(
        f"O arquivo '{file_name}' não é um Excel válido (.xlsx / .xls) nem export HTML do Excel. "
        f"Parece texto ou outro formato (início: {snippet!r}). "
        "Envie a planilha real exportada/salva pelo Excel, ou um .csv aberto no Excel e salvo como .xlsx — "
        "não use arquivos .env, secrets, SQL ou CSV renomeados para .xlsx."
    )
