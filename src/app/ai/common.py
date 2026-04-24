from __future__ import annotations

import json
import re
from typing import Any


def clean_json(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*```\s*$", "", t)
    return t.strip()


def _extract_outer_json_object(s: str) -> str:
    """Recorta do primeiro '{' ao último '}' (resposta LLM com texto extra)."""
    a = s.find("{")
    b = s.rfind("}")
    if 0 <= a < b:
        return s[a : b + 1]
    return s


def repair_json_string_controls(raw: str) -> str:
    """
    Escapa caracteres de controle ASCII inválidos dentro de literais JSON entre aspas.
    Modelos costumam colocar newline real em `feedback_star`, que quebra json.loads.
    """
    out: list[str] = []
    in_string = False
    escaped = False
    for ch in raw:
        if escaped:
            out.append(ch)
            escaped = False
            continue
        if in_string:
            if ch == "\\":
                out.append(ch)
                escaped = True
                continue
            if ch == '"':
                in_string = False
                out.append(ch)
                continue
            o = ord(ch)
            if o < 32:
                if ch == "\t":
                    out.append("\\t")
                elif ch == "\n":
                    out.append("\\n")
                elif ch == "\r":
                    out.append("\\r")
                else:
                    out.append(f"\\u{o:04x}")
                continue
            out.append(ch)
            continue
        if ch == '"':
            in_string = True
        out.append(ch)
    return "".join(out)


def loads_json(text: str) -> dict[str, Any]:
    t = _extract_outer_json_object(clean_json(text))
    err: json.JSONDecodeError | None = None
    for candidate in (t, repair_json_string_controls(t)):
        try:
            data = json.loads(candidate)
        except json.JSONDecodeError as e:
            err = e
            continue
        if not isinstance(data, dict):
            raise ValueError("Resposta da IA não é um objeto JSON.")
        return data
    assert err is not None
    raise ValueError(f"JSON inválido na resposta da IA: {err}") from err

