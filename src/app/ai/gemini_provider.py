from __future__ import annotations

from typing import Any

import google.generativeai as genai

from .common import loads_json


def _candidate_models(preferred: str) -> list[str]:
    # Ordem: preferido -> modelos comuns. A disponibilidade varia por conta/região.
    base = [
        preferred,
        # aliases comuns
        "gemini-1.5-flash-latest",
        "gemini-1.5-pro-latest",
        "gemini-1.5-flash-002",
        "gemini-1.5-pro-002",
        "gemini-2.0-flash",
        "gemini-2.0-flash-lite",
        "gemini-1.5-flash",
        "gemini-1.5-pro",
    ]
    seen: set[str] = set()
    out: list[str] = []
    for m in base:
        if m and m not in seen:
            out.append(m)
            seen.add(m)
    return out


def _generate_with_first_working_model(
    *,
    api_key: str,
    preferred_model: str,
    parts: Any,
) -> tuple[str, str]:
    genai.configure(api_key=api_key)

    # Se disponível, tenta descobrir modelos válidos (pode falhar por permissão; é ok).
    discovered: list[str] = []
    try:
        for m in genai.list_models():
            name = getattr(m, "name", "") or ""
            methods = getattr(m, "supported_generation_methods", []) or []
            if "generateContent" in methods and "gemini" in name:
                discovered.append(name.replace("models/", ""))
    except Exception:
        discovered = []

    last: Exception | None = None
    for model in _candidate_models(preferred_model) + discovered:
        try:
            m = genai.GenerativeModel(model)
            resp = m.generate_content(parts)
            text = getattr(resp, "text", "") or ""
            return text, model
        except Exception as e:
            last = e
            continue
    raise RuntimeError(f"Nenhum modelo Gemini funcionou. Último erro: {last}")


def extract_json_from_images_gemini(
    *,
    api_key: str,
    model: str,
    images: list[tuple[str, bytes]],
    prompt: str,
) -> dict[str, Any]:
    parts: list[Any] = [prompt]
    for _, img_bytes in images:
        parts.append({"mime_type": "image/png", "data": img_bytes})

    text, _model_used = _generate_with_first_working_model(
        api_key=api_key,
        preferred_model=model,
        parts=parts,
    )
    payload = loads_json(text)
    # Metadata opcional para auditoria
    payload["_model_used"] = _model_used
    return payload


def json_from_text_gemini(*, api_key: str, model: str, prompt: str) -> dict[str, Any]:
    text, _model_used = _generate_with_first_working_model(
        api_key=api_key,
        preferred_model=model,
        parts=prompt,
    )
    return loads_json(text)

