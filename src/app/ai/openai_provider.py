from __future__ import annotations

import base64
from typing import Any

from openai import OpenAI

from .common import loads_json


def _img_b64(img_bytes: bytes) -> str:
    return base64.b64encode(img_bytes).decode("utf-8")


def extract_json_from_images_openai(
    *,
    api_key: str,
    model: str,
    images: list[tuple[str, bytes]],
    prompt: str,
) -> dict[str, Any]:
    client = OpenAI(api_key=api_key)

    # Alguns ambientes usam chaves "restritas" que não têm o escopo `api.responses.write`.
    # Para maximizar compatibilidade, usamos Chat Completions (vision) aqui.
    content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
    for _, img_bytes in images:
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{_img_b64(img_bytes)}"},
            }
        )

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "Responda APENAS com JSON válido. Sem markdown. Sem texto extra.",
            },
            {"role": "user", "content": content},
        ],
    )

    text = (resp.choices[0].message.content or "").strip()
    return loads_json(text)


def json_from_text_openai(*, api_key: str, model: str, prompt: str) -> dict[str, Any]:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Responda APENAS com JSON válido. Sem markdown. Sem texto extra."},
            {"role": "user", "content": prompt},
        ],
    )
    text = (resp.choices[0].message.content or "").strip()
    return loads_json(text)

