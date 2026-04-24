from __future__ import annotations

from typing import Any, Literal

from ..config import Settings
from .gemini_provider import extract_json_from_images_gemini, json_from_text_gemini
from .openai_provider import extract_json_from_images_openai, json_from_text_openai

Provider = Literal["auto", "gemini", "openai"]


def _shorten_provider_error(provider: str, exc: Exception) -> str:
    msg = str(exc)
    if provider == "openai" and (
        "insufficient_quota" in msg
        or "429" in msg
        or "quota" in msg.lower()
    ):
        return "OpenAI: cota/plano esgotado (429). Ajuste billing em platform.openai.com ou use só Gemini."
    if provider == "gemini" and "Invalid control character" in msg:
        return (
            "Gemini: JSON com caractere de controle inválido (texto com quebra de linha "
            "dentro do JSON). Tente de novo; se persistir, escolha o provedor OpenAI com cota ativa."
        )
    # Evita dict gigante de erro HTTP no Streamlit
    if len(msg) > 220:
        msg = msg[:217] + "..."
    return f"{provider}: {msg}"


def _providers_in_order(selected: Provider) -> list[Provider]:
    if selected == "auto":
        return ["gemini", "openai"]
    return [selected]


def extract_json_from_images(
    *,
    settings: Settings,
    provider: Provider,
    images: list[tuple[str, bytes]],
    prompt: str,
) -> tuple[dict[str, Any], str, str]:
    errors: list[str] = []
    for p in _providers_in_order(provider):
        try:
            if p == "gemini":
                if not settings.google_api_key:
                    errors.append("gemini: sem GOOGLE_API_KEY")
                    continue
                payload = extract_json_from_images_gemini(
                    api_key=settings.google_api_key,
                    model=settings.gemini_model,
                    images=images,
                    prompt=prompt,
                )
                # O provider pode ter escolhido um modelo alternativo internamente.
                # Para auditoria, tentamos refletir o modelo realmente usado se vier no payload.
                model_used = str(payload.pop("_model_used", settings.gemini_model)) if isinstance(payload, dict) else settings.gemini_model
                return payload, "gemini", model_used
            if p == "openai":
                if not settings.openai_api_key:
                    errors.append("openai: sem OPENAI_API_KEY")
                    continue
                payload = extract_json_from_images_openai(
                    api_key=settings.openai_api_key,
                    model=settings.openai_model,
                    images=images,
                    prompt=prompt,
                )
                return payload, "openai", settings.openai_model
        except Exception as e:
            errors.append(_shorten_provider_error(p, e))
            continue
    details = " | ".join(errors) if errors else "nenhum provedor tentado"
    raise RuntimeError(f"Falha em todos os provedores ({details})")


def json_from_text(
    *,
    settings: Settings,
    provider: Provider,
    prompt: str,
) -> tuple[dict[str, Any], str, str]:
    errors: list[str] = []
    for p in _providers_in_order(provider):
        try:
            if p == "gemini":
                if not settings.google_api_key:
                    errors.append("gemini: sem GOOGLE_API_KEY")
                    continue
                return (
                    json_from_text_gemini(api_key=settings.google_api_key, model=settings.gemini_model, prompt=prompt),
                    "gemini",
                    settings.gemini_model,
                )
            if p == "openai":
                if not settings.openai_api_key:
                    errors.append("openai: sem OPENAI_API_KEY")
                    continue
                return (
                    json_from_text_openai(api_key=settings.openai_api_key, model=settings.openai_model, prompt=prompt),
                    "openai",
                    settings.openai_model,
                )
        except Exception as e:
            errors.append(_shorten_provider_error(p, e))
            continue
    details = " | ".join(errors) if errors else "nenhum provedor tentado"
    raise RuntimeError(f"Falha em todos os provedores ({details})")

