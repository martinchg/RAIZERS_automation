"""
llm_client.py : Factory LLM (Gemini / OpenAI).

Retourne un dict avec :
  - provider   : "gemini" | "openai"
  - model      : nom du modèle
  - text_call  : Callable[[str], str]
  - multimodal_call : Callable[[str, List[Image]], str]
"""

import base64
import io
import logging
import os
from typing import List

from PIL import Image

logger = logging.getLogger(__name__)


def get_llm_client(model_override: dict | None = None, preferred_provider: str | None = None) -> dict:
    """Retourne les callables texte / multimodal et le nom du modèle.

    model_override : surcharge optionnelle des noms de modèles, ex.
        {"openai": "gpt-4o-mini", "gemini": "gemini-2.5-flash-lite"}
    preferred_provider : "gemini" | "openai" pour forcer l'ordre de sélection.
    """
    openai_key = os.environ.get("OPENAI_API_KEY")
    gemini_key = os.environ.get("GEMINI_API_KEY")
    override = model_override or {}
    provider_order = [preferred_provider] if preferred_provider in {"openai", "gemini"} else ["openai", "gemini"]
    if preferred_provider in {"openai", "gemini"}:
        provider_order.append("gemini" if preferred_provider == "openai" else "openai")

    for provider in provider_order:
        if provider == "openai" and openai_key:
            try:
                from openai import OpenAI
            except ImportError:
                logger.warning("OpenAI API key présente mais package `openai` indisponible, fallback Gemini")
                continue
            else:
                client = OpenAI(api_key=openai_key)
                model = override.get("openai") or os.environ.get("OPENAI_MODEL", "gpt-4o")

                def text_call(prompt: str) -> str:
                    r = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0,
                        response_format={"type": "json_object"},
                    )
                    return r.choices[0].message.content

                def multimodal_call(prompt: str, images: List[Image.Image]) -> str:
                    content = [{"type": "text", "text": prompt}]
                    for image in images:
                        buffer = io.BytesIO()
                        image.save(buffer, format="PNG")
                        data = base64.b64encode(buffer.getvalue()).decode("ascii")
                        content.append(
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{data}"},
                            }
                        )
                    r = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": content}],
                        temperature=0,
                        response_format={"type": "json_object"},
                    )
                    return r.choices[0].message.content

                logger.info(f"LLM: OpenAI ({model})")
                return {
                    "provider": "openai",
                    "model": model,
                    "text_call": text_call,
                    "multimodal_call": multimodal_call,
                }

        if provider == "gemini" and gemini_key:
            from google import genai
            client = genai.Client(api_key=gemini_key)
            model = override.get("gemini") or os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

            def text_call(prompt: str) -> str:
                r = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={"temperature": 0, "response_mime_type": "application/json"},
                )
                return r.text

            def multimodal_call(prompt: str, images: List[Image.Image]) -> str:
                contents = [prompt, *images]
                r = client.models.generate_content(
                    model=model,
                    contents=contents,
                    config={"temperature": 0, "response_mime_type": "application/json"},
                )
                return r.text

            logger.info(f"LLM: Gemini ({model})")
            return {
                "provider": "gemini",
                "model": model,
                "text_call": text_call,
                "multimodal_call": multimodal_call,
            }

    raise ValueError("Aucune clé API trouvée. Ajoute GEMINI_API_KEY ou OPENAI_API_KEY dans .env")
