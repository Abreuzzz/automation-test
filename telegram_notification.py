"""Envio de mensagens via Telegram com base nos resultados do módulo ``automation``.

Este módulo reutiliza o fluxo de automação para coletar as vagas disponíveis e
publica um resumo em um chat do Telegram.
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, Iterable, List, Optional

import requests

import automation


TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


def format_spot_summary(spots: Iterable[Dict]) -> str:
    """Gera uma mensagem amigável para envio ao Telegram."""

    spots_list = list(spots)
    if not spots_list:
        return "Nenhuma vaga disponível encontrada no período consultado."

    lines: List[str] = [
        "Aulas disponíveis:",
        "",
    ]

    for spot in spots_list:
        event_name = spot.get("event_name", "Aula")
        event_hour = spot.get("event_hour", "Horário não informado")
        instructor = spot.get("instructor_nickname") or spot.get("instructor_name") or "Instrutor"
        code = spot.get("spot_code") or "Código indisponível"
        duration = spot.get("duration_time")

        line_parts = [f"• {event_name} ({event_hour})"]
        line_parts.append(f"Instrutor: {instructor}")
        line_parts.append(f"Bike: {code}")
        if duration:
            line_parts.append(f"Duração: {duration}")

        lines.append(" | ".join(line_parts))

    return "\n".join(lines)


def send_telegram_message(
    token: str,
    chat_id: str,
    message: str,
    *,
    session: Optional[requests.Session] = None,
) -> Dict:
    """Envia uma mensagem de texto para o Telegram."""

    if not token:
        raise ValueError("Token do bot do Telegram não informado.")
    if not chat_id:
        raise ValueError("Chat ID do Telegram não informado.")

    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }

    internal_session = session or requests.Session()
    response = internal_session.post(
        TELEGRAM_API_URL.format(token=token),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()

    if session is None:
        internal_session.close()

    return response.json()


def _build_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos de linha de comando."""

    parser = argparse.ArgumentParser(
        description=(
            "Coleta as aulas disponíveis e envia o resumo via Telegram. "
            "Use --dry-run para apenas exibir a mensagem localmente."
        )
    )
    parser.add_argument(
        "--token",
        help=(
            "Token do bot do Telegram. Substitui a variável de ambiente "
            "TELEGRAM_BOT_TOKEN quando informado."
        ),
    )
    parser.add_argument(
        "--chat-id",
        help=(
            "Identificador do chat do Telegram. Substitui a variável de ambiente "
            "TELEGRAM_CHAT_ID quando informado."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas imprime o resumo gerado sem enviar mensagem ao Telegram.",
    )

    return parser


def main() -> None:
    """Executa o fluxo completo e envia ou imprime a mensagem."""

    args = _build_parser().parse_args()

    token = args.token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = args.chat_id or os.environ.get("TELEGRAM_CHAT_ID")

    available_spots = automation.run_automation()
    message = format_spot_summary(available_spots)

    if args.dry_run:
        print(message)
        return

    send_telegram_message(token or "", chat_id or "", message)


if __name__ == "__main__":
    main()
