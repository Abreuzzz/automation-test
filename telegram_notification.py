"""Envio de mensagens via Telegram com base nos resultados do módulo ``automation``.

Este módulo reutiliza o fluxo de automação para coletar as vagas disponíveis e
publica um resumo em um chat do Telegram.
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests

import automation


TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_MESSAGE_LIMIT = 4096


WEEKDAY_LABELS = [
    "Segunda-feira",
    "Terça-feira",
    "Quarta-feira",
    "Quinta-feira",
    "Sexta-feira",
    "Sábado",
    "Domingo",
]


def _split_message(message: str, *, limit: int = TELEGRAM_MESSAGE_LIMIT) -> List[str]:
    """Divide uma mensagem longa em pedaços aceitos pelo Telegram."""

    if len(message) <= limit:
        return [message]

    chunks: List[str] = []
    current_lines: List[str] = []
    current_len = 0

    for line in message.splitlines():
        line_len = len(line)

        if line_len > limit:
            # Garante que linhas gigantes não quebrem o envio fatiando-as diretamente.
            if current_lines:
                chunks.append("\n".join(current_lines))
                current_lines = []
                current_len = 0

            for start in range(0, line_len, limit):
                chunks.append(line[start : start + limit])
            continue

        additional = line_len if not current_lines else line_len + 1
        if current_lines and current_len + additional > limit:
            chunks.append("\n".join(current_lines))
            current_lines = [line]
            current_len = line_len
        else:
            if current_lines:
                current_len += 1 + line_len
            else:
                current_len = line_len
            current_lines.append(line)

    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks


def _parse_start_time(value: Optional[str]) -> Optional[datetime]:
    """Converte um ``start_time`` ISO em ``datetime`` quando possível."""

    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _build_instructor_label(spot: Dict[str, Any]) -> str:
    """Monta um texto amigável para o instrutor."""

    nickname = spot.get("instructor_nickname") or ""
    name = spot.get("instructor_name") or ""

    if nickname and name and nickname.lower() not in name.lower():
        return f"{name} ({nickname})"

    return nickname or name or "Instrutor"


def format_spot_summary(spots: Iterable[Dict[str, Any]]) -> str:
    """Gera uma mensagem amigável para envio ao Telegram."""

    spots_list = sorted(
        list(spots),
        key=lambda item: item.get("start_time") or item.get("event_hour") or "",
    )

    if not spots_list:
        return "Nenhuma vaga disponível encontrada no período consultado."

    grouped_spots: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for spot in spots_list:
        start_dt = _parse_start_time(spot.get("start_time"))
        if start_dt is not None:
            key = start_dt.date().isoformat()
        else:
            key = "sem-data"
        grouped_spots[key].append(spot)

    ordered_keys = sorted(
        grouped_spots.keys(),
        key=lambda key: (key == "sem-data", key),
    )

    lines: List[str] = [
        "🏋️‍♀️ Vagas de aula liberadas!",
        "",
        "Olha só o que encontramos nas próximas duas semanas:",
        "",
    ]

    for key in ordered_keys:
        day_spots = grouped_spots[key]
        representative = day_spots[0]
        start_dt = _parse_start_time(representative.get("start_time"))

        if start_dt is None:
            header = "📅 Data não informada"
        else:
            weekday = WEEKDAY_LABELS[start_dt.weekday()]
            header = f"📅 {start_dt.strftime('%d/%m/%Y')} ({weekday})"

        lines.append(header)

        for spot in day_spots:
            start_dt = _parse_start_time(spot.get("start_time"))
            event_hour = spot.get("event_hour")
            if start_dt is not None and not event_hour:
                event_hour = start_dt.strftime("%H:%M")

            event_name = spot.get("event_name") or "Aula"
            instructor = _build_instructor_label(spot)
            code = spot.get("spot_code") or "Código indisponível"
            duration = spot.get("duration_time")
            tagline = spot.get("instructor_tagline")

            lines.append(f"   • {event_hour or 'Horário não informado'} — {event_name}")
            lines.append(f"     Instrutor: {instructor}")
            if tagline:
                lines.append(f"     ✨ {tagline}")
            lines.append(f"     Bike liberada: {code}")
            if duration:
                lines.append(f"     Duração: {duration}")
            lines.append("")

    lines.append("Boas pedaladas! 🚴‍♀️")

    return "\n".join(lines).strip()


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
        "disable_web_page_preview": True,
    }

    responses: List[Dict[str, Any]] = []
    messages = _split_message(message)

    internal_session = session or requests.Session()

    try:
        for chunk in messages:
            payload["text"] = chunk
            response = internal_session.post(
                TELEGRAM_API_URL.format(token=token),
                json=payload,
                timeout=30,
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:  # type: ignore[attr-defined]
                try:
                    error_payload = response.json()
                    detail = error_payload.get("description") or error_payload
                except ValueError:
                    detail = response.text
                raise requests.HTTPError(
                    f"Falha ao enviar mensagem ao Telegram: {detail}",
                    response=response,
                    request=exc.request,
                ) from exc

            responses.append(response.json())
    finally:
        if session is None:
            internal_session.close()

    return responses[-1] if responses else {}


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

    result = automation.run_automation()
    available_spots = result.spots
    message = format_spot_summary(available_spots)

    execution_report = (
        "Tempo total da automação: "
        f"{result.elapsed_seconds:.2f} segundos (início: {result.started_at.isoformat()} | "
        f"fim: {result.finished_at.isoformat()})."
    )

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary_file:
            summary_file.write(f"{execution_report}\n\n{message}\n")

    if args.dry_run:
        print(message)
        print()
        print(execution_report)
        return

    if not available_spots:
        print(message)
        print()
        print(execution_report)
        return

    send_telegram_message(token or "", chat_id or "", message)
    print(execution_report)


if __name__ == "__main__":
    main()

