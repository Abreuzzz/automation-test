"""Envio de mensagens via Telegram com base nos resultados do módulo ``automation``.

Este módulo reutiliza o fluxo de automação para coletar as vagas disponíveis e
publica um resumo em um chat do Telegram.
"""

from __future__ import annotations

import argparse
import os
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from html import escape
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


@dataclass
class FormattedSummary:
    """Empacota a mensagem formatada em HTML e em texto plano."""

    html: str
    plain_text: str


def _format_bike_codes(codes: List[str]) -> Tuple[str, str]:
    """Gera a representação das bikes em HTML (com spoiler) e texto plano."""

    if not codes:
        return (
            "Nenhuma bike com código disponível",
            "Nenhuma bike com código disponível",
        )

    joined_plain = ", ".join(codes)
    joined_html = escape(" • ".join(codes))
    return (
        f"<b>{len(codes)} bike{'s' if len(codes) > 1 else ''} livres:</b> "
        f"<tg-spoiler>{joined_html}</tg-spoiler>",
        f"{len(codes)} bike{'s' if len(codes) > 1 else ''} livres: {joined_plain}",
    )


def format_spot_summary(spots: Iterable[Dict[str, Any]]) -> FormattedSummary:
    """Gera mensagens amigáveis (HTML e texto plano) para envio e logs."""

    spots_list = sorted(
        list(spots),
        key=lambda item: item.get("start_time") or item.get("event_hour") or "",
    )

    if not spots_list:
        message = "Nenhuma vaga disponível encontrada no período consultado."
        return FormattedSummary(
            html=f"<b>{escape(message)}</b>",
            plain_text=message,
        )

    grouped_by_day: Dict[str, OrderedDict[str, Dict[str, Any]]] = {}
    day_order: List[str] = []

    for spot in spots_list:
        start_dt = _parse_start_time(spot.get("start_time"))
        day_key = start_dt.date().isoformat() if start_dt else "sem-data"

        if day_key not in grouped_by_day:
            grouped_by_day[day_key] = OrderedDict()
            day_order.append(day_key)

        event_key = "|".join(
            [
                spot.get("token") or "sem-token",
                spot.get("start_time") or "sem-inicio",
                spot.get("event_hour") or "sem-horario",
                spot.get("event_name") or "sem-nome",
            ]
        )

        event_group = grouped_by_day[day_key].setdefault(
            event_key,
            {"spots": [], "start_dt": start_dt},
        )

        if event_group.get("start_dt") is None and start_dt is not None:
            event_group["start_dt"] = start_dt

        event_group["spots"].append(spot)

    html_lines: List[str] = [
        "<b>🏋️‍♀️ Vagas de aula liberadas!</b>",
        "",
        "Confira as oportunidades nas próximas duas semanas:",
        "",
    ]

    text_lines: List[str] = [
        "🏋️‍♀️ Vagas de aula liberadas!",
        "",
        "Confira as oportunidades nas próximas duas semanas:",
        "",
    ]

    for day_key in day_order:
        day_groups = grouped_by_day[day_key]
        representative = next(iter(day_groups.values()))
        start_dt = representative.get("start_dt")

        if start_dt is None:
            header_html = "<b>📅 Data não informada</b>"
            header_text = "📅 Data não informada"
        else:
            weekday = WEEKDAY_LABELS[start_dt.weekday()]
            date_label = start_dt.strftime("%d/%m/%Y")
            header_html = f"<b>📅 {escape(date_label)} ({escape(weekday)})</b>"
            header_text = f"📅 {date_label} ({weekday})"

        html_lines.append(header_html)
        text_lines.append(header_text)

        for event_group in day_groups.values():
            spots_for_event = event_group["spots"]
            if not spots_for_event:
                continue

            representative_spot = spots_for_event[0]
            start_dt = event_group.get("start_dt")

            event_hour = representative_spot.get("event_hour")
            if start_dt is not None and not event_hour:
                event_hour = start_dt.strftime("%H:%M")

            hour_label = event_hour or "Horário não informado"

            duration_value = representative_spot.get("duration_time")
            if isinstance(duration_value, (int, float)):
                duration = f"{duration_value:g} min"
            elif duration_value:
                duration = str(duration_value)
            else:
                duration = "Duração não informada"
            event_name = representative_spot.get("event_name") or "Aula"
            instructor = _build_instructor_label(representative_spot)
            tagline = representative_spot.get("instructor_tagline")

            bike_codes = [
                code
                for code in (
                    spot_item.get("spot_code")
                    for spot_item in spots_for_event
                )
                if code
            ]
            bikes_html, bikes_text = _format_bike_codes(bike_codes)

            html_lines.extend(
                [
                    "╭────────────────────────────╮",
                    f"│ 🕒 <b>{escape(hour_label)}</b> • {escape(duration)}",
                    f"│ 🎯 {escape(event_name)}",
                    f"│ 👤 {escape(instructor)}",
                ]
            )
            text_lines.extend(
                [
                    "╭────────────────────────────╮",
                    f"│ 🕒 {hour_label} • {duration}",
                    f"│ 🎯 {event_name}",
                    f"│ 👤 {instructor}",
                ]
            )

            if tagline:
                html_lines.append(f"│ ✨ {escape(tagline)}")
                text_lines.append(f"│ ✨ {tagline}")

            html_lines.extend(
                [
                    f"│ 🚲 {bikes_html}",
                    "╰────────────────────────────╯",
                    "",
                ]
            )
            text_lines.extend(
                [
                    f"│ 🚲 {bikes_text}",
                    "╰────────────────────────────╯",
                    "",
                ]
            )

        html_lines.append("")
        text_lines.append("")

    html_lines.append("<i>Boas pedaladas! 🚴‍♀️</i>")
    text_lines.append("Boas pedaladas! 🚴‍♀️")

    html_message = "\n".join(html_lines).strip()
    text_message = "\n".join(text_lines).strip()

    return FormattedSummary(html=html_message, plain_text=text_message)


def send_telegram_message(
    token: str,
    chat_id: str,
    message: str,
    *,
    session: Optional[requests.Session] = None,
    parse_mode: str = "HTML",
) -> Dict:
    """Envia uma mensagem de texto para o Telegram."""

    if not token:
        raise ValueError("Token do bot do Telegram não informado.")
    if not chat_id:
        raise ValueError("Chat ID do Telegram não informado.")

    payload = {
        "chat_id": chat_id,
        "disable_web_page_preview": True,
        "parse_mode": parse_mode,
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
    summary = format_spot_summary(available_spots)

    execution_report = (
        "Tempo total da automação: "
        f"{result.elapsed_seconds:.2f} segundos (início: {result.started_at.isoformat()} | "
        f"fim: {result.finished_at.isoformat()})."
    )

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary_file:
            summary_file.write(f"{execution_report}\n\n{summary.plain_text}\n")

    if args.dry_run:
        print(summary.plain_text)
        print()
        print(execution_report)
        return

    if not available_spots:
        print(summary.plain_text)
        print()
        print(execution_report)
        return

    send_telegram_message(token or "", chat_id or "", summary.html)
    print(execution_report)


if __name__ == "__main__":
    main()

