"""Automação para buscar e filtrar aulas do Studio Velocity.

Este módulo implementa o fluxo descrito nos requisitos do usuário:

1. Buscar a agenda dos próximos 14 dias (duas páginas) na API pública.
2. Filtrar as aulas ministradas pelo instrutor 525 que ainda estejam abertas.
3. Classificar as aulas como dia de semana, final de semana ou feriado e aplicar
   a regra de horário para dias úteis (apenas após 19h).
4. Obter os detalhes completos dos eventos filtrados e extrair os lugares
   disponíveis com informações do instrutor.

O módulo expõe funções auxiliares para manter o comportamento testável e um
ponto de entrada ``main`` que imprime o payload final em JSON quando o script é
executado diretamente.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from time import perf_counter
from typing import Dict, Iterable, List, Optional, Sequence

import holidays
import requests


SCHEDULE_URL = "https://studiovelocity.com.br/api/v1/events/schedule/"
EVENT_URL = "https://studiovelocity.com.br/api/v1/events/events/"

# Parâmetros padrões da requisição espelhados do ``reservar_bike.txt``.
DEFAULT_SCHEDULE_PARAMS = {
    "sort": "start_time",
    "is_canceled": "false",
    "unit_list": "35",
    "activity_list": "1",
    "timezone_from_unit": "35",
}


@dataclass
class ScheduleEvent:
    """Dataclass auxiliar com o subconjunto de campos necessário."""

    token: str
    start_time: datetime


@dataclass
class AutomationResult:
    """Resultado agregado contendo as vagas e métricas de execução."""

    spots: List[Dict]
    started_at: datetime
    finished_at: datetime
    elapsed_seconds: float

    def to_dict(self) -> Dict:
        """Converte o resultado para um ``dict`` serializável em JSON."""

        return {
            "spots": self.spots,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "elapsed_seconds": self.elapsed_seconds,
        }


def _parse_start_time(raw_start: str) -> datetime:
    """Converte o valor ``start_time`` retornado pela API para ``datetime``.

    A API retorna strings no formato ISO 8601 com fuso horário (por exemplo,
    ``"2025-11-14T19:30:00-03:00"``). ``datetime.fromisoformat`` entende esse
    formato, portanto podemos fazer o parse diretamente.
    """

    try:
        return datetime.fromisoformat(raw_start)
    except ValueError as exc:  # pragma: no cover - programação defensiva
        raise ValueError(f"Valor de start_time inválido: {raw_start}") from exc


def fetch_schedule(
    session: requests.Session,
    *,
    pages: Sequence[int] = (1, 2),
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> List[Dict]:
    """Busca eventos da agenda para a janela de datas selecionada.

    Args:
        session: instância de ``requests.Session`` utilizada para chamadas HTTP.
        pages: páginas que serão baixadas do endpoint de agenda.
        start: data inicial da janela (inclusiva). Padrão: hoje.
        end: data final da janela (inclusiva). Padrão: ``start`` + 14 dias.

    Returns:
        Lista com os ``results`` combinados de todas as páginas baixadas.
    """

    if start is None:
        start = date.today()
    if end is None:
        end = start + timedelta(days=14)

    aggregated_results: List[Dict] = []
    for page in pages:
        params = {
            **DEFAULT_SCHEDULE_PARAMS,
            "page": str(page),
            "date_from": start.strftime("%Y-%m-%d"),
            "date_to": end.strftime("%Y-%m-%d"),
        }

        response = session.get(SCHEDULE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        results = payload.get("results", [])
        if not isinstance(results, list):  # pragma: no cover - programação defensiva
            raise TypeError("Payload de agenda inesperado: 'results' não é uma lista")

        aggregated_results.extend(results)

    return aggregated_results


def classify_event_day(start_dt: datetime, *, estado: str = "SP") -> str:
    """Classifica o dia do evento como ``dia_de_semana``, ``final_de_semana`` ou ``feriado``.

    Args:
        start_dt: ``datetime`` do início da aula.
        estado: sigla do estado brasileiro utilizado para o calendário de feriados.

    Returns:
        Uma das strings ``"feriado"``, ``"final_de_semana"`` ou ``"dia_de_semana"``.
    """

    br_holidays = holidays.country_holidays(
        "BR", subdiv=estado, years={start_dt.year}
    )

    if start_dt.date() in br_holidays:
        return "feriado"

    if start_dt.weekday() >= 5:
        return "final_de_semana"

    return "dia_de_semana"


def filter_events(
    raw_events: Iterable[Dict],
    *,
    instructor_id: int = 525,
) -> List[ScheduleEvent]:
    """Filtra os eventos da agenda conforme as regras de negócio."""

    filtered: List[ScheduleEvent] = []
    evening_cutoff = time(hour=19)

    for event in raw_events:
        if event.get("instructor") != instructor_id:
            continue

        if event.get("closed_at") is not None:
            continue

        start_raw = event.get("start_time")
        if not start_raw:
            continue  # Ignora entradas malformadas de forma silenciosa.

        start_dt = _parse_start_time(start_raw)

        day_classification = classify_event_day(start_dt)

        if day_classification == "dia_de_semana" and start_dt.timetz().replace(tzinfo=None) <= evening_cutoff:
            # Apenas aulas estritamente após 19h em dias de semana são válidas.
            continue

        filtered.append(ScheduleEvent(token=event["token"], start_time=start_dt))

    return filtered


def fetch_event_details(session: requests.Session, token: str) -> Dict:
    """Busca os detalhes para um token de evento específico."""

    url = f"{EVENT_URL}{token}/"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_available_spots(event_payload: Dict, start_time: datetime) -> List[Dict]:
    """Extrai os lugares disponíveis com informações do instrutor a partir do payload."""

    instructor_detail = event_payload.get("instructor_detail") or {}
    nickname = instructor_detail.get("nickname")
    first_name = instructor_detail.get("first_name", "")
    last_name = instructor_detail.get("last_name", "")
    instructor_name = " ".join(part for part in (first_name, last_name) if part).strip()
    tagline = event_payload.get("tagline", "🚲")

    duration_time = event_payload.get("duration_time")
    event_hour = event_payload.get("event_hour")
    event_name = event_payload.get("name")
    token = event_payload.get("token")

    available_spots: List[Dict] = []

    for spot in event_payload.get("map_spots", []):
        bookings = spot.get("bookings", [])
        maintenance = spot.get("maintenance", False)

        if bookings or maintenance:
            continue

        available_spots.append(
            {
                "token": token,
                "spot_code": spot.get("code"),
                "event_name": event_name,
                "event_hour": event_hour,
                "duration_time": duration_time,
                "instructor_nickname": nickname,
                "instructor_name": instructor_name,
                "instructor_tagline": tagline,
                "start_time": start_time.isoformat(),
            }
        )

    return available_spots


def collect_available_spots(
    session: requests.Session,
    schedule_events: Sequence[ScheduleEvent],
) -> List[Dict]:
    """Busca detalhes de cada evento e consolida os lugares disponíveis."""

    all_spots: List[Dict] = []
    for schedule_event in schedule_events:
        payload = fetch_event_details(session, schedule_event.token)
        all_spots.extend(
            extract_available_spots(payload, schedule_event.start_time)
        )

    return all_spots


def run_automation(session: Optional[requests.Session] = None) -> AutomationResult:
    """Executa o fluxo completo e retorna as vagas disponíveis com métricas."""

    should_close_session = False
    if session is None:
        internal_session = requests.Session()
        should_close_session = True
    else:
        internal_session = session

    started_at = datetime.now().astimezone()
    timer_start = perf_counter()

    schedule = fetch_schedule(internal_session)
    filtered_events = filter_events(schedule)
    available_spots = collect_available_spots(internal_session, filtered_events)

    finished_at = datetime.now().astimezone()
    elapsed_seconds = perf_counter() - timer_start

    if should_close_session:
        internal_session.close()

    return AutomationResult(
        spots=available_spots,
        started_at=started_at,
        finished_at=finished_at,
        elapsed_seconds=elapsed_seconds,
    )


def main() -> None:
    """Executa todo o fluxo de automação e imprime o payload JSON."""

    result = run_automation()

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
