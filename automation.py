"""Automation script for fetching and filtering Studio Velocity classes.

This module encapsulates the workflow described in the user requirements:

1. Fetch the schedule for the next 14 days (two pages) from the public API.
2. Filter the classes taught by instructor 525 that are still open.
3. Classify the classes as weekday, weekend or holiday and apply the
   time-of-day rule for weekdays (after 19:00 only).
4. Retrieve the full event details for the filtered classes and extract the
   available map spots together with instructor details.

The module exposes helper functions to keep the behaviour testable and a
``main`` entry point that prints the final payload as JSON when the script is
executed directly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

import holidays
import requests


SCHEDULE_URL = "https://studiovelocity.com.br/api/v1/events/schedule/"
EVENT_URL = "https://studiovelocity.com.br/api/v1/events/events/"

# Default request parameters mirrored from ``reservar_bike.txt``.
DEFAULT_SCHEDULE_PARAMS = {
    "sort": "start_time",
    "is_canceled": "false",
    "unit_list": "35",
    "activity_list": "1",
    "timezone_from_unit": "35",
}


@dataclass
class ScheduleEvent:
    """Small helper dataclass with the subset of fields we require."""

    token: str
    start_time: datetime


def _parse_start_time(raw_start: str) -> datetime:
    """Parse the ``start_time`` value returned by the API.

    The API returns ISO 8601 strings with the timezone offset (e.g.
    ``"2025-11-14T19:30:00-03:00"``). ``datetime.fromisoformat`` understands
    this format, so we can parse it directly.
    """

    try:
        return datetime.fromisoformat(raw_start)
    except ValueError as exc:  # pragma: no cover - defensive programming
        raise ValueError(f"Invalid start_time value: {raw_start}") from exc


def fetch_schedule(
    session: requests.Session,
    *,
    pages: Sequence[int] = (1, 2),
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> List[Dict]:
    """Fetch schedule events for the selected date window.

    Args:
        session: a ``requests.Session`` instance used for HTTP calls.
        pages: which pages to download from the schedule endpoint.
        start: starting date for the window (inclusive). Defaults to today.
        end: ending date for the window (inclusive). Defaults to ``start`` + 14
            days.

    Returns:
        A list with the combined ``results`` from all downloaded pages.
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
        if not isinstance(results, list):  # pragma: no cover - defensive
            raise TypeError("Unexpected schedule payload: 'results' is not a list")

        aggregated_results.extend(results)

    return aggregated_results


def classify_event_day(start_dt: datetime, *, country_code: str = "BR") -> str:
    """Classify the event day as ``weekday``, ``weekend`` or ``holiday``.

    Args:
        start_dt: datetime of the class start.
        country_code: country code used for the holidays calendar.

    Returns:
        One of ``"feriado"``, ``"final_de_semana"`` or ``"dia_de_semana"``.
    """

    br_holidays = holidays.country_holidays(country_code, years={start_dt.year})

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
    """Filter schedule events according to the business rules."""

    filtered: List[ScheduleEvent] = []
    evening_cutoff = time(hour=19)

    for event in raw_events:
        if event.get("instructor") != instructor_id:
            continue

        if event.get("closed_at") is not None:
            continue

        start_raw = event.get("start_time")
        if not start_raw:
            continue  # Skip malformed entries silently.

        start_dt = _parse_start_time(start_raw)

        day_classification = classify_event_day(start_dt)

        if day_classification == "dia_de_semana" and start_dt.timetz().replace(tzinfo=None) <= evening_cutoff:
            # Only classes strictly after 19:00 on weekdays are allowed.
            continue

        filtered.append(ScheduleEvent(token=event["token"], start_time=start_dt))

    return filtered


def fetch_event_details(session: requests.Session, token: str) -> Dict:
    """Fetch the details for a specific event token."""

    url = f"{EVENT_URL}{token}/"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_available_spots(event_payload: Dict) -> List[Dict]:
    """Extract available spots with instructor information from an event payload."""

    instructor_detail = event_payload.get("instructor_detail") or {}
    nickname = instructor_detail.get("nickname")
    first_name = instructor_detail.get("first_name", "")
    last_name = instructor_detail.get("last_name", "")
    instructor_name = " ".join(part for part in (first_name, last_name) if part).strip()
    tagline = instructor_detail.get("tagline")

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
            }
        )

    return available_spots


def collect_available_spots(
    session: requests.Session,
    schedule_events: Sequence[ScheduleEvent],
) -> List[Dict]:
    """Fetch details for each schedule event and collect available spots."""

    all_spots: List[Dict] = []
    for schedule_event in schedule_events:
        payload = fetch_event_details(session, schedule_event.token)
        all_spots.extend(extract_available_spots(payload))

    return all_spots


def main() -> None:
    """Execute the full automation pipeline and print the JSON payload."""

    session = requests.Session()

    schedule = fetch_schedule(session)
    filtered_events = filter_events(schedule)
    available_spots = collect_available_spots(session, filtered_events)

    print(json.dumps(available_spots, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
