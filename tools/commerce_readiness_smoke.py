"""Redacted readiness smoke for the Commerce public API."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import sys
import time
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


_DOMAIN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+",
    re.ASCII,
)
_REGION = re.compile(r"[a-z]{2}(?:-gov)?-[a-z]+-[0-9]", re.ASCII)
_API_HOST = re.compile(
    r"[a-z0-9]{10}\.execute-api\.(?P<region>[a-z0-9-]+)\.amazonaws\.com(?:\.cn)?",
    re.ASCII,
)
_REQUIRED = (
    "ZLP_COMMERCE_SMOKE_API_URL",
    "ZLP_COMMERCE_SMOKE_DOMAIN",
    "AWS_REGION",
)
_CLASSIFICATIONS = frozenset(
    {
        "ready",
        "missing_input",
        "auth_failure",
        "configuration_failure",
        "provider_failure",
        "propagation_delay",
    }
)


@dataclass(frozen=True, slots=True)
class SmokeRequest:
    url: str
    environment: str
    region: str
    headers: dict[str, str]
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SmokeResponse:
    status: int


def run(
    environment: Mapping[str, str],
    *,
    sender: Callable[[SmokeRequest], SmokeResponse] | None = None,
    clock: Callable[[], int] | None = None,
) -> dict[str, Any]:
    observed_at_epoch = _observed_epoch(clock)
    values = {name: environment.get(name, "").strip() for name in _REQUIRED}
    if any(not values[name] for name in _REQUIRED):
        return _result(
            False,
            "missing_input",
            attempts=0,
            environment=None,
            observed_at_epoch=observed_at_epoch,
        )
    try:
        smoke_request = _request(values)
    except (UnicodeError, ValueError):
        smoke_request = None
    if smoke_request is None:
        return _result(
            False,
            "missing_input",
            attempts=0,
            environment=None,
            observed_at_epoch=observed_at_epoch,
        )
    try:
        response = (sender or _send)(smoke_request)
        status = response.status
    except Exception:
        return _result(
            False,
            "provider_failure",
            attempts=1,
            environment=smoke_request.environment,
            observed_at_epoch=observed_at_epoch,
        )
    if type(status) is not int or not 100 <= status <= 599:
        return _result(
            False,
            "provider_failure",
            attempts=1,
            environment=smoke_request.environment,
            observed_at_epoch=observed_at_epoch,
        )
    if 200 <= status <= 299:
        return _result(
            True,
            "ready",
            status=status,
            attempts=1,
            environment=smoke_request.environment,
            observed_at_epoch=observed_at_epoch,
        )
    if status in {401, 403}:
        return _result(
            False,
            "auth_failure",
            status=status,
            attempts=1,
            environment=smoke_request.environment,
            observed_at_epoch=observed_at_epoch,
        )
    if status == 404 and _before_propagation_deadline(
        environment, observed_at_epoch
    ):
        return _result(
            False,
            "propagation_delay",
            status=status,
            attempts=1,
            environment=smoke_request.environment,
            observed_at_epoch=observed_at_epoch,
        )
    if 400 <= status <= 499:
        return _result(
            False,
            "configuration_failure",
            status=status,
            attempts=1,
            environment=smoke_request.environment,
            observed_at_epoch=observed_at_epoch,
        )
    return _result(
        False,
        "provider_failure",
        status=status,
        attempts=1,
        environment=smoke_request.environment,
        observed_at_epoch=observed_at_epoch,
    )


def _request(values: Mapping[str, str]) -> SmokeRequest | None:
    parsed = urlsplit(values["ZLP_COMMERCE_SMOKE_API_URL"])
    host = parsed.hostname or ""
    host_match = _API_HOST.fullmatch(host)
    region = values["AWS_REGION"]
    stage = parsed.path.strip("/")
    domain = values["ZLP_COMMERCE_SMOKE_DOMAIN"].lower()
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.query
        or parsed.fragment
        or host_match is None
        or host_match["region"] != region
        or _REGION.fullmatch(region) is None
        or stage not in {"test", "production"}
        or _DOMAIN.fullmatch(domain) is None
    ):
        return None
    return SmokeRequest(
        url=(
            f"{values['ZLP_COMMERCE_SMOKE_API_URL'].rstrip('/')}"
            "/features/commerce/public-read"
        ),
        environment=stage,
        region=region,
        headers={"x-zlp-domain": domain},
        payload={"operation": "offerList", "input": {"limit": 1}},
    )


def _send(smoke_request: SmokeRequest) -> SmokeResponse:
    body = json.dumps(
        smoke_request.payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    request = Request(
        smoke_request.url,
        data=body,
        headers={
            **smoke_request.headers,
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            return SmokeResponse(response.status)
    except HTTPError as error:
        return SmokeResponse(error.code)
    except URLError as error:
        raise RuntimeError("Readiness transport unavailable") from error


def _before_propagation_deadline(
    environment: Mapping[str, str], now_epoch: int
) -> bool:
    raw = environment.get(
        "ZLP_COMMERCE_SMOKE_PROPAGATION_UNTIL_EPOCH", ""
    ).strip()
    if type(now_epoch) is not int or re.fullmatch(r"[0-9]{1,10}", raw, re.ASCII) is None:
        return False
    deadline = int(raw)
    return 0 <= now_epoch < deadline <= now_epoch + 900


def _result(
    ok: bool,
    classification: str,
    *,
    status: int | None = None,
    attempts: int,
    environment: str | None,
    observed_at_epoch: int,
) -> dict[str, Any]:
    if classification not in _CLASSIFICATIONS:
        raise ValueError("Smoke classification is invalid")
    if environment not in {None, "test", "production"}:
        raise ValueError("Smoke environment is invalid")
    result: dict[str, Any] = {
        "ok": ok,
        "classification": classification,
        "attempts": attempts,
        "environment": environment,
        "observedAtEpoch": observed_at_epoch,
    }
    if status is not None:
        result["httpStatus"] = status
    return result


def _observed_epoch(clock: Callable[[], int] | None) -> int:
    value = (clock or (lambda: int(time.time())))()
    if type(value) is not int or not 0 <= value <= 9_999_999_999:
        raise ValueError("Smoke clock is invalid")
    return value


def main() -> int:
    import os

    result = run(os.environ)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    sys.exit(main())
