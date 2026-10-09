"""Prepare expiring development credentials locally; never copy signing secrets to the generator."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import jwt

from ticketing.config import Settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", type=Path, default=Path("tmp/load-manifest.json"))
    parser.add_argument("--viewers", type=int, default=8000)
    parser.add_argument("--seat-offset", type=int, default=100)
    args = parser.parse_args()
    settings = Settings()
    target = urlsplit(args.origin)
    if settings.environment != "development":
        parser.error("Development token issuer only")
    if (
        target.scheme not in {"http", "https"}
        or not target.netloc
        or target.username
        or target.query
        or target.fragment
        or target.path not in {"", "/"}
    ):
        parser.error("Provide a test API origin without credentials or a path")
    if not args.output.resolve().is_relative_to((Path.cwd() / "tmp").resolve()):
        parser.error("Credential manifests must be written under ignored tmp/")
    if not 1 <= args.viewers <= 50000 or not 0 <= args.seat_offset < 300:
        parser.error("Invalid viewer count or seat offset")
    source = json.loads(args.results.read_text())
    shows = source["show_ids"]
    now, identifier = datetime.now(UTC), str(uuid4())
    expiry = now + timedelta(hours=1)
    tokens = [
        jwt.encode(
            {"sub": f"load-{identifier}-{i}", "aud": "ticketing", "iss": "ticketing", "exp": expiry},
            settings.jwt_secret,
            algorithm="HS256",
        )
        for i in range(args.viewers)
    ]
    payload = {
        "schema_version": 1,
        "environment": "development",
        "id": identifier,
        "origin": args.origin.rstrip("/"),
        "expires_at": expiry.isoformat(),
        "show_ids": shows,
        "viewer_tokens": tokens,
        "seat_offset": args.seat_offset,
        "seats_per_show": 300,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload))
    print(f"Manifest written to {args.output}; expires {expiry.isoformat()}. Keep it private.")


if __name__ == "__main__":
    main()
