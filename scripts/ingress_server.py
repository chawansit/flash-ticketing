"""Opt-in protocol timing adapter for the pinned Uvicorn/httptools benchmark."""
import logging
from time import perf_counter

import uvicorn
from prometheus_client import Histogram
from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

INGRESS = Histogram("ticketing_ingress_seconds", "Protocol/application boundary duration", ["phase"])
log = logging.getLogger("ticketing.ingress")
STAMP = "ticketing_headers_complete_at"


class TimedHttpToolsProtocol(HttpToolsProtocol):
    def on_headers_complete(self):
        # Each on_message_begin creates a fresh scope. Stamp before scheduling the ASGI task.
        self.scope["state"][STAMP] = perf_counter()
        return super().on_headers_complete()


class IngressTiming:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        entered = perf_counter()
        parsed = scope.get("state", {}).get(STAMP)
        queue = entered-parsed if parsed is not None else None
        async def timed_send(message):
            if message["type"] == "http.response.start":
                elapsed = perf_counter()-entered
                headers = list(message.get("headers", []))
                timings = [f"asgi_headers;dur={elapsed*1000:.3f}"]
                if queue is not None:
                    timings.append(f"protocol_queue;dur={queue*1000:.3f}")
                    INGRESS.labels("headers_to_asgi").observe(queue)
                INGRESS.labels("asgi_to_headers").observe(elapsed)
                headers.append((b"server-timing", ", ".join(timings).encode("ascii")))
                request_id = next((v.decode("ascii") for k,v in headers if k.lower()==b"x-request-id"), None)
                log.info("ingress", extra={"fields": {
                    "request_id": request_id, "status": message["status"],
                    "protocol_queue_ms": round(queue*1000,3) if queue is not None else None,
                    "asgi_headers_ms": round(elapsed*1000,3),
                }})
                message = {**message, "headers": headers}
            await send(message)
        await self.app(scope, receive, timed_send)


if __name__ == "__main__":
    import os

    from ticketing.api import app

    if os.getenv("DISPATCH_PROBE", "0") == "1":
        from thread_dispatch_probe import install
        install()
    uvicorn.run(IngressTiming(app), host="0.0.0.0", port=8000,
                http=TimedHttpToolsProtocol, limit_concurrency=256, timeout_keep_alive=5)
