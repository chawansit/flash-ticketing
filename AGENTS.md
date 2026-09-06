# Repository engineering rules

Whenever selecting or changing an architectural pattern, create an ADR under docs/adr before implementing the choice. Include status, context, decision, alternatives, consequences, failure/recovery behavior and validation evidence. Link it from docs/adr/README.md. Cover persistence/locking, messaging, idempotency, TTL and scaling decisions. Supersede accepted decisions explicitly. Do not claim tests passed unless they were executed; distinguish implemented behavior from future scope.

