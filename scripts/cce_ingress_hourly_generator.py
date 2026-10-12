"""ADR0295 ingress-only adapter; sealed hourly scheduling and aggregation unchanged."""
import copy
from types import SimpleNamespace

CONTROL_ORIGIN = "http://10.1.137.69:8000"
INGRESS_ORIGIN = "http://10.2.39.55"


def validate(original, args, manifest):
    if args.origin != INGRESS_ORIGIN or manifest.get("origin") != args.origin:
        raise ValueError("Exact existing CCE hourly ingress and matching manifest required")
    structural = copy.copy(args)
    structural.origin = CONTROL_ORIGIN
    return original(structural, {**manifest, "origin": CONTROL_ORIGIN})


def install(module):
    original = module.validate
    module.validate = lambda args, manifest: validate(original, args, manifest)
    return original


def preflight(arguments, manifest):
    """Validate exact launch settings before reserving remote paid dispatch."""
    import cce_hourly_paid_generator as parent
    kinds = {"rate": int, "seconds": int, "completion_deadline_seconds": int,
             "concurrency": int, "http_client_count": int, "poll_seconds": float,
             "duplicates": int, "start_at_epoch": float, "recovery_max_attempts": int,
             "manifest": str, "origin": str, "output": str}
    values, tokens = {}, iter(arguments[2:])
    for flag in tokens:
        key = flag.removeprefix("--").replace("-", "_")
        if flag == "--lifecycle-diagnostics":
            if key in values:
                raise ValueError("Duplicate hourly launch flag")
            values[key] = True
        elif flag.startswith("--") and key in kinds and key not in values:
            try:
                values[key] = kinds[key](next(tokens))
            except (StopIteration, ValueError) as error:
                raise ValueError("Malformed hourly launch arguments") from error
        else:
            raise ValueError("Unknown or duplicate hourly launch argument")
    args = SimpleNamespace(**values)
    if arguments[1].endswith("/cce_ingress_hourly_generator.py"):
        validate(parent.validate, args, manifest)
    elif arguments[1].endswith("/cce_hourly_paid_generator.py"):
        parent.validate(args, manifest)
    else:
        raise ValueError("Exact hourly coordinator path required")
    return True


def main():
    import cce_hourly_paid_generator as parent
    original = install(parent)
    try:
        parent.main()
    finally:
        parent.validate = original


if __name__ == "__main__":
    main()
