"""ADR0241 observer entry: retain slot snapshots from the same admitted metrics read."""
import observe_cce_paid_pipeline as native
import slot_failure_evidence as slots


def install(module, value, **kwargs):
    starts = native.install(module, value, **kwargs)
    slots.install(module)
    return starts


def main(argv=None):
    # Delegate the proven receipt, worker, diagnostic and cleanup argument handling.
    original = native.install

    def adapter(module, value, **kwargs):
        starts = original(module, value, **kwargs)
        slots.install(module)
        return starts

    native.install = adapter
    try:
        native.main(argv)
    finally:
        native.install = original


if __name__ == "__main__":
    main()
