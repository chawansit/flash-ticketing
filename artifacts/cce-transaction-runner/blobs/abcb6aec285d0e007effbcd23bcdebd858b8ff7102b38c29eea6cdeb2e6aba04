"""ADR0232 bounded allocation adapter over the unchanged frozen leaf generator."""

SHARD_SHOWS = 504
BLOCK_SHOWS = 42
BLOCK_JOURNEYS = 12600
SHARD_JOURNEYS = 151200


def allocation(manifest, index, views):
    if (
        type(index) is not int
        or not 0 <= index < SHARD_JOURNEYS
        or len(manifest.get("show_ids", [])) != SHARD_SHOWS
        or len(manifest.get("viewer_tokens", [])) != SHARD_JOURNEYS
        or manifest.get("seats_per_show") != 300
        or manifest.get("seat_offset") != 0
    ):
        raise ValueError("Exact hourly shard allocation required")
    block, local = divmod(index, BLOCK_JOURNEYS)
    if block not in views:
        views[block] = {
            **manifest,
            "show_ids": manifest["show_ids"][block * BLOCK_SHOWS : (block + 1) * BLOCK_SHOWS],
            "viewer_tokens": manifest["viewer_tokens"][block * BLOCK_JOURNEYS : (block + 1) * BLOCK_JOURNEYS],
        }
    return views[block], local, block


def install(module):
    original = module.scheduled_journeys
    journey = module.journey

    async def scheduled(args, manifest):
        if (
            args.rate != 42
            or args.seconds != 3600
            or args.concurrency != 250
            or args.completion_deadline_seconds != 3720
            or args.http_max_connections != 250
            or args.http_client_count != 8
            or args.poll_seconds != 1
            or args.duplicates != 1
        ):
            raise ValueError("Exact bounded hourly leaf settings required")
        views = {}

        async def allocated(client, data, index, run_id, timeout, poll, duplicates):
            view, local, block = allocation(data, index, views)
            return await journey(
                client, view, local, run_id + "-block-" + str(block), timeout, poll, duplicates
            )

        return await original(args, manifest, journey_fn=allocated)

    module.scheduled_journeys = scheduled
    return original


def main():
    import paid_ticket_load_generator as frozen

    original = install(frozen)
    try:
        frozen.main()
    finally:
        frozen.scheduled_journeys = original


if __name__ == "__main__":
    main()
