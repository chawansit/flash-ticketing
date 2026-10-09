"""Bounded tooling-only fixture layouts and shared-show shard seat ranges."""
LAYOUTS = ("distributed", "single-concert")
SINGLE_CONCERT_MAX_SEATS = 18000


def fixture_layout(layout, shows, seats):
    if layout not in LAYOUTS:
        raise ValueError("Unsupported fixture layout")
    if type(shows) is not int or type(seats) is not int:
        raise ValueError("Fixture sizes must be integers")
    if layout == "single-concert":
        if shows != 1 or not 1 <= seats <= SINGLE_CONCERT_MAX_SEATS:
            raise ValueError("Single-concert requires one show and at most18000 seats")
    elif not 1 <= shows <= 2000 or not 1 <= seats <= 1000:
        raise ValueError("Distributed fixtures require1..2000 shows and1..1000 seats/show")
    return layout


def single_concert_parts(manifest, shards, scheduled_per_shard):
    shows, tokens = manifest.get("show_ids", []), manifest.get("viewer_tokens", [])
    fixture_layout(manifest.get("fixture_layout"), len(shows), manifest.get("seats_per_show"))
    if manifest.get("fixture_layout") != "single-concert":
        raise ValueError("Shared-show allocation requires explicit single-concert layout")
    if (shards != 2 or type(scheduled_per_shard) is not int or scheduled_per_shard < 1
            or type(manifest.get("seat_offset")) is not int or manifest["seat_offset"] != 0
            or len(tokens) < shards or len(set(tokens)) != len(tokens)
            or manifest.get("seat_allocation") is not None):
        raise ValueError("Invalid single-concert parent allocation or viewer partition")
    if shards * scheduled_per_shard > manifest["seats_per_show"]:
        raise ValueError("A shard has insufficient distinct seats")
    return [
        {**manifest, "viewer_tokens": tokens[index::shards],
         "seat_offset": index * scheduled_per_shard,
         "seat_allocation": {"shard": index, "shards": shards,
                             "first": index * scheduled_per_shard,
                             "stop": (index + 1) * scheduled_per_shard}}
        for index in range(shards)
    ]


def validate_child_allocation(manifest, scheduled):
    layout = manifest.get("fixture_layout", "distributed")
    if layout not in LAYOUTS:
        raise ValueError("Unsupported fixture layout")
    allocation = manifest.get("seat_allocation")
    if layout == "distributed":
        if allocation is not None:
            raise ValueError("Distributed layout cannot contain shared-show allocation")
        return
    fixture_layout(layout, len(manifest.get("show_ids", [])), manifest.get("seats_per_show"))
    if (not isinstance(allocation, dict) or set(allocation) != {"shard", "shards", "first", "stop"}
            or any(type(value) is not int for value in allocation.values())):
        raise ValueError("Single-concert child requires explicit integer seat allocation")
    shard, count, first, stop = (allocation[k] for k in ("shard", "shards", "first", "stop"))
    width = stop - first
    if (count != 2 or shard not in (0, 1) or width < 1
            or first != shard * width or first != manifest.get("seat_offset")
            or stop > manifest["seats_per_show"] or count * width > manifest["seats_per_show"]
            or type(scheduled) is not int or not 1 <= scheduled <= width):
        raise ValueError("Single-concert child exceeds or mismatches its disjoint seat range")
