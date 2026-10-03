"""Verify a bounded consumer layout without emitting connection credentials."""
import argparse
import json
import subprocess


def budget_view(pgbouncer, consumers, count, ceiling):
    expected = {"DEFAULT_POOL_SIZE": "24", "RESERVE_POOL_SIZE": "0",
                "MAX_CLIENT_CONN": "160", "POOL_MODE": "transaction"}
    actual = {key: pgbouncer.get(key) for key in expected}
    pools = [row.get("DB_POOL_MAX") for row in consumers]
    return {"pgbouncer": actual, "consumer_count": len(consumers),
            "consumer_pool_max_per_replica": pools,
            "aggregate_consumer_pool_ceiling": len(consumers) * ceiling,
            "pass": actual == expected and len(consumers) == count and pools == [str(ceiling)] * count}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumers", type=int, required=True, choices=(1, 2, 4, 6))
    parser.add_argument("--pool-max", type=int, required=True, choices=(8, 12))
    args = parser.parse_args()
    compose = ["docker", "compose", "--env-file", ".env.rds", "-f", "compose.yaml",
               "-f", "compose.rds.yaml", "-f", "compose.horizontal.yaml", "-f", "compose.keepalive10.yaml"]

    def containers(service):
        return subprocess.check_output(compose + ["ps", "-q", service], text=True).splitlines()

    def environment(container):
        record = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[0]
        return dict(value.split("=", 1) for value in record["Config"]["Env"])

    pgb = containers("pgbouncer")
    if len(pgb) != 1:
        raise RuntimeError("Expected one running application pooler")
    result = budget_view(environment(pgb[0]), [environment(c) for c in containers("consumer")],
                         args.consumers, args.pool_max)
    print(json.dumps(result))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
