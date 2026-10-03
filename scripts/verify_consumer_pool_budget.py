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


def api_budget_view(apis, ceiling):
    pools = [row.get("DB_POOL_MAX") for row in apis]
    return {"api_count": len(apis), "api_pool_max_per_replica": pools,
            "aggregate_api_pool_ceiling": sum(int(value) for value in pools)
            if all(value is not None and value.isdigit() for value in pools) else None,
            "pass": len(apis) == 4 and pools == [str(ceiling)] * 4}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumers", type=int, required=True, choices=(1, 2, 4, 6))
    parser.add_argument("--pool-max", type=int, required=True, choices=(8, 12))
    parser.add_argument("--api-pool-max", type=int, choices=(3, 4))
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
    if args.api_pool_max is not None:
        result["api"] = api_budget_view([environment(c) for c in containers("api")], args.api_pool_max)
        result["pass"] = result["pass"] and result["api"]["pass"]
    print(json.dumps(result))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
