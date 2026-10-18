"""ADR0315 exact native writer-count gates; historical three-writer checks stay intact."""
from cce_worker_identity import validate_bundle


def selected(bundle):
    if bundle is None or bundle.get("decision") not in {"ADR0315", "ADR0319", "ADR0321", "ADR0322", "ADR0323", "ADR0324", "ADR0345", "ADR0357"}:
        return False
    validate_bundle(bundle)
    return True


def role_counts(bundle):
    from cce_worker_identity import validate_transferred_bundle
    validate_transferred_bundle(bundle)
    from collections import Counter
    counts = Counter(row['role'] for row in bundle['receipts'])
    return {'writer': counts['reservation-writer'], 'consumer': counts['consumer'],
            'maintenance': counts['maintenance'], 'reconciler': counts['reconciler'],
            'simulator': counts['simulator'], 'publisher': counts['publisher']}


def startup(row, bundle, legacy):
    if not selected(bundle):
        return legacy(row, 6)
    counts = role_counts(bundle)
    errors = sorted(k for k in row if k.endswith("_error"))
    apis = row.get("api_replicas", {})
    common = (isinstance(row.get("utc"), str) and "issued_tickets" in row and not errors
              and isinstance(apis, dict) and len(apis) == 4 and row.get("consumer_replicas") == counts["consumer"]
              and row.get("simulator_replicas") == 1 and row.get("publisher_replicas") == 1)
    ready = (row.get("extended_diagnostics") is True
             and row.get("writer_replicas") == counts["writer"] and row.get("maintenance_replicas") == 1
             and row.get("reconciler_replicas") == 1
             and len(row.get("host_cpu_ticks", [])) == 8 and bool(row.get("pgbouncer"))
             and all(len(row.get(role + "_db_replicas", {})) == count
                     and all("process_cpu_seconds_total" in item for item in row[role + "_db_replicas"].values())
                     for role, count in counts.items()))
    return {"utc": row.get("utc"), "api_count": len(apis), "consumer_count": row.get("consumer_replicas"),
            "writer_count": row.get("writer_replicas"), "sample_errors": errors,
            "diagnostics_ready": ready, "writer_scale_decision": "ADR0357" if bundle["decision"] == "ADR0357" else "ADR0315",
            "pass": bool(common and ready)}


def pipeline(summary, bundle, legacy):
    if not selected(bundle):
        return legacy(summary)
    counts = role_counts(bundle)
    return (summary["api"]["observed_replicas"] == 4
            and summary["consumer"]["max_replicas"] == counts["consumer"]
            and summary["api_metrics_errors"] == summary["database_errors"] == 0
            and summary["resources"]["pass"] is True
            and summary["role_database"]["writer"]["observed_replicas"] == counts["writer"]
            and all(not value["counter_reset_detected"] for value in summary["role_database"].values())
            and not summary["writer_phases"]["counter_reset_detected"])


def summary(rows, bundle, legacy):
    result = legacy(rows)
    if not selected(bundle):
        return result
    extended = [row for row in rows if row.get("extended_diagnostics")]
    views = [row for row in extended if "pgbouncer" in row]
    ticks = [row for row in extended if len(row.get("host_cpu_ticks", [])) == 8]
    counts = role_counts(bundle)
    coverage = bool(extended) and all(
        row.get(role + "_replicas") == count
        and len(row.get(role + "_db_replicas", {})) == count
        and all("process_cpu_seconds_total" in item
                for item in row[role + "_db_replicas"].values())
        for row in extended for role, count in counts.items())
    resource = result["resources"]
    resource["legacy_resource_coverage_pass"] = resource["role_coverage_pass"]
    resource["role_coverage_pass"] = coverage
    resource["writer_scale_decision"] = "ADR0357" if bundle["decision"] == "ADR0357" else "ADR0315"
    resource["pass"] = (resource["host_cpu_p95_percent"] is not None
        and len(views) == len(ticks) == len(extended)
        and coverage and not resource["errors"] and not resource["counter_reset_detected"])
    return result
