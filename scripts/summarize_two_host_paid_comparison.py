"""Secret-free comparison of executed ADR0147 stages; never infer maximum capacity."""
import argparse
import json
from pathlib import Path

LATENCIES = ('hold_http_p95_ms', 'durable_p95_ms', 'payment_http_p95_ms', 'payment_to_ticket_p95_ms', 'hold_to_ticket_p95_ms')


def compact(stage):
    customer = stage.get('customer', {})
    dispatched = customer.get('dispatched', 0)
    errors = sum(v for k, v in customer.get('outcomes', {}).items() if k != 'fulfilled')
    return {
        'stage_pass': stage.get('pass', False),
        'customer_pass': customer.get('pass', False),
        'offered_buyers_per_second': customer.get('rate_target_per_second'),
        'dispatch_seconds': customer.get('dispatch_seconds'),
        'scheduled': customer.get('scheduled'),
        'dispatched': dispatched,
        'customer_confirmations_by_deadline': customer.get('fulfilled_by_deadline'),
        'generator_drops': customer.get('generator_drops'),
        'failed_dispatched_journeys': errors,
        'errors_per_dispatched_journey_percent': 100 * errors / dispatched if dispatched else None,
        'customer_outcomes': customer.get('outcomes'),
        'customer_http_attempts_total': sum(customer.get('physical_http_attempts', {}).values()),
        'latency_summary_mode': customer.get('latency_summary_mode'),
        'p95_ms': {k: customer.get(k) for k in LATENCIES},
        'host_cpu_percent': stage.get('cpu_window', {}).get('host_cpu_percent'),
        'aggregate_api_cpu_cores': stage.get('cpu_window', {}).get('aggregate_api_cpu_cores'),
        'financial': stage.get('financial'),
        'global_queues': stage.get('global_queues'),
        'all_four_replicas_observed': stage.get('distribution', {}).get('all_four_replicas_observed'),
        'request_shares': sorted(stage.get('distribution', {}).get('business_request_shares', {}).values()),
        'private_cleanup_pass': stage.get('private_cleanup_pass'),
        'failure_type': stage.get('failure_type'),
    }


def report(directory):
    integration = json.loads((directory / 'integration-summary.json').read_text())
    stages = {arm: compact(json.loads((directory / arm / 'stage.private.json').read_text()))
              for arm in integration['stage_names']}
    differences = {}
    if set(stages) == {'control', 'candidate'}:
        for key in LATENCIES:
            baseline, candidate = (stages[arm]['p95_ms'][key] for arm in ('control', 'candidate'))
            if baseline is not None and candidate is not None:
                differences[key] = {'control': baseline, 'candidate': candidate, 'change_percent': 100 * (candidate - baseline) / baseline if baseline else None}
    return {
        'kind': 'fixed_budget_two_host_paid_comparison',
        'run': directory.name,
        'pass': integration['pass'],
        'restore_pass': integration['restore_pass'],
        'capacity_stages_started': integration['capacity_stages_started'],
        'application_revision': 'deb330ec91e553640d1d0ba10e92aa8f29cd86dc',
        'scope': 'Four APIs on one host versus two on each host; same offered workload and database budgets. This does not measure maximum capacity or sustained hourly throughput.',
        'stages': stages,
        'latency_comparison': differences,
        'gates': integration.get('gates', {}),
        'adapter_identity': integration.get('adapter_identity', {}),
        'http_rps_note': 'Customer HTTP attempts include completion-tail requests; do not divide by dispatch duration and call that offered-window HTTP RPS. Journey errors are not HTTP error percentage.',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(report(args.directory), indent=2) + '\n')


if __name__ == '__main__':
    main()
