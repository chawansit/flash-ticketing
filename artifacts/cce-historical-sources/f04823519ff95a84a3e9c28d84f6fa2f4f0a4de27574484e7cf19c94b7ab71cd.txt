from dataclasses import dataclass


@dataclass
class Failure(Exception):
    code: str
    status: int = 409


def payment_decision(order_status: str, valid_hold: bool, outcome: str) -> str:
    """A financial success is never discarded, even after a failed/expired checkout."""
    if order_status in {"PAID", "FULFILLED"}:
        return "UNCHANGED"
    if outcome == "FAILED":
        return "FAILED" if order_status == "PENDING" else "UNCHANGED"
    return "BOOK" if order_status == "PENDING" and valid_hold else "REFUND"
