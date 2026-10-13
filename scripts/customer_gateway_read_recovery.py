"""ADR0310 optional gateway read recovery; existing sealed bundles stay unchanged."""
from customer_recovery_client import CustomerRecoveryClient


class GatewayReadRecoveryClient(CustomerRecoveryClient):
    """Retry existing order-status reads on502/504 without changing writes."""

    async def read(self, url, kwargs, role):
        for attempt in range(self.policy.max_attempts):
            if attempt:
                self.retry_attempts[role] += 1
            response, error = await self.send("get", url, kwargs)
            reason = self.reason(response, error)
            if error is None and response.status_code in {502, 504}:
                reason = "http_" + str(response.status_code)
            if attempt == 0 and (reason is not None or response.status_code != 200):
                self.first_attempt_errors[role + ":" + (reason or "http_" + str(response.status_code))] += 1
            if reason is None:
                return response
            self.processing = True
            if attempt + 1 == self.policy.max_attempts or not await self.pause(attempt, response):
                return self.finish(response, error)
        raise AssertionError("Unreachable recovery loop")
