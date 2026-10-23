"""Shared validated lease/retry policy; no third-party settings dependency."""

from dataclasses import dataclass, fields
import math
import os


@dataclass(frozen=True)
class JobPolicy:
    assignment_seconds: float = 15
    lease_seconds: float = 30
    renewal_seconds: float = 10
    max_attempts: int = 3
    retry_seconds: float = 5
    retry_cap_seconds: float = 60

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{field.name} must be finite and positive")
        if type(self.max_attempts) is not int:
            raise ValueError("max_attempts must be an integer")
        if self.renewal_seconds >= self.lease_seconds:
            raise ValueError("Renewal interval must be shorter than lease")
        if self.retry_cap_seconds < self.retry_seconds:
            raise ValueError("Retry cap must be at least the initial delay")

    @classmethod
    def from_env(cls):
        return cls(**{
            field.name: (int if field.name == "max_attempts" else float)(os.environ[
                "MINI_CLOUD_JOB_" + field.name.upper()
            ]) for field in fields(cls) if "MINI_CLOUD_JOB_" + field.name.upper() in os.environ
        })

    def retry_delay(self, attempt: int) -> float:
        return min(self.retry_seconds * 2 ** min(max(attempt - 1, 0), 30), self.retry_cap_seconds)
