from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class LatencyBotStatus:
    runner_status: str
    phase: str
    last_cycle_started_at: str | None
    last_cycle_completed_at: str | None
    last_error: str | None
    tracked_markets_count: int
    open_orders_count: int
    open_positions_count: int
    realized_pnl_usdc: float
    unrealized_pnl_usdc: float
    risk_state: str
    notes: list[str] = field(default_factory=list)
    last_cycle_result: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "runner_status": self.runner_status,
            "phase": self.phase,
            "last_cycle_started_at": self.last_cycle_started_at,
            "last_cycle_completed_at": self.last_cycle_completed_at,
            "last_error": self.last_error,
            "tracked_markets_count": self.tracked_markets_count,
            "open_orders_count": self.open_orders_count,
            "open_positions_count": self.open_positions_count,
            "realized_pnl_usdc": self.realized_pnl_usdc,
            "unrealized_pnl_usdc": self.unrealized_pnl_usdc,
            "risk_state": self.risk_state,
            "notes": list(self.notes),
            "last_cycle_result": dict(self.last_cycle_result),
        }
