from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass(slots=True)
class MarketCandidate:
    market_id: str
    question: str
    slug: str
    token_id: str
    midpoint: float
    best_bid: float
    best_ask: float
    bids_depth: float
    asks_depth: float
    spread: float
    hours_to_resolution: float
    total_volume: float
    book_imbalance: float
    category: str = ""
    priority_score: float = 0.0
    raw_market: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class Thesis:
    market_id: str
    token_id: str
    estimated_probability: float
    confidence: float
    thesis: str
    catalysts: list[str]
    crowd_error: str
    source: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class Vote:
    agent: str
    action: str
    confidence: float
    estimated_probability: float
    rationale: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class Position:
    position_id: str
    market_id: str
    token_id: str
    question: str
    side: str
    shares: float
    notional_usdc: float
    entry_price: float
    expected_gap: float
    opened_at: str
    thesis_confidence: float
    mode: str
    category: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
