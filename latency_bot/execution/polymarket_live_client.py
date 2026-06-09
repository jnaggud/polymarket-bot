from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

from ..config import LatencyBotSettings


@dataclass(frozen=True)
class PolymarketLivePreflight:
    ok: bool
    reason: str


class PolymarketLiveCompleteSetClient:
    """Thin optional adapter around the official Polymarket CLOB V2 SDK.

    The adapter is fail-closed: missing SDK, missing credentials, or unsupported
    paired-FOK methods return preflight failures instead of falling back to
    sequential live orders.
    """

    def __init__(self, settings: LatencyBotSettings) -> None:
        self.settings = settings
        self._sdk: Any | None = None
        self._client: Any | None = None

    def preflight(self) -> PolymarketLivePreflight:
        missing = []
        if not self.settings.live_complete_set_arb_pilot_private_key:
            missing.append("private key")
        if not self.settings.live_complete_set_arb_pilot_api_key:
            missing.append("api key")
        if not self.settings.live_complete_set_arb_pilot_api_secret:
            missing.append("api secret")
        if not self.settings.live_complete_set_arb_pilot_api_passphrase:
            missing.append("api passphrase")
        if not self.settings.live_complete_set_arb_pilot_funder_address:
            missing.append("funder address")
        if missing:
            return PolymarketLivePreflight(False, f"missing live CLOB credentials: {', '.join(missing)}")
        try:
            self._load_sdk()
        except Exception as exc:
            return PolymarketLivePreflight(False, f"official CLOB V2 SDK unavailable: {exc}")
        try:
            client = self._build_client()
        except Exception as exc:
            return PolymarketLivePreflight(False, f"official CLOB V2 client init failed: {exc}")
        if not self._method(client, "create_market_order", "createMarketOrder"):
            return PolymarketLivePreflight(False, "official CLOB client missing create_market_order")
        if not self._method(client, "post_orders", "postOrders"):
            return PolymarketLivePreflight(False, "official CLOB client missing post_orders batch method")
        return PolymarketLivePreflight(True, "official CLOB V2 client ready")

    def post_paired_fok_market_buys(
        self,
        *,
        yes_token_id: str,
        no_token_id: str,
        yes_worst_price: float,
        no_worst_price: float,
        yes_amount_usdc: float,
        no_amount_usdc: float,
    ) -> dict[str, Any]:
        preflight = self.preflight()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        create_market_order = self._method(client, "create_market_order", "createMarketOrder")
        post_orders = self._method(client, "post_orders", "postOrders")
        if create_market_order is None or post_orders is None:
            raise RuntimeError("paired FOK batch methods unavailable")

        side_buy = self._enum_value("Side", "BUY", fallback="BUY")
        fok = self._enum_value("OrderType", "FOK", fallback="FOK")
        options = {
            "tickSize": str(self.settings.live_complete_set_arb_pilot_tick_size),
            "negRisk": bool(self.settings.live_complete_set_arb_pilot_neg_risk),
        }
        snake_options = {
            "tick_size": str(self.settings.live_complete_set_arb_pilot_tick_size),
            "neg_risk": bool(self.settings.live_complete_set_arb_pilot_neg_risk),
        }
        user_balance = float(self.settings.live_complete_set_arb_pilot_capital_usdc)
        yes_order = self._create_market_order_with_fallbacks(
            create_market_order,
            token_id=yes_token_id,
            side=side_buy,
            amount=yes_amount_usdc,
            price=yes_worst_price,
            user_balance=user_balance,
            options=options,
            snake_options=snake_options,
        )
        no_order = self._create_market_order_with_fallbacks(
            create_market_order,
            token_id=no_token_id,
            side=side_buy,
            amount=no_amount_usdc,
            price=no_worst_price,
            user_balance=user_balance,
            options=options,
            snake_options=snake_options,
        )
        batch_args = [
            {"order": yes_order, "orderType": fok},
            {"order": no_order, "orderType": fok},
        ]
        response = post_orders(batch_args)
        return {
            "yes_order": yes_order,
            "no_order": no_order,
            "response": response,
            "success": self._batch_success(response),
        }

    def _load_sdk(self) -> Any:
        if self._sdk is not None:
            return self._sdk
        self._sdk = importlib.import_module("py_clob_client_v2")
        return self._sdk

    def _build_client(self) -> Any:
        if self._client is not None:
            return self._client
        sdk = self._load_sdk()
        clob_client = getattr(sdk, "ClobClient")
        api_creds_cls = getattr(sdk, "ApiCreds")
        creds = api_creds_cls(
            api_key=self.settings.live_complete_set_arb_pilot_api_key,
            api_secret=self.settings.live_complete_set_arb_pilot_api_secret,
            api_passphrase=self.settings.live_complete_set_arb_pilot_api_passphrase,
        )
        self._client = clob_client(
            host=self.settings.live_complete_set_arb_pilot_host,
            chain_id=int(self.settings.live_complete_set_arb_pilot_chain_id),
            key=self.settings.live_complete_set_arb_pilot_private_key,
            creds=creds,
            signature_type=int(self.settings.live_complete_set_arb_pilot_signature_type),
            funder=self.settings.live_complete_set_arb_pilot_funder_address,
        )
        return self._client

    def _enum_value(self, enum_name: str, member: str, *, fallback: str) -> Any:
        sdk = self._load_sdk()
        enum = getattr(sdk, enum_name, None)
        if enum is None:
            return fallback
        return getattr(enum, member, fallback)

    @staticmethod
    def _method(client: Any, *names: str) -> Any:
        for name in names:
            method = getattr(client, name, None)
            if callable(method):
                return method
        return None

    @staticmethod
    def _create_market_order_with_fallbacks(
        create_market_order: Any,
        *,
        token_id: str,
        side: Any,
        amount: float,
        price: float,
        user_balance: float,
        options: dict[str, Any],
        snake_options: dict[str, Any],
    ) -> Any:
        payloads = [
            {"tokenID": token_id, "side": side, "amount": float(amount), "price": float(price), "userUSDCBalance": user_balance},
            {"token_id": token_id, "side": side, "amount": float(amount), "price": float(price), "user_usdc_balance": user_balance},
        ]
        errors: list[str] = []
        for payload in payloads:
            for opts in (options, snake_options):
                try:
                    return create_market_order(payload, opts)
                except TypeError as exc:
                    errors.append(str(exc))
                    continue
        raise RuntimeError("create_market_order failed with supported payload shapes: " + " | ".join(errors[-4:]))

    @staticmethod
    def _batch_success(response: Any) -> bool:
        if isinstance(response, list):
            if len(response) < 2:
                return False
            return all(PolymarketLiveCompleteSetClient._order_success(item) for item in response[:2])
        if isinstance(response, dict):
            if "success" in response:
                return bool(response.get("success"))
            nested = response.get("orders") or response.get("responses") or response.get("data")
            if isinstance(nested, list):
                return PolymarketLiveCompleteSetClient._batch_success(nested)
        return False

    @staticmethod
    def _order_success(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        if "success" in payload:
            return bool(payload.get("success"))
        status = str(payload.get("status") or "").strip().lower()
        return status in {"filled", "matched", "success", "live", "open"}
