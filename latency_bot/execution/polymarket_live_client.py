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

    def _preflight_account(self) -> PolymarketLivePreflight:
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
        return PolymarketLivePreflight(True, "official CLOB V2 account client ready")

    def preflight(self) -> PolymarketLivePreflight:
        account = self._preflight_account()
        if not account.ok:
            return account
        client = self._build_client()
        if not self._method(client, "create_market_order", "createMarketOrder"):
            return PolymarketLivePreflight(False, "official CLOB client missing create_market_order")
        if not self._method(client, "post_orders", "postOrders"):
            return PolymarketLivePreflight(False, "official CLOB client missing post_orders batch method")
        if not self._method(client, "post_order", "postOrder"):
            return PolymarketLivePreflight(False, "official CLOB client missing post_order method")
        return PolymarketLivePreflight(True, "official CLOB V2 client ready")

    def preflight_maker(self) -> PolymarketLivePreflight:
        base = self._preflight_account()
        if not base.ok:
            return base
        client = self._build_client()
        if not self._method(client, "create_order", "createOrder"):
            return PolymarketLivePreflight(False, "official CLOB client missing create_order limit method")
        if not self._method(client, "post_order", "postOrder"):
            return PolymarketLivePreflight(False, "official CLOB client missing post_order method")
        if not self._method(client, "get_open_orders", "getOpenOrders"):
            return PolymarketLivePreflight(False, "official CLOB client missing get_open_orders")
        if not self._method(client, "cancel", "cancel_order", "cancelOrder"):
            return PolymarketLivePreflight(False, "official CLOB client missing cancel order method")
        return PolymarketLivePreflight(True, "official CLOB V2 maker client ready")

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
            self._load_sdk(),
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
            self._load_sdk(),
            create_market_order,
            token_id=no_token_id,
            side=side_buy,
            amount=no_amount_usdc,
            price=no_worst_price,
            user_balance=user_balance,
            options=options,
            snake_options=snake_options,
        )
        batch_arg_cls = getattr(self._load_sdk(), "PostOrdersV2Args", None) or getattr(self._load_sdk(), "PostOrdersV1Args", None)
        if batch_arg_cls is None:
            raise RuntimeError("official CLOB SDK missing PostOrdersArgs class")
        batch_args = [
            batch_arg_cls(order=yes_order, orderType=fok),
            batch_arg_cls(order=no_order, orderType=fok),
        ]
        response = post_orders(batch_args)
        return {
            "yes_order": yes_order,
            "no_order": no_order,
            "response": response,
            "success": self._batch_success(response),
        }

    def post_fok_market_sell(
        self,
        *,
        token_id: str,
        shares: float,
        worst_price: float = 0.0,
    ) -> dict[str, Any]:
        preflight = self.preflight()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        create_market_order = self._method(client, "create_market_order", "createMarketOrder")
        post_order = self._method(client, "post_order", "postOrder")
        if create_market_order is None or post_order is None:
            raise RuntimeError("single-leg FOK rescue methods unavailable")

        side_sell = self._enum_value("Side", "SELL", fallback="SELL")
        fok = self._enum_value("OrderType", "FOK", fallback="FOK")
        options = {
            "tickSize": str(self.settings.live_complete_set_arb_pilot_tick_size),
            "negRisk": bool(self.settings.live_complete_set_arb_pilot_neg_risk),
        }
        snake_options = {
            "tick_size": str(self.settings.live_complete_set_arb_pilot_tick_size),
            "neg_risk": bool(self.settings.live_complete_set_arb_pilot_neg_risk),
        }
        order = self._create_market_order_with_fallbacks(
            self._load_sdk(),
            create_market_order,
            token_id=token_id,
            side=side_sell,
            amount=shares,
            price=worst_price,
            user_balance=float(self.settings.live_complete_set_arb_pilot_capital_usdc),
            options=options,
            snake_options=snake_options,
        )
        response = post_order(order, order_type=fok)
        return {
            "order": order,
            "response": response,
            "success": self._order_success(response),
        }

    def post_limit_buy(
        self,
        *,
        token_id: str,
        price: float,
        size: float,
    ) -> dict[str, Any]:
        preflight = self.preflight_maker()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        create_order = self._method(client, "create_order", "createOrder")
        post_order = self._method(client, "post_order", "postOrder")
        if create_order is None or post_order is None:
            raise RuntimeError("limit maker order methods unavailable")
        side_buy = self._enum_value("Side", "BUY", fallback="BUY")
        gtc = self._enum_value("OrderType", "GTC", fallback="GTC")
        options = {
            "tickSize": str(self.settings.live_complete_set_arb_pilot_tick_size),
            "negRisk": bool(self.settings.live_complete_set_arb_pilot_neg_risk),
        }
        snake_options = {
            "tick_size": str(self.settings.live_complete_set_arb_pilot_tick_size),
            "neg_risk": bool(self.settings.live_complete_set_arb_pilot_neg_risk),
        }
        order = self._create_limit_order_with_fallbacks(
            self._load_sdk(),
            create_order,
            token_id=token_id,
            side=side_buy,
            price=price,
            size=size,
            options=options,
            snake_options=snake_options,
        )
        response = self._post_order_with_fallbacks(post_order, order, gtc)
        return {
            "order": order,
            "response": response,
            "success": self._order_success(response),
        }

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        preflight = self.preflight_maker()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        cancel = self._method(client, "cancel", "cancel_order", "cancelOrder")
        if cancel is None:
            raise RuntimeError("cancel order method unavailable")
        payloads: list[Any] = [order_id, {"orderID": order_id}, {"order_id": order_id}, {"id": order_id}]
        errors: list[str] = []
        for payload in payloads:
            try:
                response = cancel(payload)
                return {
                    "response": response,
                    "success": self._cancel_success(response),
                }
            except TypeError as exc:
                errors.append(str(exc))
                continue
        raise RuntimeError("cancel order failed with supported payload shapes: " + " | ".join(errors[-4:]))

    def cancel_all_orders(self) -> dict[str, Any]:
        preflight = self.preflight_maker()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        cancel_all = self._method(client, "cancel_all", "cancel_all_orders", "cancelAll", "cancelAllOrders")
        if cancel_all is not None:
            response = cancel_all()
            return {"response": response, "success": self._cancel_success(response)}
        open_orders = self.get_open_orders()
        cancelled: list[dict[str, Any]] = []
        for order in open_orders:
            order_id = str(order.get("id") or order.get("order_id") or order.get("orderID") or "").strip()
            if not order_id:
                continue
            cancelled.append(self.cancel_order(order_id))
        return {
            "response": cancelled,
            "success": all(bool(item.get("success")) for item in cancelled) if cancelled else True,
            "fallback": "cancel_each_open_order",
        }

    def get_order_scoring_status(self, order_id: str) -> dict[str, Any]:
        """Return observed liquidity-reward scoring status for a submitted order."""
        preflight = self._preflight_account()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        method = self._method(
            client,
            "is_order_scoring",
            "isOrderScoring",
            "get_order_scoring",
            "getOrderScoring",
        )
        if method is None:
            return {"available": False, "scoring": None, "reason": "official CLOB client has no order-scoring method"}
        response: Any = method(order_id)
        if isinstance(response, dict):
            scoring = response.get("scoring")
            if scoring is None:
                scoring = response.get("is_scoring", response.get("isScoring"))
        else:
            scoring = response if isinstance(response, bool) else None
        return {"available": True, "scoring": scoring, "response": response}

    def get_collateral_balance_allowance(self) -> dict[str, Any]:
        preflight = self._preflight_account()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        sdk = self._load_sdk()
        get_balance_allowance = self._method(client, "get_balance_allowance", "getBalanceAllowance")
        if get_balance_allowance is None:
            raise RuntimeError("official CLOB client missing get_balance_allowance")
        params_cls = getattr(sdk, "BalanceAllowanceParams", None)
        asset_type = getattr(sdk, "AssetType", None)
        if params_cls is None or asset_type is None:
            raise RuntimeError("official CLOB SDK missing balance allowance params")
        params = params_cls(
            asset_type=getattr(asset_type, "COLLATERAL", "COLLATERAL"),
            signature_type=int(self.settings.live_complete_set_arb_pilot_signature_type),
        )
        response = get_balance_allowance(params)
        return response if isinstance(response, dict) else {"response": response}

    def get_open_orders(self) -> list[dict[str, Any]]:
        preflight = self._preflight_account()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        get_open_orders = self._method(client, "get_open_orders", "getOpenOrders")
        if get_open_orders is None:
            raise RuntimeError("official CLOB client missing get_open_orders")
        response = get_open_orders()
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        return []

    def get_recent_trades(self) -> list[dict[str, Any]]:
        preflight = self._preflight_account()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        sdk = self._load_sdk()
        get_trades = self._method(client, "get_trades", "getTrades")
        if get_trades is None:
            raise RuntimeError("official CLOB client missing get_trades")
        params_cls = getattr(sdk, "TradeParams", None)
        if params_cls is None:
            raise RuntimeError("official CLOB SDK missing trade params")
        params = params_cls(maker_address=self.settings.live_complete_set_arb_pilot_funder_address)
        response = get_trades(params, only_first_page=True)
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        return []

    def get_positions(self) -> list[dict[str, Any]]:
        preflight = self._preflight_account()
        if not preflight.ok:
            raise RuntimeError(preflight.reason)
        client = self._build_client()
        get_positions = self._method(client, "get_positions", "getPositions")
        if get_positions is None:
            return []
        response = get_positions()
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        if isinstance(response, dict):
            items = response.get("positions") or response.get("data") or response.get("items") or []
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return []

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
        sdk: Any,
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
        market_order_args_cls = getattr(sdk, "MarketOrderArgs", None)
        options_cls = getattr(sdk, "PartialCreateOrderOptions", None)
        if market_order_args_cls is not None and options_cls is not None:
            order_args = market_order_args_cls(
                token_id=token_id,
                side=side,
                amount=float(amount),
                price=float(price),
                order_type=getattr(sdk.OrderType, "FOK", "FOK"),
                user_usdc_balance=user_balance,
            )
            create_options = options_cls(**snake_options)
            return create_market_order(order_args, create_options)

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
    def _create_limit_order_with_fallbacks(
        sdk: Any,
        create_order: Any,
        *,
        token_id: str,
        side: Any,
        price: float,
        size: float,
        options: dict[str, Any],
        snake_options: dict[str, Any],
    ) -> Any:
        order_args_cls = getattr(sdk, "OrderArgs", None) or getattr(sdk, "LimitOrderArgs", None)
        options_cls = getattr(sdk, "PartialCreateOrderOptions", None)
        if order_args_cls is not None and options_cls is not None:
            order_args = order_args_cls(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=side,
            )
            create_options = options_cls(**snake_options)
            return create_order(order_args, create_options)

        payloads = [
            {"tokenID": token_id, "side": side, "price": float(price), "size": float(size)},
            {"token_id": token_id, "side": side, "price": float(price), "size": float(size)},
        ]
        errors: list[str] = []
        for payload in payloads:
            for opts in (options, snake_options):
                try:
                    return create_order(payload, opts)
                except TypeError as exc:
                    errors.append(str(exc))
                    continue
        raise RuntimeError("create_order failed with supported payload shapes: " + " | ".join(errors[-4:]))

    @staticmethod
    def _post_order_with_fallbacks(post_order: Any, order: Any, order_type: Any) -> Any:
        errors: list[str] = []
        attempts = (
            lambda: post_order(order, order_type=order_type),
            lambda: post_order(order, orderType=order_type),
            lambda: post_order(order, order_type),
            lambda: post_order(order),
        )
        for attempt in attempts:
            try:
                return attempt()
            except TypeError as exc:
                errors.append(str(exc))
                continue
        raise RuntimeError("post_order failed with supported payload shapes: " + " | ".join(errors[-4:]))

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
        error_msg = str(payload.get("errorMsg") or payload.get("error_msg") or "").strip()
        if error_msg:
            return False
        status = str(payload.get("status") or "").strip().lower()
        if status:
            return status in {"filled", "matched", "success", "live", "open"}
        return False

    @staticmethod
    def _cancel_success(payload: Any) -> bool:
        if isinstance(payload, list):
            return all(PolymarketLiveCompleteSetClient._cancel_success(item) for item in payload)
        if not isinstance(payload, dict):
            return payload is None
        error_msg = str(payload.get("errorMsg") or payload.get("error_msg") or payload.get("error") or "").strip()
        if error_msg:
            return False
        if "success" in payload:
            return bool(payload.get("success"))
        status = str(payload.get("status") or "").strip().lower()
        if status:
            return status in {"cancelled", "canceled", "success"}
        return True
