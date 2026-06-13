from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any


ENV_KEYS = {
    "private_key": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_PRIVATE_KEY",
    "api_key": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_API_KEY",
    "api_secret": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_API_SECRET",
    "api_passphrase": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_API_PASSPHRASE",
    "funder": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_FUNDER_ADDRESS",
    "signature_type": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_SIGNATURE_TYPE",
    "host": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_HOST",
    "chain_id": "LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_CHAIN_ID",
}


def _load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def _normalize_private_key(raw: str) -> str:
    key = raw.strip().strip("'").strip('"')
    if key.startswith("0x"):
        key_body = key[2:]
    else:
        key_body = key
        key = f"0x{key_body}"
    if not re.fullmatch(r"[0-9a-fA-F]{64}", key_body):
        raise SystemExit("PRIVATE_KEY must be 64 hex characters, optionally prefixed with 0x.")
    return key


def _cred_value(creds: Any, *names: str) -> str:
    if isinstance(creds, dict):
        for name in names:
            value = creds.get(name)
            if value:
                return str(value)
    for name in names:
        value = getattr(creds, name, None)
        if value:
            return str(value)
    return ""


def _replace_or_append_env(path: Path, updates: dict[str, str]) -> None:
    existing = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    output: list[str] = []
    for line in existing:
        if "=" not in line or line.lstrip().startswith("#"):
            output.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            output.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            output.append(line)
    for key, value in updates.items():
        if key not in seen:
            output.append(f"{key}={value}")
    path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Derive Polymarket CLOB API credentials from local .env private key.")
    parser.add_argument("--env-file", default=".env", help="Path to local env file to read/update.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing API credential fields.")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    env = _load_env(env_path)
    private_key = _normalize_private_key(env.get(ENV_KEYS["private_key"], ""))
    funder = env.get(ENV_KEYS["funder"], "").strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", funder):
        raise SystemExit("FUNDER_ADDRESS must be a 0x-prefixed EVM address.")

    existing = [ENV_KEYS["api_key"], ENV_KEYS["api_secret"], ENV_KEYS["api_passphrase"]]
    if not args.overwrite and any(env.get(key) for key in existing):
        raise SystemExit("API credential fields already contain values. Re-run with --overwrite to replace them.")

    from py_clob_client_v2 import ClobClient

    host = env.get(ENV_KEYS["host"], "https://clob.polymarket.com") or "https://clob.polymarket.com"
    chain_id = int(env.get(ENV_KEYS["chain_id"], "137") or "137")
    signature_type = int(env.get(ENV_KEYS["signature_type"], "3") or "3")
    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )
    creds = client.create_or_derive_api_key()
    api_key = _cred_value(creds, "key", "api_key", "apiKey")
    api_secret = _cred_value(creds, "secret", "api_secret", "apiSecret")
    api_passphrase = _cred_value(creds, "passphrase", "api_passphrase", "apiPassphrase")
    if not api_key or not api_secret or not api_passphrase:
        raise SystemExit("SDK returned incomplete credentials; .env was not updated.")

    _replace_or_append_env(
        env_path,
        {
            ENV_KEYS["private_key"]: private_key,
            ENV_KEYS["api_key"]: api_key,
            ENV_KEYS["api_secret"]: api_secret,
            ENV_KEYS["api_passphrase"]: api_passphrase,
        },
    )
    print(f"Updated {env_path} with derived CLOB API credentials.")
    print("Credential values were not printed. Keep .env ignored and private.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
