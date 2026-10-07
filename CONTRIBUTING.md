# Contributing

Thanks for helping improve the Polymarket Research Engine. Keep contributions focused, reproducible, and safe by
default.

## Development setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env
```

Never commit `.env`, venue credentials, wallet keys, state databases, or logs.

## Before opening a pull request

```bash
ruff check .
python3 -W error::DeprecationWarning -W error::ResourceWarning -m unittest discover -s tests
python3 main.py --help >/dev/null
```

Add tests for behavioral changes. For execution or accounting changes, include the assumptions being changed and a
fixture that proves legacy results cannot leak into the new execution epoch.

## Pull requests

- Explain the problem, approach, user impact, and validation performed.
- Keep live trading disabled and dry-run by default.
- Do not weaken geoblock, confirmation, reconciliation, heartbeat, loss, exposure, or strategy-validation gates.
- Label PnL according to its evidence tier: model, executable paper, observed rebate, or wallet reconciled.
- Keep generated runtime data out of the repository.

Small, reviewable pull requests are preferred over broad rewrites.
