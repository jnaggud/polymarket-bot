# Security Policy

## Reporting a vulnerability

Please do not open a public issue for vulnerabilities involving credentials, order placement, wallet signing,
geographic restrictions, or the possibility of unintended live execution.

Use GitHub's private vulnerability-reporting flow for this repository. Include:

- the affected command or module;
- the required configuration and preconditions;
- a minimal reproduction;
- the possible impact; and
- any suggested mitigation.

Reports will be acknowledged as soon as practical. A fix may be developed privately before coordinated disclosure.

## Supported version

Security fixes target the latest commit on `main`. Earlier research snapshots are not maintained separately.

## Credential handling

- Real secrets belong only in ignored local environment files or an external secret manager.
- Never commit private keys, API secrets, passphrases, or funded wallet addresses tied to private operations.
- Treat logs, state databases, dashboard payloads, and screenshots as potentially sensitive.
- Rotate a credential immediately if it is exposed; deleting it from Git history is not sufficient.

## Live-trading boundary

This project is paper-first. A report that bypasses dry-run defaults, explicit confirmation, geoblock checks,
reconciliation, heartbeat, loss limits, exposure limits, or strategy-validation gates should be treated as high
priority.
