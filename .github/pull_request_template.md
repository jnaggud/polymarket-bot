## Summary

Describe the problem, the approach, and the user or operator impact.

## Execution and risk assumptions

List any changed fill, fee, latency, depth, queue, sizing, reconciliation, or live-pilot assumptions. Write `None` if
the change cannot affect execution or risk.

## Verification

- [ ] `ruff check .`
- [ ] `python3 -W error::DeprecationWarning -W error::ResourceWarning -m unittest discover -s tests`
- [ ] `python3 main.py --help >/dev/null`
- [ ] Live trading remains disabled/dry-run by default
- [ ] No secrets, state files, logs, or private wallet data are included

## Screenshots

Include before/after images for dashboard changes.
