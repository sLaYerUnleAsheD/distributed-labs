# Lab 3 test harness — COMP41720 (2026-27)

`lab3_harness.py` is the checker the demonstrator runs at your machine at the
end of the Lab 3 session. It is also yours: run it while you work, and arrive
already green.

## Requirements

Python 3.8+ only — no packages to install. It talks to your service on
`http://localhost:8080`, and probes your metrics endpoint, Prometheus
(`:9090`) and Grafana (`:3000`).

## How to run it

```
python3 lab3_harness.py --quick      # while developing: contract + reachability
python3 lab3_harness.py              # the full session check (interactive)
```

The full run walks the three criteria on the demonstrator's sheet:

1. **The contract** — `POST /generate {"prompt": "..."}` answers 2xx with
   generated content, repeatedly. Your metrics endpoint, Prometheus and
   Grafana are probed here too (warnings now beat surprises later).
2. **The breaker, live** — the harness confirms CLOSED with a successful
   request; you break your LLM dependency (stop Ollama, or point
   `LLM_API_URL` at a dead port and restart); it drives requests and watches
   slow, retried failures give way to **instant `503 {"status":
   "circuit_open", ...}`** — the OPEN fallback the brief mandates. Then you
   restore the dependency, it waits out your open timeout, and watches the
   HALF-OPEN trial succeed and stay succeeding (CLOSED). If your `/metrics`
   exposes a circuit-state gauge, the observed state sequence is reported as
   corroborating evidence — but the behaviour is what is judged.
3. **Grafana during chaos** — Prometheus is queried for your service's
   metric names (a circuit metric is looked for by name), Grafana's health
   is checked, and the demonstrator confirms your dashboard showed request
   rate, error rate and circuit state moving during the run.

Useful flags: `--timeout 180` for a cold model, `--metrics URL` if your
metrics live somewhere other than `/metrics` or `/actuator/prometheus`,
`--base/--prom/--grafana` for non-default ports.

Two tips: the fail-fast contrast is what proves OPEN, so keep your retry
backoff at the brief's defaults (visible seconds) rather than milliseconds;
and return the exact `circuit_open` body — the harness keys on it.

The harness never modifies your code, your services or your dashboards; it
only sends requests and reads health/metrics endpoints. Exit code 0 means
every check it ran passed.
