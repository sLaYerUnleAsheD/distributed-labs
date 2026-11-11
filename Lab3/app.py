import time
import logging
import threading
from collections import deque
from typing import Dict, Any, Optional

import requests
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

import config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s"
)
logger = logging.getLogger("resilient-llm-service")

app = FastAPI(title="Resilient LLM API Service")

# --------------------------------------------------------------------------
# Clean up Prometheus Registry to prevent DuplicateTimeseries errors
# --------------------------------------------------------------------------
for metric_name in [
    "llm_requests",
    "llm_errors",
    "llm_request_duration_seconds",
    "circuit_breaker_state",
    "resilience4j_circuitbreaker_state",
]:
    if metric_name in REGISTRY._names_to_collectors:
        try:
            REGISTRY.unregister(REGISTRY._names_to_collectors[metric_name])
        except KeyError:
            pass

# --------------------------------------------------------------------------
# Prometheus Observability Metrics
# --------------------------------------------------------------------------
REQUEST_COUNT = Counter(
    "llm_requests",
    "Total HTTP requests served by /generate",
    ["status_code"]
)
ERROR_COUNT = Counter(
    "llm_errors",
    "Total errors encountered when executing /generate",
    ["type"]
)
LATENCY_HISTOGRAM = Histogram(
    "llm_request_duration_seconds",
    "Latency histogram for /generate requests in seconds",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)
# Circuit State Gauge: 0=CLOSED, 1=OPEN, 2=HALF-OPEN
CIRCUIT_STATE_GAUGE = Gauge(
    "circuit_breaker_state",
    "Current state of the circuit breaker (0=CLOSED, 1=OPEN, 2=HALF-OPEN)"
)
# Per-state label gauge compatibility for lab harness
CIRCUIT_STATE_LABEL_GAUGE = Gauge(
    "resilience4j_circuitbreaker_state",
    "Circuit breaker state as a labelled gauge",
    ["state"]
)


def update_prometheus_circuit_state(state_str: str):
    mapping = {"CLOSED": 0, "OPEN": 1, "HALF-OPEN": 2}
    val = mapping.get(state_str.upper(), 0)
    CIRCUIT_STATE_GAUGE.set(val)
    
    for s in ["closed", "open", "half_open"]:
        CIRCUIT_STATE_LABEL_GAUGE.labels(state=s).set(0.0)
    
    label_key = state_str.lower().replace("-", "_")
    CIRCUIT_STATE_LABEL_GAUGE.labels(state=label_key).set(1.0)


# Initialize gauge to CLOSED
update_prometheus_circuit_state("CLOSED")

# --------------------------------------------------------------------------
# Circuit Breaker Implementation
# --------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(
        self,
        window_size: int = config.CB_WINDOW_SIZE,
        failure_threshold: int = config.CB_FAILURE_THRESHOLD,
        open_timeout: float = config.CB_OPEN_TIMEOUT,
    ):
        self.window_size = window_size
        self.failure_threshold = failure_threshold
        self.open_timeout = open_timeout
        
        self.state = "CLOSED"
        self.recent_results = deque(maxlen=window_size)  # True = success, False = failure
        self.last_state_change = time.monotonic()
        self.lock = threading.Lock()

    def allow_request(self) -> bool:
        with self.lock:
            now = time.monotonic()
            if self.state == "OPEN":
                if now - self.last_state_change >= self.open_timeout:
                    self._transition_to("HALF-OPEN")
                    return True
                return False
            return True

    def record_result(self, success: bool):
        with self.lock:
            if self.state == "CLOSED":
                self.recent_results.append(success)
                failures = self.recent_results.count(False)
                if len(self.recent_results) >= 5 and failures >= self.failure_threshold:
                    self._transition_to("OPEN")
            elif self.state == "HALF-OPEN":
                if success:
                    self._transition_to("CLOSED")
                else:
                    self._transition_to("OPEN")

    def _transition_to(self, new_state: str):
        logger.info(f"[CIRCUIT BREAKER] Transitioning: {self.state} -> {new_state}")
        self.state = new_state
        self.last_state_change = time.monotonic()
        if new_state == "CLOSED":
            self.recent_results.clear()
        update_prometheus_circuit_state(new_state)

    def get_state(self) -> str:
        with self.lock:
            return self.state


circuit_breaker = CircuitBreaker()

# --------------------------------------------------------------------------
# LLM Service Client with Exponential Backoff Retry
# --------------------------------------------------------------------------
class NonRetryableException(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def execute_llm_call_with_retry(prompt: str) -> str:
    """
    Calls downstream LLM API with exponential backoff retry mechanics.
    Retries on 429, 503, connection errors, and timeouts.
    """
    attempt = 0
    delay = config.RETRY_INITIAL_DELAY

    while attempt < config.RETRY_MAX_ATTEMPTS:
        attempt += 1
        start_time = time.monotonic()
        try:
            logger.info(f"[RETRY ATTEMPT] Attempt {attempt}/{config.RETRY_MAX_ATTEMPTS} sending request...")
            
            payload = {
                "model": config.LLM_MODEL,
                "prompt": prompt,
                "stream": False
            }
            
            response = requests.post(
                config.LLM_API_URL,
                json=payload,
                timeout=config.LLM_TIMEOUT_SECONDS
            )
            
            # Non-retryable Auth Errors
            if response.status_code in (401, 403):
                raise NonRetryableException(
                    response.status_code,
                    f"Authentication failure ({response.status_code})"
                )
            
            # Retryable HTTP status codes (429 Rate Limit, 503 Service Unavailable)
            if response.status_code in (429, 503):
                logger.warning(
                    f"[LLM API WARNING] Code {response.status_code} received on attempt {attempt}."
                )
                if attempt >= config.RETRY_MAX_ATTEMPTS:
                    response.raise_for_status()
            else:
                response.raise_for_status()

            # Process Success Body
            res_data = response.json()
            if "response" in res_data:
                return res_data["response"]
            elif "choices" in res_data:
                return res_data["choices"][0]["text"]
            return str(res_data)

        except NonRetryableException:
            raise

        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as exc:
            elapsed = time.monotonic() - start_time
            logger.warning(
                f"[RETRY FAILED] Attempt {attempt} failed after {elapsed:.2f}s with error: {exc}"
            )
            
            if attempt >= config.RETRY_MAX_ATTEMPTS:
                logger.error(f"[RETRY EXHAUSTED] Reached maximum attempts ({config.RETRY_MAX_ATTEMPTS}).")
                raise exc

            logger.info(f"[BACKOFF] Sleeping for {delay:.1f}s before retrying...")
            time.sleep(delay)
            delay *= config.RETRY_MULTIPLIER


# --------------------------------------------------------------------------
# API Data Models & Endpoints
# --------------------------------------------------------------------------
class GenerateRequest(BaseModel):
    prompt: str


@app.post("/generate")
def generate_text(req: GenerateRequest):
    start_time = time.monotonic()

    # 1. Check Circuit Breaker State
    if not circuit_breaker.allow_request():
        logger.warning("[FALLBACK TRIGGERED] Circuit is OPEN. Rejecting request instantly.")
        REQUEST_COUNT.labels(status_code="503").inc()
        ERROR_COUNT.labels(type="circuit_open").inc()
        
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "circuit_open",
                "retry_after": int(config.CB_OPEN_TIMEOUT),
                "message": "LLM service temporarily unavailable"
            }
        )

    # 2. Execute Request with Resilience Patterns
    try:
        output_text = execute_llm_call_with_retry(req.prompt)
        
        # Record Success
        circuit_breaker.record_result(success=True)
        elapsed = time.monotonic() - start_time
        LATENCY_HISTOGRAM.observe(elapsed)
        REQUEST_COUNT.labels(status_code="200").inc()
        
        return {
            "status": "success",
            "generated_text": output_text
        }

    except NonRetryableException as nre:
        circuit_breaker.record_result(success=False)
        REQUEST_COUNT.labels(status_code=str(nre.status_code)).inc()
        ERROR_COUNT.labels(type="auth_error").inc()
        return JSONResponse(
            status_code=nre.status_code,
            content={"error": nre.message}
        )

    except requests.exceptions.Timeout:
        circuit_breaker.record_result(success=False)
        REQUEST_COUNT.labels(status_code="504").inc()
        ERROR_COUNT.labels(type="timeout").inc()
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={"error": "LLM API request timed out."}
        )

    except Exception as exc:
        circuit_breaker.record_result(success=False)
        REQUEST_COUNT.labels(status_code="500").inc()
        ERROR_COUNT.labels(type="llm_failure").inc()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": f"LLM execution failure: {str(exc)}"}
        )


@app.get("/metrics")
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=config.PORT,
        reload=False,
    )