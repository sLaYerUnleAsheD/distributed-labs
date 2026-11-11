import os

# Server Config
PORT = int(os.getenv("PORT", 8080))
LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:11434/api/generate")
# LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:9999/api/generate")

LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:1b")

# Retry Configuration
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", 3))
RETRY_INITIAL_DELAY = float(os.getenv("RETRY_INITIAL_DELAY", 1.0))
RETRY_MULTIPLIER = float(os.getenv("RETRY_MULTIPLIER", 2.0))

# Circuit Breaker Configuration
# Breaker tracks windows of 10 requests; opens if 5 or more fail
CB_WINDOW_SIZE = int(os.getenv("CB_WINDOW_SIZE", 10))
CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", 5))
CB_OPEN_TIMEOUT = float(os.getenv("CB_OPEN_TIMEOUT", 10.0))

# Timeout Configuration
LLM_TIMEOUT_SECONDS = float(os.getenv("LLM_TIMEOUT_SECONDS", 30.0))