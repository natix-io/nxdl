def safe_float(value, default=0.0):
    try:
        return float(value) if value else default
    except ValueError:
        return default

def safe_int(value, default=None):
    """Safely convert a value to int, returning default on failure."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default