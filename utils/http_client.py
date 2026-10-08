"""One pooled HTTP client per process.

``httpx.get`` / ``httpx.stream`` build a throwaway ``Client`` per call, so every
manifest fetch and every asset download paid a fresh TCP connect + TLS handshake
and then closed the connection. With a few thousand small files per trip that
setup/teardown dominated the wall clock. A single reused ``Client`` keeps the
connection to the API host and to the R2 presigned host alive across requests.

**Fork safety.** ``app.main`` fetches the dataset manifest in the parent *before*
forking its workers, so a plain module-level client would be inherited by every
child and they would all write into the same TLS socket. The client is therefore
keyed on the pid that built it and rebuilt after a fork.

``keepalive_expiry`` is raised well above httpx's 5s default: a worker spends
seconds formatting/validating a trip between downloads, and a 5s idle timeout
would drop the pooled connection in exactly that gap and re-handshake anyway.
"""

import os

import httpx

# Bigger than the gap between a trip's last download and the next trip's first
# request (format + mcap + validate), so the pooled connection survives it.
KEEPALIVE_EXPIRY_SECONDS = 60.0

_client: httpx.Client | None = None
_client_pid: int | None = None


def client() -> httpx.Client:
    """The calling process's shared client, built on first use (and after a fork)."""
    global _client, _client_pid
    if _client is None or _client_pid != os.getpid():
        # Sized to what the busiest caller in this process actually asks for.
        # Before this was sized, `httpx.Limits(keepalive_expiry=...)` was passed
        # with nothing else — and `Limits` defaults its two counts to None, which
        # is UNBOUNDED (the 100/20 figures are httpx's `DEFAULT_LIMITS`, used only
        # when no `Limits` is passed at all). So this is a tightening from
        # unbounded, not a change from 100/20.
        # Download workers run `max_parallel_files` transfers at once; QA's
        # manifest coverage pass runs `manifest_fetch_workers` requests at once,
        # in the same process and through this same client. Sizing for downloads
        # alone left QA's ten threads sharing four connections, so six waited and
        # could time out of the pool rather than on the network. Plus two, for the
        # API host alongside the presigned one.
        from utils.settings import settings  # noqa: PLC0415 - avoids an import cycle

        pool = max(settings.max_parallel_files, settings.manifest_fetch_workers, 1) + 2
        # Do NOT close an inherited client here: its sockets belong to the parent.
        # NOT follow_redirects=True. httpx strips `Authorization` when a redirect
        # crosses origins but forwards any other header, and the API is
        # authenticated with a custom `ApiKey` one — so a client-wide default
        # would hand the dataset credential to whatever a 3xx pointed at. The
        # module-level httpx.get this replaced did not follow redirects either;
        # asset downloads opt in per request, and those URLs carry no header.
        _client = httpx.Client(
            limits=httpx.Limits(
                max_connections=pool,
                max_keepalive_connections=pool,
                keepalive_expiry=KEEPALIVE_EXPIRY_SECONDS,
            ),
        )
        _client_pid = os.getpid()
    return _client
