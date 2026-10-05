"""FastAPI backend for the Stock Intelligence Platform.

Layers: routers (HTTP/WebSocket) -> services (use cases, degradation rules)
-> repositories (PostgreSQL history, Redis latest state). Routers never touch
storage directly; repositories never know about HTTP.
"""
