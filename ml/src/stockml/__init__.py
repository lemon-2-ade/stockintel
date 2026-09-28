"""Offline data and ML pipeline for the Stock Intelligence Platform.

Stages (see docs/DATA_PIPELINE.md): acquire -> validate -> clean -> features
-> train -> evaluate. Raw snapshots are immutable; everything downstream is
reproducible from them.
"""
