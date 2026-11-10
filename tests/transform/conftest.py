"""Fixtures of the transform tests (package Y). The default run is hermetic: local DuckDB only.

Opt-in:
    uv run pytest -m localpg tests/transform                        # Postgres prints (pg-y, ports 55656-55663)
    uv run pytest -m "localpg and jvm" tests/transform               # the cross-path test with the engine
    CDC_V2_Y_MEASURE=1 uv run pytest tests/transform/test_transform_measure.py -s   # speed and memory
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
