"""Print `docs/state_machines.md` from the declared machines: ``uv run python -m cdc_v2.state``."""
import sys

from cdc_v2 import state

state.load_all()
sys.stdout.write(state.render_document())
