"""A parent process for the parent-death test: starts an engine, prints its JVM pid, reads frames until killed."""
import json
import sys

from cdc_v2.deadline import Deadline
from cdc_v2.stream.engine import Engine, EngineSettings
from cdc_v2.stream.jvm import JvmArtifacts

spec = json.load(open(sys.argv[1]))
s = spec["settings"]
s["message_prefixes"] = tuple(s["message_prefixes"])
eng = Engine(EngineSettings(**s), None, JvmArtifacts(**spec["artifacts"]))
print(json.dumps({"jvm_pid": eng.pid}), flush=True)
while True:
    for _ in eng.frames(Deadline.after(1)):
        pass
