"""The fuzzers of the robustness bar (SPEC §4, R22; PLAN §6) and their campaign ledger.

    tools/fuzz run --fuzzer workload|fault|stream|leadership --system toy[:variant] --source-node replica|primary \
                   [--seeds A..B | --hours H] --workers N
    tools/fuzz status [--write]
    tools/fuzz suite-1.7 --system toy
    tools/fuzz clean

Everything attacks a `System` (system.py); docs/testing/fuzzers.md is the full account. The pipeline never imports
this package.
"""
