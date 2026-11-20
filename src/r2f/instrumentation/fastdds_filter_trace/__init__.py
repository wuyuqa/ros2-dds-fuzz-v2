"""Explicit instrumentation-only Fast DDS evaluate tracing.

Importing this package does not load a DSO, change the environment, or create
DDS entities. A trace is an actual evaluate invocation, not a cached relevance
query; absence of a trace does not imply acceptance or complete coverage.
"""

SCHEMA_VERSION = 1
RECORD_SIZE = 4192
STATS_SIZE = 48
PAYLOAD_CAPACITY = 4096
RING_CAPACITY = 128
