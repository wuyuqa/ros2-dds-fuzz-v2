"""Opt-in Fast DDS 2.14.7 source instrumentation; never loaded by black-box mode.

``patch_vendor`` grants one typed friend access in six vendor headers, without
changing fields or virtual functions. Build provider.cpp separately against the
patched source/configuration and the SAME instrumented vendor library. Neither
importing this package nor applying the patch loads a native library or DDS.

The C ABI is documented/defined in provider.cpp. snapshot_ordinal is an
observation serial in one provider instance, NOT a matching epoch. Runtime
loading, lifetime protection, library provenance and endpoint-pair barriers are
the caller's responsibility, not implemented by this package.
"""

SCHEMA_VERSION = 1
RESULT_SIZE = 64
GUID_SIZE = 16
VENDOR_VERSION = "2.14.7"
