"""Shared, standard-library-only contracts; JSON boundaries use plain dicts."""

from .semantic_target import SemanticTarget, MutationSlot
from .state_machine import StateCondition, StateStep, StateExecutor, StepReceipt
from .scenario import Scenario, compile_scenario
from .mutation import MutationReceipt
from .evidence import canonical_hash, effective_input_hash, raw_sha256, verify_raw
from .execution import ExecutionStage, ExecutionRecord, validate_execution, validate_fault_control_execution
from .registry import Registry
from .interfaces import Module, TargetProvider, MutationProvider, Carrier, FactProducer, Oracle, ExecutionServices
from .oracle import OracleVerdict, TriageResult, attribute_responsibility, validate_control, unknown_gate
from .candidate import CandidateLifecycle, CandidateStatus
from .replay import ReplayCapsule, ReplayStatus, validate_replay_input, rerun_conformance, conformance_signature

__all__ = [
    "SemanticTarget", "MutationSlot", "StateCondition", "StateStep", "StateExecutor",
    "StepReceipt", "Scenario", "compile_scenario", "MutationReceipt", "canonical_hash",
    "effective_input_hash", "raw_sha256", "verify_raw", "ExecutionStage", "ExecutionRecord",
    "validate_execution", "validate_fault_control_execution", "Registry", "Module", "TargetProvider", "MutationProvider", "Carrier",
    "FactProducer", "Oracle", "ExecutionServices", "OracleVerdict", "TriageResult", "attribute_responsibility",
    "validate_control", "unknown_gate", "CandidateLifecycle", "CandidateStatus", "ReplayCapsule",
    "ReplayStatus", "validate_replay_input", "rerun_conformance", "conformance_signature",
]
