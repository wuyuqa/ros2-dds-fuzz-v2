"""Content-filtered-topic control surface for the unified runtime.

Scope: ROS2 public API only (``rclpy.node.create_subscription`` with
``content_filter_options``, ``Subscription.set_content_filter`` /
``get_content_filter`` / ``is_cft_enabled``).  This module never creates a
context, node, subscription or participant by itself; the worker passes its
already-created public endpoint in.  Import of rclpy is lazy and confined to
the option factory so this module stays importable without ROS.

Evidence rules implemented here:

* An empty expression means the filter is disabled.  It does **not** mean the
  subscription is unmatched, and no receipt may claim that.
* Sample filtering (which messages were delivered) and matching graph
  (publisher/subscription counts and graph rows) are separate observation
  channels.  ``summarize_observations`` keeps them apart and the summary
  carries ``filter_false_implies_unmatch=False``.
* Expression/parameter validation failures and API submission failures are
  layered guard receipts.  They are harness/input rejections, never SUT bugs
  and never candidates (``candidate_eligible=False``).
* Validation is a conservative harness subset.  ``conformance_claim`` is
  always false; only the runtime readback plus observed samples can support a
  configured/effect claim.
"""
from __future__ import annotations

import json
import re
import time

SCHEMA = 'r2f.participant-filter/v1'
MODULE = 'r2f.runtime.filter_backend'
# Canonical mutation stages are recorded nowhere in these receipts: a filter
# step is configuration control, not a mutation hit.
RECEIPT_STAGES = ('planned', 'submitted', 'carrier_admitted', 'configured',
                  'applied', 'hit', 'effect_observed')
LAYERS = ('expression_syntax', 'expression_type', 'parameter_type',
          'parameter_index', 'parameter_unexpected', 'unsupported_feature',
          'unsupported_field', 'submission_api', 'submission_rcl',
          'readback_mismatch')

_TOKEN = re.compile(
    r"\s*(?:(?P<param>%\d+)|(?P<number>-?\d+(?:\.\d+)?)"
    r"|(?P<string>'(?:[^'\\]|\\.)*')|(?P<op><=|>=|<>|=|<|>)"
    r"|(?P<lparen>\()|(?P<rparen>\))|(?P<ident>[A-Za-z_][A-Za-z0-9_.]*))")
_KEYWORDS = {'AND', 'OR', 'NOT', 'BETWEEN', 'LIKE', 'TRUE', 'FALSE', 'NULL'}
_NUMERIC_OPS = {'<', '>', '<=', '>='}
_FIELD_TYPES = {'string', 'number', 'boolean'}


class FilterGuardError(ValueError):
    """Invalid filter input or unmet precondition; never a SUT defect."""

    def __init__(self, layer, reason, detail=None):
        if layer not in LAYERS:
            raise ValueError('unknown filter layer: ' + str(layer))
        super().__init__(reason)
        self.layer = layer
        self.reason = reason
        self.detail = detail


def _now():
    return time.time_ns(), time.monotonic_ns()


def receipt(kind, **fields):
    """Base receipt.  Filter receipts are never execution/mutation evidence."""
    wall, mono = _now()
    value = {'schema': SCHEMA, 'module': MODULE, 'receipt_kind': kind,
             'time_ns': wall, 'monotonic_ns': mono,
             'counts_toward_maturity': False, 'candidate_eligible': False,
             'mutation_receipt': False}
    value.update(fields)
    return value


def guard_receipt(reason, layer='submission_api', stage='guard', **fields):
    value = receipt('filter_guard', stage=stage, reason=reason, layer=layer,
                    ok=False, accepted=False, submitted=False, sut_bug=False)
    value.update(fields)
    return value


def _fail(layer, reason, **fields):
    value = guard_receipt(reason, layer=layer, **fields)
    # Guard receipts carry the same step fields the success path reports, so a
    # caller never has to guess whether a rejected step "applied".
    value.setdefault('applied', False)
    value.setdefault('readback_status', 'not_attempted')
    return value


class _Tokens:
    def __init__(self, text):
        self.items = []
        self.error = None
        pos = 0
        while pos < len(text):
            match = _TOKEN.match(text, pos)
            if match is None:
                if text[pos:].strip():
                    self.error = ('expression_syntax',
                                  'unparsed_input_at_%d' % pos)
                    return
                break
            pos = match.end()
            kind = match.lastgroup
            value = match.group(kind)
            if kind == 'ident' and value.upper() in _KEYWORDS:
                kind, value = 'kw', value.upper()
            self.items.append((kind, value))
        self.index = 0

    def peek(self, offset=0):
        index = self.index + offset
        return self.items[index] if index < len(self.items) else (None, None)

    def next(self):
        item = self.peek()
        self.index += 1
        return item

    def accept(self, kind, value=None):
        item = self.peek()
        if item[0] == kind and (value is None or item[1] == value):
            self.index += 1
            return item
        return None


class _Parser:
    """Recursive-descent validation for the conservative harness subset."""

    def __init__(self, text, field_types):
        self.tokens = _Tokens(text)
        self.field_types = field_types or {}
        self.fields = []
        self.params = []

    def parse(self):
        if self.tokens.error:
            raise FilterGuardError(*self.tokens.error)
        if not self.tokens.items:
            raise FilterGuardError('expression_syntax', 'empty_expression')
        self._or()
        if self.tokens.peek()[0] is not None:
            raise FilterGuardError('expression_syntax',
                                   'trailing_token:' + str(self.tokens.peek()[1]))
        return {'fields': sorted(set(self.fields)), 'parameters': self.params}

    def _or(self):
        self._and()
        while self.tokens.accept('kw', 'OR'):
            self._and()

    def _and(self):
        self._not()
        while self.tokens.accept('kw', 'AND'):
            self._not()

    def _not(self):
        if self.tokens.accept('kw', 'NOT'):
            return self._not()
        if self.tokens.accept('lparen'):
            self._or()
            if not self.tokens.accept('rparen'):
                raise FilterGuardError('expression_syntax', 'missing_rparen')
            return
        self._predicate()

    def _predicate(self):
        kind, value = self.tokens.next()
        if kind != 'ident':
            raise FilterGuardError('expression_syntax',
                                   'field_expected:' + str(value))
        field = value
        self.fields.append(field)
        if field not in self.field_types and self.field_types:
            raise FilterGuardError('unsupported_field', 'unknown_field:' + field)
        if self.tokens.accept('kw', 'BETWEEN'):
            self._operand()
            if not self.tokens.accept('kw', 'AND'):
                raise FilterGuardError('expression_syntax', 'between_missing_and')
            self._operand()
            return
        kind, value = self.tokens.next()
        if kind == 'kw' and value == 'LIKE':
            self._operand()
            return
        if kind != 'op':
            raise FilterGuardError('expression_syntax',
                                   'operator_expected:' + str(value))
        self._operand()
        if field in self.field_types:
            declared = self.field_types[field]
            if declared not in _FIELD_TYPES:
                raise FilterGuardError('unsupported_field',
                                       'unsupported_declared_type:' + str(declared))
            if value in _NUMERIC_OPS and declared == 'string':
                raise FilterGuardError('expression_type',
                                       'numeric_operator_on_string_field:' + field)
            if value == 'LIKE' and declared == 'number':
                raise FilterGuardError('expression_type',
                                       'like_on_numeric_field:' + field)

    def _operand(self):
        kind, value = self.tokens.next()
        if kind == 'param':
            index = int(value[1:])
            self.params.append(index)
            return
        if kind in ('number', 'string'):
            return
        if kind == 'kw' and value in ('TRUE', 'FALSE', 'NULL'):
            return
        if kind == 'ident':
            raise FilterGuardError('unsupported_feature',
                                   'field_to_field_comparison')
        raise FilterGuardError('expression_syntax', 'operand_expected:' + str(value))


def validate_filter(expression, parameters=None, field_types=None):
    """Validate a single filter specification without touching a DDS entity.

    Returns a dict with ``ok`` and, on rejection, the layer-specific guard
    fields.  ``field_types`` is optional; when supplied it is the caller's
    declared ROS message schema and unknown fields are rejected rather than
    guessed.
    """
    parameters = [] if parameters is None else parameters
    checked = ['expression_type', 'parameter_type', 'expression_syntax',
               'parameter_index', 'unsupported_feature']
    if not isinstance(expression, str):
        result = _fail('expression_type', 'expression_must_be_string',
                       expression_type=type(expression).__name__,
                       layers_checked=checked)
        return result
    normalized = expression.strip()
    if not isinstance(parameters, (list, tuple)) or any(
            not isinstance(item, str) for item in parameters):
        return _fail('parameter_type', 'parameters_must_be_list_of_str',
                     parameter_types=[type(item).__name__ for item in list(parameters or [])],
                     layers_checked=checked)
    if not normalized:
        if parameters:
            return _fail('parameter_unexpected',
                         'parameters_with_empty_expression',
                         parameter_count=len(parameters),
                         layers_checked=checked)
        return receipt('filter_validation', ok=True, enabled=False,
                       expression='', parameters=[],
                       disable_semantics='empty_expression_disables_filter',
                       filter_false_implies_unmatch=False,
                       conformance_claim=False,
                       validation_scope='harness_subset',
                       layers_checked=checked)
    if '[' in normalized or ']' in normalized:
        return _fail('unsupported_feature', 'index_or_collection_syntax',
                     layers_checked=checked)
    try:
        parsed = _Parser(normalized, field_types).parse()
    except FilterGuardError as exc:
        value = _fail(exc.layer, exc.reason, detail=exc.detail,
                      conformance_claim=False,
                      validation_scope='harness_subset',
                      layers_checked=checked)
        return value
    maximum = len(parameters) - 1
    bad = sorted({index for index in parsed['parameters']
                  if index > maximum or index < 0})
    if bad:
        return _fail('parameter_index', 'parameter_index_out_of_range',
                     indices=bad, parameter_count=len(parameters),
                     conformance_claim=False,
                     validation_scope='harness_subset',
                     layers_checked=checked)
    used = sorted(set(parsed['parameters']))
    return receipt('filter_validation', ok=True, enabled=True,
                   expression=normalized, parameters=list(parameters),
                   fields=parsed['fields'], parameter_indices=used,
                   unused_parameters=[i for i in range(len(parameters)) if i not in used],
                   filter_false_implies_unmatch=False,
                   conformance_claim=False,
                   validation_scope='harness_subset',
                   layers_checked=checked + ['unsupported_field'])


def content_filter_options(expression, parameters=None, options_factory=None):
    """Build rclpy ``ContentFilterOptions`` for a fresh subscription.

    Returns ``None`` when the expression is empty (create without CFT kwargs).
    Raises ``FilterGuardError`` on invalid input; the caller must record the
    guard receipt instead of creating the subscription.
    """
    validation = validate_filter(expression, parameters)
    if not validation.get('ok'):
        raise FilterGuardError(validation['layer'], validation['reason'],
                              validation.get('detail'))
    if not validation['enabled']:
        return None
    if options_factory is None:
        from rclpy.subscription_content_filter_options import ContentFilterOptions
        options_factory = ContentFilterOptions
    # Validation may strip surrounding whitespace to parse the harness subset;
    # the submitted nonempty expression must still be the exact caller input.
    return options_factory(expression, list(validation['parameters']))


def normalize_options(value):
    """Normalize a readback object (rclpy NamedTuple or dict) for comparison."""
    if value is None:
        return None
    if isinstance(value, dict):
        expression = value.get('filter_expression')
        parameters = value.get('expression_parameters')
    else:
        expression = getattr(value, 'filter_expression', None)
        parameters = getattr(value, 'expression_parameters', None)
    if parameters is None:
        parameters = []
    return {'expression': expression, 'parameters': list(parameters)}



def filter_api_failure(exc):
    """Separate public API failures from actual RCL/RMW call exceptions.

    rclpy's exception lives in the extension module; avoiding its import keeps
    this helper offline-safe, and MRO matching also handles RCLError subclasses.
    The error text/type remain raw evidence, not proof of unsupported RMW CFT.
    """
    rcl_error = any(cls.__name__ == 'RCLError' for cls in type(exc).__mro__)
    return {'layer': 'submission_rcl' if rcl_error else 'submission_api',
            'error_type': type(exc).__name__, 'error': str(exc)}


class ContentFilterAdapter:
    """Thin controller around an existing public subscription.

    The unified worker owns creation/execution.  This class only validates
    input, calls the public API, records readback, and keeps filter/sample
    observations separate from match/graph observations.
    """

    def __init__(self, emit=None, clock=None):
        self.emit = emit
        self.clock = clock or time.monotonic_ns
        self.subscription = None
        self.created_with = None
        self.history = []

    def _record(self, value):
        self.history.append(value)
        if self.emit is not None:
            self.emit(value)
        return value

    def attach(self, subscription, created_with=None):
        self.subscription = subscription
        self.created_with = created_with
        return receipt('filter_attach',
                       subscription_type=type(subscription).__name__,
                       created_with=created_with,
                       api='rclpy.Subscription')

    def create(self, subscription, expression, parameters=None, *, topic=None,
               field_types=None, options_factory=None):
        """Verify a subscription that was created with filter options.

        ``subscription`` must already exist; creation itself is performed by
        the worker's ``create_subscription(..., content_filter_options=...)``.
        """
        validation = validate_filter(expression, parameters, field_types)
        if not validation.get('ok'):
            return self._record(validation)
        options = content_filter_options(expression, parameters, options_factory)
        attach = self.attach(subscription,
                             created_with={'expression': validation['expression'],
                                           'parameters': validation['parameters'],
                                           'enabled': validation['enabled']})
        value = receipt('filter_create', stage='carrier_admitted',
                        api='rclpy.node.create_subscription(content_filter_options=...)',
                        requested={'expression': expression,
                                   'parameters': list(parameters or []),
                                   'enabled': validation['enabled']},
                        options_built=options is not None,
                        topic=topic,
                        validation_sha=validation.get('expression'),
                        attach=attach)
        return self._record(value)

    def readback(self, *, stage='configured'):
        if self.subscription is None:
            return self._record(_fail('submission_api', 'no_attached_subscription',
                                      stage=stage))
        try:
            observed = self.subscription.get_content_filter()
        except Exception as exc:  # rclpy raises RCLError/invalid handle
            return self._record(_fail('submission_rcl', 'readback_failed',
                                      stage=stage,
                                      error=type(exc).__name__ + ':' + str(exc)))
        observed = normalize_options(observed)
        enabled = None
        try:
            enabled = bool(self.subscription.is_cft_enabled)
        except Exception:
            enabled = None
        value = receipt('filter_readback', stage=stage,
                        api='rclpy.Subscription.get_content_filter',
                        observed=observed, is_cft_enabled=enabled)
        return self._record(value)

    def initial_readback(self, *, fixture=False):
        """Observe both initial-configuration APIs independently, never inputs.

        Unlike a sample oracle, this only compares the returned configuration.
        Missing/failed/malformed results remain unknown; a disabled CFT is not
        evidence of an unmatched reader. The dynamic-update path is unchanged.
        """
        requested = self.created_with
        if self.subscription is None:
            return self._record(receipt(
                'filter_initial_configuration', stage='initial_readback', scope='initial_only',
                fixture=fixture, execution_evidence=False, requested=requested,
                submitted=False, accepted=False, observed=None, is_cft_enabled=None,
                api_results={}, configured=None, configuration_status='unverified',
                readback_status='unavailable', reason='no_attached_subscription',
                filter_effect_observed=None, filter_false_implies_unmatch=False,
                dynamic_update_covered=False, lease_covered=False))
        observed = None
        enabled = None
        results = {}
        get_started = time.monotonic_ns()
        try:
            getter = getattr(self.subscription, 'get_content_filter', None)
            if not callable(getter):
                results['get_content_filter'] = {'status': 'unavailable_api'}
            else:
                raw = getter()
                if isinstance(raw, dict):
                    expression = raw.get('filter_expression')
                    parameters = raw.get('expression_parameters')
                else:
                    expression = getattr(raw, 'filter_expression', None)
                    parameters = getattr(raw, 'expression_parameters', None)
                valid_expression = isinstance(expression, str)
                valid_parameters = isinstance(parameters, (list, tuple)) and all(
                    isinstance(item, str) for item in parameters)
                observed = {'expression': expression if valid_expression else None,
                            'parameters': list(parameters) if valid_parameters else None}
                results['get_content_filter'] = {
                    'status': 'observed' if valid_expression and valid_parameters else 'invalid_result',
                    'result_type': type(raw).__name__,
                    'expression_type': type(expression).__name__,
                    'parameters_type': type(parameters).__name__}
                # Keep malformed-but-JSON results as raw too. Never replace a
                # missing/null parameter result with the requested list or [].
                returned = {'filter_expression': expression, 'expression_parameters': parameters}
                try:
                    json.dumps(returned, allow_nan=False)
                except (TypeError, ValueError):
                    results['get_content_filter']['raw_result_status'] = 'not_json_serializable'
                else:
                    results['get_content_filter']['raw_result'] = returned
        except Exception as exc:
            results['get_content_filter'] = {'status': 'failed', **filter_api_failure(exc)}
        results['get_content_filter'].update(
            started_monotonic_ns=get_started, completed_monotonic_ns=time.monotonic_ns())
        # Do not skip the enabled flag because get_content_filter is absent or
        # failed. AttributeError from an existing descriptor is a call failure,
        # not evidence that every installed rclpy lacks the public API.
        import inspect
        missing = object()
        enabled_started = time.monotonic_ns()
        try:
            descriptor = inspect.getattr_static(self.subscription, 'is_cft_enabled', missing)
            if descriptor is missing:
                results['is_cft_enabled'] = {'status': 'unavailable_api'}
            else:
                raw_enabled = self.subscription.is_cft_enabled
                if type(raw_enabled) is bool:
                    enabled = raw_enabled
                results['is_cft_enabled'] = {
                    'status': 'observed' if type(raw_enabled) is bool else 'invalid_result',
                    'result_type': type(raw_enabled).__name__}
                try:
                    json.dumps(raw_enabled, allow_nan=False)
                except (TypeError, ValueError):
                    results['is_cft_enabled']['raw_result_status'] = 'not_json_serializable'
                else:
                    results['is_cft_enabled']['raw_result'] = raw_enabled
        except Exception as exc:
            results['is_cft_enabled'] = {'status': 'failed', **filter_api_failure(exc)}
        results['is_cft_enabled'].update(
            started_monotonic_ns=enabled_started, completed_monotonic_ns=time.monotonic_ns())
        complete = all(results[api]['status'] == 'observed' for api in
                       ('get_content_filter', 'is_cft_enabled'))
        configured = None
        if complete and requested is not None:
            configured = (observed['expression'] == requested['expression']
                          and observed['parameters'] == requested['parameters']
                          and enabled == requested['enabled'])
        status = ('verified' if configured is True else
                  'mismatch' if configured is False else 'unverified')
        value = receipt(
            'filter_initial_configuration', stage='initial_readback', scope='initial_only',
            fixture=fixture, execution_evidence=False, requested=requested,
            submitted=True, accepted=True, observed=observed, is_cft_enabled=enabled,
            api_results=results, configured=configured, configuration_status=status,
            readback_status=('matched' if configured is True else
                             'mismatched' if configured is False else 'unavailable'),
            reason=('readback_mismatch' if configured is False else
                    'initial_readback_incomplete' if configured is None else None),
            filter_effect_observed=None, filter_false_implies_unmatch=False,
            dynamic_update_covered=False, lease_covered=False)
        return self._record(value)

    def update(self, expression, parameters=None, *, field_types=None):
        """Apply an expression change and verify it by readback."""
        if self.subscription is None:
            return self._record(_fail('submission_api', 'no_attached_subscription',
                                      stage='submitted'))
        validation = validate_filter(expression, parameters, field_types)
        if not validation.get('ok'):
            return self._record(validation)
        target = {'expression': validation['expression'],
                  'parameters': validation['parameters'],
                  'enabled': validation['enabled']}
        try:
            if validation['enabled']:
                self.subscription.set_content_filter(
                    validation['expression'], list(validation['parameters']))
            else:
                # Public rcl/RMW contract: empty expression resets the CFT.
                self.subscription.set_content_filter('', [])
        except Exception as exc:
            layer = 'submission_rcl' if type(exc).__name__ == 'RCLError' else 'submission_api'
            return self._record(_fail(layer, 'set_content_filter_failed',
                                      requested=target,
                                      error=type(exc).__name__ + ':' + str(exc)))
        observed = self.readback(stage='configured')
        if not observed.get('observed'):
            result = dict(observed)
            result['receipt_kind'] = 'filter_update'
            result['requested'] = target
            result['applied'] = False
            result['readback_status'] = 'unavailable'
            return self._record(result)
        got = observed['observed']
        same = (got['expression'] == target['expression']
                and list(got['parameters']) == list(target['parameters']))
        value = receipt('filter_update', stage='applied' if same else 'configured',
                        requested=target, observed=got,
                        applied=same, readback_status='matched' if same else 'mismatched',
                        mutation_receipt=False, hit=False)
        if not same:
            value.update(reason='readback_mismatch', layer='readback_mismatch',
                         sut_bug=False, candidate_eligible=False)
        return self._record(value)

    def disable(self):
        return self.update('', [])


def summarize_observations(sent, received, match_before=None, match_after=None,
                           *, expected=None):
    """Separate sample filtering from matching-graph evidence.

    ``sent`` is a list of dicts with at least ``sequence``; ``expected`` is an
    optional mapping sequence -> bool telling whether the filter should admit
    it.  Without ``expected`` only the delivered/suppressed split is reported
    and no ``filter_effect_observed`` claim is made.
    """
    sent_sequences = [item['sequence'] for item in sent]
    delivered = [seq for seq in sent_sequences if seq in set(received)]
    suppressed = [seq for seq in sent_sequences if seq not in set(received)]
    result = receipt('filter_observation', sent_sequences=sent_sequences,
                     delivered_sequences=delivered,
                     suppressed_sequences=suppressed,
                     received_order=list(received),
                     observation_window='finite',
                     cycle_complete=True,
                     filter_false_implies_unmatch=False,
                     match_graph_channel={
                         'before': match_before, 'after': match_after,
                         'inference': 'match_graph_only'},
                     sample_channel={
                         'basis': 'delivered_vs_sent_samples',
                         'inference': 'sample_filtering_only'},
                     cross_channel_inference_forbidden=True)
    if expected is not None:
        wrong_admit = [seq for seq in sent_sequences
                       if expected.get(seq) is True and seq in suppressed]
        wrong_drop = [seq for seq in sent_sequences
                      if expected.get(seq) is False and seq in delivered]
        result['expected_admitted'] = sorted(seq for seq, ok in expected.items() if ok)
        result['unexpected_suppressed'] = wrong_admit
        result['unexpected_delivered'] = wrong_drop
        result['filter_effect_observed'] = bool(
            sent_sequences and not wrong_admit and not wrong_drop)
        result['oracle_scope'] = 'sample_admission_only'
        result['oracle_note'] = ('does not prove matching graph unchanged; '
                                 'does not turn filter=false into unmatch')
    else:
        result['filter_effect_observed'] = None
        result['oracle_note'] = 'expected admission map not supplied; observation only'
    return result


def load_receipts(path):
    """Read JSONL receipts written by the existing raw writer."""
    values = []
    with open(path, 'r', encoding='utf-8') as handle:
        for line in handle:
            line = line.strip()
            if line:
                values.append(json.loads(line))
    return values


def summarize_history(history):
    return receipt('filter_history_summary', steps=len(history),
                   guard_steps=[item.get('reason') for item in history
                                if item.get('receipt_kind') == 'filter_guard'],
                   mutations=0, candidates=0)
