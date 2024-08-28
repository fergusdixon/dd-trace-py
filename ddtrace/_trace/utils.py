from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
import json

from decimal import Decimal
from ddtrace import Span
from ddtrace import config
from ddtrace.constants import ANALYTICS_SAMPLE_RATE_KEY
from ddtrace.constants import SPAN_KIND
from ddtrace.constants import SPAN_MEASURED_KEY
from ddtrace.ext import SpanKind
from ddtrace.ext import aws
from ddtrace.ext import http
from ddtrace.internal.constants import COMPONENT
from ddtrace.internal.utils.formats import deep_getattr
from ddtrace.propagation.http import HTTPPropagator

redactable_keys = ["authorization", "x-authorization", "password", "token"]
max_depth = 10

def set_botocore_patched_api_call_span_tags(span: Span, instance, args, params, endpoint_name, operation):
    span.set_tag_str(COMPONENT, config.botocore.integration_name)
    # set span.kind to the type of request being performed
    span.set_tag_str(SPAN_KIND, SpanKind.CLIENT)
    span.set_tag(SPAN_MEASURED_KEY)

    if args:
        # DEV: join is the fastest way of concatenating strings that is compatible
        # across Python versions (see
        # https://stackoverflow.com/questions/1316887/what-is-the-most-efficient-string-concatenation-method-in-python)
        span.resource = ".".join((endpoint_name, operation.lower()))
        span.set_tag("aws_service", endpoint_name)

        if params and not config.botocore["tag_no_params"]:
            aws._add_api_param_span_tags(span, endpoint_name, params)

        if params:
            expand_payload_as_tags(span, params, "aws.request")

    else:
        span.resource = endpoint_name

    region_name = deep_getattr(instance, "meta.region_name")

    span.set_tag_str("aws.agent", "botocore")
    if operation is not None:
        span.set_tag_str("aws.operation", operation)
    if region_name is not None:
        span.set_tag_str("aws.region", region_name)
        span.set_tag_str("region", region_name)

    # set analytics sample rate
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, config.botocore.get_analytics_sample_rate())


def set_botocore_response_metadata_tags(
    span: Span, result: Dict[str, Any], is_error_code_fn: Optional[Callable] = None
) -> None:
    if not result or not result.get("ResponseMetadata"):
        return
    response_meta = result["ResponseMetadata"]

    expand_payload_as_tags(span, result, "aws.response")

    if "HTTPStatusCode" in response_meta:
        status_code = response_meta["HTTPStatusCode"]
        span.set_tag(http.STATUS_CODE, status_code)

        # Mark this span as an error if requested
        if is_error_code_fn is not None and is_error_code_fn(int(status_code)):
            span.error = 1

    if "RetryAttempts" in response_meta:
        span.set_tag("retry_attempts", response_meta["RetryAttempts"])

    if "RequestId" in response_meta:
        span.set_tag_str("aws.requestid", response_meta["RequestId"])


def extract_DD_context_from_messages(messages, extract_from_message: Callable):
    ctx = None
    if len(messages) >= 1:
        message = messages[0]
        context_json = extract_from_message(message)
        if context_json is not None:
            child_of = HTTPPropagator.extract(context_json)
            if child_of.trace_id is not None:
                ctx = child_of
    return ctx

def tag_object(span, key, obj, depth=0):
    if obj is None:
        return span.set_tag(key, obj)
    if depth >= max_depth:
        return span.set_tag(key, _redact_val(key, str(obj)[0:5000]))
    depth += 1
    if _should_try_string(obj):
        parsed = None
        try:
            parsed = json.loads(obj)
            return tag_object(span, key, parsed, depth)
        except ValueError:
            redacted = _redact_val(key, obj[0:5000])
            return span.set_tag(key, redacted)
    if isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, Decimal):
        return span.set_tag(key, str(obj))
    if isinstance(obj, list):
        for k, v in enumerate(obj):
            formatted_key = f"{key}.{k}"
            tag_object(span, formatted_key, v, depth)
        return
    if hasattr(obj, "items"):
        for k, v in obj.items():
            formatted_key = f"{key}.{k}"
            tag_object(span, formatted_key, v, depth)
        return
    if hasattr(obj, "to_dict"):
        for k, v in obj.to_dict().items():
            formatted_key = f"{key}.{k}"
            tag_object(span, formatted_key, v, depth)
        return
    try:
        value_as_str = str(obj)
    except Exception:
        value_as_str = "UNKNOWN"
    return span.set_tag(key, value_as_str)

def expand_payload_as_tags(span: Span, result: Dict[str, Any], key):
    # TODO add configuration if this is enabled or not DD_TRACE_CLOUD_REQUEST_PAYLOAD_TAGGING
    # TODO add configuration if this is enabled or not DD_TRACE_CLOUD_RESPONSE_PAYLOAD_TAGGING
    #   supported values "all" OR a comma-separated list of JSONPath queries defining payload paths that will be replaced with "redacted"
    # TODO add max depth configuration DD_TRACE_CLOUD_PAYLOAD_TAGGING_MAX_DEPTH (default 10)

    if not result:
        return
    
    # handle response messages list
    if result.get("Messages"):
        message = result["Messages"]
        tag_object(span, key, message) 
        return
    
    # handle params request list
    for key2, value in result.items():
        tag_object(span, key, value)

def payload_expansion(span: Span, message, key, depth=0):
    if message is None:
        return
    if depth >= max_depth:
        return
    else:
        depth += 1
    

def _should_try_string(obj):
    try:
        if isinstance(obj, str) or isinstance(obj, unicode):
            return True
    except NameError:
        if isinstance(obj, bytes):
            return True

    return False


def _redact_val(k, v):
    split_key = k.split(".").pop() or k
    if split_key in redactable_keys:
        return "redacted"
    return v
