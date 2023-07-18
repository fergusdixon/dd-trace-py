import os

import rq

from ddtrace import Pin
from ddtrace import config
from ddtrace.constants import SPAN_KIND
from ddtrace.internal.constants import COMPONENT
from ddtrace.internal.schema import schematize_messaging_operation
from ddtrace.internal.schema import schematize_service_name
from ddtrace.internal.schema.span_attribute_schema import SpanDirection
from ddtrace.internal.utils.version import parse_version

from .. import trace_utils
from ...ext import SpanKind
from ...ext import SpanTypes
from ...internal.utils import get_argument_value
from ...internal.utils.formats import asbool
from ...propagation.http import HTTPPropagator


config._add(
    "rq",
    dict(
        distributed_tracing_enabled=asbool(os.environ.get("DD_RQ_DISTRIBUTED_TRACING_ENABLED", True)),
        _default_service=schematize_service_name("rq"),
    ),
)

config._add(
    "rq_worker",
    dict(
        distributed_tracing_enabled=asbool(os.environ.get("DD_RQ_DISTRIBUTED_TRACING_ENABLED", True)),
        _default_service=schematize_service_name("rq-worker"),
    ),
)

rq_version_str = getattr(rq, "__version__", "0.0.0")
rq_version = parse_version(rq_version_str)


@trace_utils.with_traced_module
def traced_queue_enqueue_job(rq, pin, func, instance, args, kwargs):
    job = get_argument_value(args, kwargs, 0, "f")

    func_name = job.func_name
    job_inst = job.instance
    job_inst_str = "%s.%s" % (job_inst.__module__, job_inst.__class__.__name__) if job_inst else ""

    if job_inst_str:
        resource = "%s.%s" % (job_inst_str, func_name)
    else:
        resource = func_name

    with pin.tracer.trace(
        schematize_messaging_operation("rq.queue.enqueue_job", provider="rq", direction=SpanDirection.OUTBOUND),
        service=trace_utils.int_service(pin, config.rq),
        resource=resource,
        span_type=SpanTypes.WORKER,
    ) as span:
        span.set_tag_str(COMPONENT, config.rq.integration_name)

        # set span.kind to the type of request being performed
        span.set_tag_str(SPAN_KIND, SpanKind.PRODUCER)

        span.set_tag_str("queue.name", instance.name)
        span.set_tag_str("job.id", job.get_id())
        span.set_tag_str("job.func_name", job.func_name)

        # If the queue is_async then add distributed tracing headers to the job
        if instance.is_async and config.rq.distributed_tracing_enabled:
            HTTPPropagator.inject(span.context, job.meta)
        return func(*args, **kwargs)


@trace_utils.with_traced_module
def traced_queue_fetch_job(rq, pin, func, instance, args, kwargs):
    with pin.tracer.trace(
        schematize_messaging_operation("rq.queue.fetch_job", provider="rq", direction=SpanDirection.PROCESSING),
        service=trace_utils.int_service(pin, config.rq),
    ) as span:
        span.set_tag_str(COMPONENT, config.rq.integration_name)

        job_id = get_argument_value(args, kwargs, 0, "job_id")
        span.set_tag_str("job.id", job_id)
        return func(*args, **kwargs)


@trace_utils.with_traced_module
def traced_perform_job(rq, pin, func, instance, args, kwargs):
    """Trace rq.Worker.perform_job"""
    # `perform_job` is executed in a freshly forked, short-lived instance
    job = get_argument_value(args, kwargs, 0, "job")

    if config.rq_worker.distributed_tracing_enabled:
        ctx = HTTPPropagator.extract(job.meta)
        if ctx.trace_id:
            pin.tracer.context_provider.activate(ctx)

    try:
        with pin.tracer.trace(
            "rq.worker.perform_job",
            service=trace_utils.int_service(pin, config.rq_worker),
            span_type=SpanTypes.WORKER,
            resource=job.func_name,
        ) as span:
            span.set_tag_str(COMPONENT, config.rq.integration_name)

            # set span.kind to the type of request being performed
            span.set_tag_str(SPAN_KIND, SpanKind.CONSUMER)
            span.set_tag_str("job.id", job.get_id())
            try:
                return func(*args, **kwargs)
            finally:
                span.set_tag_str("job.status", job.get_status())
                span.set_tag_str("job.origin", job.origin)
                if job.is_failed:
                    span.error = 1
    finally:
        # Force flush to agent since the process `os.exit()`s
        # immediately after this method returns
        pin.tracer.flush()


@trace_utils.with_traced_module
def traced_job_perform(rq, pin, func, instance, args, kwargs):
    """Trace rq.Job.perform(...)"""
    job = instance

    # Inherit the service name from whatever parent exists.
    # eg. in a worker, a perform_job parent span will exist with the worker
    #     service.
    with pin.tracer.trace("rq.job.perform", resource=job.func_name) as span:
        span.set_tag_str(COMPONENT, config.rq.integration_name)

        span.set_tag("job.id", job.get_id())
        return func(*args, **kwargs)


@trace_utils.with_traced_module
def traced_job_fetch_many(rq, pin, func, instance, args, kwargs):
    """Trace rq.Job.fetch_many(...)"""
    with pin.tracer.trace(
        schematize_messaging_operation("rq.job.fetch_many", provider="rq", direction=SpanDirection.PROCESSING),
        service=trace_utils.ext_service(pin, config.rq_worker),
    ) as span:
        span.set_tag_str(COMPONENT, config.rq.integration_name)

        job_ids = get_argument_value(args, kwargs, 0, "job_ids")
        span.set_tag("job_ids", job_ids)
        return func(*args, **kwargs)


def patch():
    if getattr(rq, "_datadog_patch", False):
        return

    Pin().onto(rq)

    # Patch rq.job.Job
    Pin().onto(rq.job.Job)
    trace_utils.wrap(rq.job, "Job.perform", traced_job_perform(rq.job.Job))

    # Patch rq.queue.Queue
    Pin().onto(rq.queue.Queue)
    if rq_version >= (1, 14, 0):
        trace_utils.wrap("rq.queue", "Queue._enqueue_job", traced_queue_enqueue_job(rq))
    else:
        trace_utils.wrap("rq.queue", "Queue.enqueue_job", traced_queue_enqueue_job(rq))
    trace_utils.wrap("rq.queue", "Queue.fetch_job", traced_queue_fetch_job(rq))

    # Patch rq.worker.Worker
    Pin().onto(rq.worker.Worker)
    trace_utils.wrap(rq.worker, "Worker.perform_job", traced_perform_job(rq))

    setattr(rq, "_datadog_patch", True)


def unpatch():
    if not getattr(rq, "_datadog_patch", False):
        return

    Pin().remove_from(rq)

    # Unpatch rq.job.Job
    Pin().remove_from(rq.job.Job)
    trace_utils.unwrap(rq.job.Job, "perform")

    # Unpatch rq.queue.Queue
    Pin().remove_from(rq.queue.Queue)
    if rq_version >= (1, 14, 0):
        trace_utils.unwrap(rq.queue.Queue, "_enqueue_job")
    else:
        trace_utils.unwrap(rq.queue.Queue, "enqueue_job")
    trace_utils.unwrap(rq.queue.Queue, "fetch_job")

    # Unpatch rq.worker.Worker
    Pin().remove_from(rq.worker.Worker)
    trace_utils.unwrap(rq.worker.Worker, "perform_job")

    setattr(rq, "_datadog_patch", False)
