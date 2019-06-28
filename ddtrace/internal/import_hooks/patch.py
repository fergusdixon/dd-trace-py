import sys

import wrapt

from ..logger import get_logger
from .registry import hooks


log = get_logger(__name__)


def exec_and_call_hooks(module_name, wrapped, args, kwargs):
    """
    Helper used to execute the wrapped function with args/kwargs and then call any
      module hooks for `module_name` after
    """
    try:
        return wrapped(*args, **kwargs)
    finally:
        # Never let this function fail to execute
        try:
            hooks.call(module_name)
        except Exception:
            log.debug('Failed to call hooks for module %r', module_name, exec_info=True)


def wrapped_reload(wrapped, instance, args, kwargs):
    """
    Wrapper for `importlib.reload` to we can trigger hooks on a module reload
    """
    module_name = None
    try:
        # Python 3 added specs, no need to even check for `__spec__` if we are in Python 2
        if sys.version_info >= (3, 4):
            try:
                module_name = args[0].__spec__.name
            except AttributeError:
                module_name = args[0].__name__
        else:
            module_name = args[0].__name__
    except Exception:
        log.debug('Failed to determine module name when calling `reload`: %r', args, exec_info=True)

    return exec_and_call_hooks(module_name, wrapped, args, kwargs)


def wrapped_find_and_load_unlocked(wrapped, instance, args, kwargs):
    """
    Wrapper for `importlib._bootstrap._find_and_load_unlocked` so we can trigger
      hooks on module loading

    NOTE: This code does not get called for module reloading
    """
    module_name = None
    try:
        module_name = args[0]
    except Exception:
        log.debug('Failed to determine module name when importing module: %r', args, exec_info=True)
        return wrapped(*args, **kwargs)

    return exec_and_call_hooks(module_name, wrapped, args, kwargs)


def wrapped_import(wrapped, instance, args, kwargs):
    """
    Wrapper for `__import__` so we can trigger hooks on module loading
    """
    module_name = None
    try:
        module_name = args[0]
    except Exception:
        log.debug('Failed to determine module name when importing module: %r', args, exec_info=True)
        return wrapped(*args, **kwargs)

    # Do not call the hooks every time `import <module>` is called,
    #   only on the first time it is loaded
    if module_name and module_name not in sys.modules:
        return exec_and_call_hooks(module_name, wrapped, args, kwargs)

    return wrapped(*args, **kwargs)


# Keep track of whether we have patched or not
_patched = False


def _patch():
    # Only patch once
    global _patched
    if _patched:
        return
    _patched = True

    # 3.4 -> 3.8
    # DEV: Explicitly stop at 3.8 in case the functions we are patching change in any way,
    #      we need to validate them before adding support here
    if (3, 4) <= sys.version_info <= (3, 8):
        # 3.4: https://github.com/python/cpython/blob/3.4/Lib/importlib/_bootstrap.py#L2207-L2231
        # 3.5: https://github.com/python/cpython/blob/3.5/Lib/importlib/_bootstrap.py#L938-L962
        # 3.6: https://github.com/python/cpython/blob/3.6/Lib/importlib/_bootstrap.py#L936-L960
        # 3.7: https://github.com/python/cpython/blob/3.7/Lib/importlib/_bootstrap.py#L948-L972
        # 3.8: https://github.com/python/cpython/blob/3.8/Lib/importlib/_bootstrap.py#L956-L980
        wrapt.wrap_function_wrapper('importlib._bootstrap', '_find_and_load_unlocked', wrapped_find_and_load_unlocked)

        # 3.4: https://github.com/python/cpython/blob/3.4/Lib/importlib/__init__.py#L115-L156
        # 3.5: https://github.com/python/cpython/blob/3.5/Lib/importlib/__init__.py#L132-L173
        # 3.6: https://github.com/python/cpython/blob/3.6/Lib/importlib/__init__.py#L132-L173
        # 3.7: https://github.com/python/cpython/blob/3.7/Lib/importlib/__init__.py#L133-L176
        # 3.8: https://github.com/python/cpython/blob/3.8/Lib/importlib/__init__.py#L133-L176
        wrapt.wrap_function_wrapper('importlib', 'reload', wrapped_reload)

    # 2.7
    # DEV: Slightly more direct approach of patching `__import__` and `reload` functions
    elif sys.version_info >= (2, 7):
        # https://github.com/python/cpython/blob/2.7/Python/bltinmodule.c#L35-L68
        __builtins__['__import__'] = wrapt.FunctionWrapper(__builtins__['__import__'], wrapped_import)

        # https://github.com/python/cpython/blob/2.7/Python/bltinmodule.c#L2147-L2160
        __builtins__['reload'] = wrapt.FunctionWrapper(__builtins__['reload'], wrapped_reload)


def patch():
    """
    Patch Python import system, enabling import hooks
    """
    # This should never cause their application to not load
    try:
        _patch()
    except Exception:
        log.debug('Failed to patch module importing', exec_info=True)


def _unpatch():
    # Only patch once
    global _patched
    if not _patched:
        return
    _patched = False

    # 3.4 -> 3.8
    # DEV: Explicitly stop at 3.8 in case the functions we are patching change in any way,
    #      we need to validate them before adding support here
    if (3, 4) <= sys.version_info <= (3, 8):
        import importlib

        if isinstance(importlib._bootstrap._find_and_load_unlocked, wrapt.FunctionWrapper):
            setattr(importlib._bootstrap, '_find_and_load_unlocked', importlib._bootstrap._find_and_load_unlocked.__wrapped__)
        if isinstance(importlib.reload, wrapt.FunctionWrapper):
            setattr(importlib, 'reload', importlib.reload.__wrapped__)

    # 2.7
    # DEV: Slightly more direct approach
    elif sys.version_info >= (2, 7):
        if isinstance(__builtins__['__import__'], wrapt.FunctionWrapper):
            __builtins__['__import__'] = __builtins__['__import__'].__wrapped__
        if isinstance(__builtins__['reload'], wrapt.FunctionWrapper):
            __builtins__['reload'] = __builtins__['reload'].__wrapped__


def unpatch():
    """
    Unpatch Python import system, disabling import hooks
    """
    # This should never cause their application to not load
    try:
        _unpatch()
    except Exception:
        log.debug('Failed to unpatch module importing', exec_info=True)
