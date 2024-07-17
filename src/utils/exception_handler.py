import functools
import inspect
import logging
from typing import Any, Callable, Generator, Optional


def handle_exceptions(
    message: str, level: int = logging.ERROR, reraise: bool = False, default_return: Any = None
) -> Callable:
    def decorator(func: Callable) -> Callable:
        if inspect.isgeneratorfunction(func):

            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Generator:
                try:
                    yield from func(*args, **kwargs)
                except Exception as e:
                    logging.log(level, f"{message}: {str(e)}")
                    if reraise:
                        raise
                    yield default_return

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.log(level, f"{message}: {str(e)}")
                    if reraise:
                        raise
                    return default_return

        return wrapper

    return decorator
