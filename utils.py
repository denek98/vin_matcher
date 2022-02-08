import time
from loguru import logger


def retry_with_logging(times:int = 10):
    def decorator(func):
        def newfn(self, *args, **kwargs):
            attempt = 0
            while attempt <= int(times):
                try:
                    return func(self, *args, **kwargs)
                except Exception as ex:
                    print(f'Exception "{ex}"" was thrown when attempting to run method {func.__name__}, attempt {attempt+1} of {times}')
                    attempt += 1
                    time.sleep(2)
            return func(*args, **kwargs)
        return newfn
    return decorator

def timeit(func):
    def wrapper(*args,**kwargs):
        try:
            time_started = time.time()
            return func(*args, **kwargs)
        finally:
            logger.info(f'Function executed in {time.time() - time_started} seconds')
    return wrapper