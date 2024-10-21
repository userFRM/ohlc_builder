import concurrent.futures
from .config import MAX_WORKERS

def concurrent_apply(functions):
    """
    Applies a list of functions concurrently.

    :param functions: List of functions to execute concurrently
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(func) for func in functions]
        concurrent.futures.wait(futures)
