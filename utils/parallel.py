import typing
import numpy as np
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

def parallel_df(
    func: typing.Callable,
    iterable: typing.List,
    n_jobs: int = 1,
    chunksize: int = 1,
    verbose: bool = False
):
    """Parallelise Dataframe computation on multiple chunks (long format)

    Args:
        func (typing.Callable): A function to parallelise
        iterable (typing.List): A list of items to process in parallel
        n_jobs (int, optional): Number of parallel jobs. Defaults to 1.
        chunksize (int, optional): Chunk size. Defaults to 1.
        verbose (bool, optional): Verbosity. Defaults to False.

    Returns:
        np.ndarray: Processed data
    """
    results = []
    list_size = len(iterable)
    with mp.Pool(n_jobs) as pool:
        res_queue = pool.imap(func, iterable, chunksize=chunksize)
        if verbose:
            print("Generating rows...")
            for res in tqdm(res_queue, total=list_size):
                results.append(res)
        else:
            for res in res_queue:
                results.append(res)
        if results[0].ndim < 2:
            temp_data = np.concatenate(results)
        else:
            # temp_data = np.vstack(results)
            temp_data = pd.concat(results, axis=0, ignore_index=True)
        pool.close()
        pool.join()
    return temp_data