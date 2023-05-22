import multiprocessing as mp

from dask.distributed import Client, LocalCluster


def dask_local_cluster_setup(n_workers=None, memory_limit=None, threads_per_worker=2, ip=8786):
    """Dask server setup. See https://docs.dask.org/en/stable/how-to/deploy-dask/single-distributed.html."""
    if n_workers is None:
        n_workers = int(0.5 * mp.cpu_count())

    cluster = LocalCluster(
        ip=f"tcp://127.0.0.1:{ip}",  # http://localhost:8787/status
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=True,
        memory_limit=f"{memory_limit}GB" if memory_limit else None,
    )

    return Client, cluster