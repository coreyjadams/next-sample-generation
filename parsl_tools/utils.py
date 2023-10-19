import socket

from parsl.addresses import address_by_interface

from parsl.providers import PBSProProvider, LocalProvider
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor

def create_provider_by_hostname(user_opts):

    hostname = socket.gethostname()
    if 'polaris' in hostname:
        provider = PBSProProvider(
            account = "datascience",
            queue   = "debug",
            nodes_per_block = 1,
            cpus_per_node   = user_opts["cpus_per_node"],
            init_blocks     = 1,
            max_blocks      = 1,
            walltime        = "01:00:00",
            scheduler_options = '#PBS -l filesystems=home:grand',
            worker_init     = "source /home/cadams/Polaris/NEXT/next-sample-generation/setup_worker.sh",
        )
    else:
        return LocalProvider()

def create_executor_by_hostname(user_opts, provider):

    hostname = socket.gethostname()

    if 'polaris' in hostname:
        from parsl import HighThroughputExecutor
        return HighThroughputExecutor(
                    label="htex",
                    heartbeat_period=15,
                    heartbeat_threshold=120,
                    worker_debug=True,
                    max_workers=user_opts["cpus_per_node"],
                    cores_per_worker=1,
                    address=address_by_interface("bond0"),
                    cpu_affinity="alternating",
                    prefetch_capacity=0,
                    provider=provider
                )
    
    else:
        # default: 
        from parsl import ThreadPoolExecutor
        return ThreadPoolExecutor(
            label="threads",
            max_threads=user_opts["cpus_per_node"]
        )