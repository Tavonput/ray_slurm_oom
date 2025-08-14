import os

os.environ["RAY_record_ref_creation_sites"] = "1"  # More info for 'ray memory'
os.environ["RAY_DEDUP_LOGS"] = "0"  # More logs

import time
from typing import List, Optional
import argparse
from pathlib import Path

import ray
import memray
import psutil
from psutil._pslinux import pfullmem

from flag import RemoteFlag

MB = 1000 ** 2
GB = 1000 ** 3


def wait(msg: Optional[str] = None) -> None:
    """Just wait."""
    if msg is not None:
        print(msg)

    while True:
        pass


def init_ray(tmp_path: Path, obs_size: Optional[int] = None) -> None:
    """Initialize ray with a given tmp path."""
    if ray.is_initialized():
        ray.shutdown()

    ray.init(
        _temp_dir=str(tmp_path),
        include_dashboard=False,
        object_store_memory=obs_size,
    )


def get_mem_info() -> pfullmem:
    return psutil.Process().memory_full_info()


def log_memory_usage(process_name: str, mem_info: pfullmem):
    print(
        f"Process: {process_name}\n"
        f"\tRSS: {mem_info.rss / MB:.2f} MB\n"
        f"\tUSS: {mem_info.uss / MB:.2f} MB\n"
        f"\tPSS: {mem_info.pss / MB:.2f} MB\n"
        f"\tSHR: {mem_info.shared / MB:.2f} MB\n"
    )


def check_peak_mem_info(mi_a: pfullmem, mi_b: pfullmem) -> pfullmem:
    """Return peak memory info between two sets of memory info."""
    get_greater = lambda x, y: x if x > y else y
    return pfullmem(
        rss=get_greater(mi_a.rss, mi_b.rss),
        vms=get_greater(mi_a.vms, mi_b.vms),
        shared=get_greater(mi_a.shared, mi_b.shared),
        text=get_greater(mi_a.text, mi_b.text),
        lib=get_greater(mi_a.lib, mi_b.lib),
        data=get_greater(mi_a.data, mi_b.data),
        dirty=get_greater(mi_a.dirty, mi_b.dirty),
        uss=get_greater(mi_a.uss, mi_b.uss),
        pss=get_greater(mi_a.pss, mi_b.pss),
        swap=get_greater(mi_a.swap, mi_b.swap),
    )


@ray.remote(num_cpus=1)
class Server:
    def __init__(self, stop_flag: RemoteFlag, memray_path: Path, array_size: int) -> None:
        memray.Tracker(memray_path / f"{self.__class__.__name__}_mem_profile.bin").__enter__()

        self.clients: List[ray.ObjectRef] = []
        self.array_size = array_size
        self.peak_mem_info: pfullmem = get_mem_info()

        self.stop_flag = stop_flag

        self.n_rounds = 0
        self.rounds_completed: List[int] = []

        self.verbose = False


    def add_clients(self, clients: List[ray.ObjectRef]) -> None:
        self.clients += clients


    def get_peak_mem(self) -> pfullmem:
        return self.peak_mem_info


    def run(self, n_rounds: int) -> None:
        self.n_rounds = n_rounds
        self.rounds_completed = [0] * len(self.clients)
        for i in range(len(self.clients)):
            self.send_array(i, verbose=self.verbose)


    def send_array(self, c_id: int, n_times: int = 1, verbose: bool = True, block: bool = False) -> None:
        # Send array to client
        for _ in range(n_times):
            large_array = bytearray(self.array_size)
            self.clients[c_id].receive_array.remote(large_array, verbose=verbose)  # type: ignore

        # Monitor peak memory usage
        mem = get_mem_info()
        self.peak_mem_info = check_peak_mem_info(self.peak_mem_info, mem)

        if verbose:
            log_memory_usage("Server after send", mem)

        if block:
            wait("Server is waiting")


    def receive_array(self, array: bytearray, c_id: int) -> None:
        # Do something
        # time.sleep(1)

        self.rounds_completed[c_id] += 1
        if self.rounds_completed[c_id] >= self.n_rounds:
            self.check_stop_condition()
        else:
            self.send_array(c_id, verbose=self.verbose)


    def check_stop_condition(self) -> None:
        should_stop = True
        for c_rounds in self.rounds_completed:
            if c_rounds < self.n_rounds:
                should_stop = False

        if should_stop:
            self.stop_flag.set()


@ray.remote(num_cpus=1)
class Client:
    def __init__(self, id: int, memray_path: Path, array_size: int) -> None:
        if id == 0:
            memray.Tracker(memray_path / f"{self.__class__.__name__}_{id}_mem_profile.bin").__enter__()

        self.id = id
        self.server: ray.ObjectRef = None  # type: ignore

        self.peak_mem_info: pfullmem = get_mem_info()
        self.array_size = array_size


    def add_server(self, server: ray.ObjectRef) -> None:
        self.server = server


    def get_peak_mem(self) -> pfullmem:
        return self.peak_mem_info


    def receive_array(self, array: bytearray, verbose: bool = True, block: bool = False) -> None:
        # Do something with the array
        time.sleep(1)
        new_array = bytearray(self.array_size)
        self.send_array_to_server(new_array)

        # Monitor peak memory usage
        mem = get_mem_info()
        self.peak_mem_info = check_peak_mem_info(self.peak_mem_info, mem)

        if verbose:
            log_memory_usage(f"Client {self.id} after recieve", mem)

        if block:
            wait(f"Client {self.id} is waiting")


    def send_array_to_server(self, array: bytearray) -> None:
        self.server.receive_array.remote(array, self.id)  # type: ignore


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray_tmp",
        default=Path.cwd() / "./tmp/ray",
        help="Tmp path for ray. default=./tmp/ray",
    )
    parser.add_argument(
        "--mray_dir",
        default="./memray",
        help="Path to store memray results. default=./memray",
    )
    parser.add_argument(
        "--array_size",
        default=100 * MB,
        type=int,
        help="Size of transfer array. default=100MB",
    )
    parser.add_argument(
        "--obj_store_mem",
        default=10 * GB,
        type=int,
        help="Size of ray object store. Ray recommends 30% of total memory. default=10GB",
    )
    parser.add_argument(
        "--timeout",
        default=1000,
        type=int,
        help="How long the main process will wait for the server to finish in seconds. default=1000",
    )
    parser.add_argument(
        "--poll_int",
        default=1,
        type=int,
        help="Polling interval for the main process to check on the server status in seconds. default=1",
    )
    parser.add_argument(
        "--clients",
        default=10,
        type=int,
        help="Number of clients. default=10",
    )
    parser.add_argument(
        "--rounds",
        default=20,
        type=int,
        help="Number of rounds. default=20",
    )

    args = parser.parse_args()
    assert Path(args.ray_tmp).is_absolute(), f"Ray tmp directory {args.ray_tmp} must be an absolute path"

    Path(args.ray_tmp).mkdir(parents=True, exist_ok=True)
    Path(args.mray_dir).mkdir(parents=True, exist_ok=True)

    assert len(os.listdir(args.mray_dir)) == 0, f"Memray directory {args.mray_dir} must be empty"

    return args


def main() -> None:
    args = parse_args()

    memray.Tracker(Path(args.mray_dir) / "main_mem_profile.bin").__enter__()

    init_ray(Path(args.ray_tmp), args.obj_store_mem)

    # Create actors
    stop_flag_handle = RemoteFlag()
    server_handle = Server.remote(stop_flag_handle, Path(args.mray_dir), args.array_size)
    client_handles = [Client.remote(i, Path(args.mray_dir), args.array_size) for i in range(args.clients)]

    # Print actors hex handles for debugging in 'ray memory'
    print("Ray actor handles")
    print(f"\t{server_handle}")
    [print(f"\t{client_handle}") for client_handle in client_handles]

    # Give the actors handles to each other
    ray.get(server_handle.add_clients.remote(client_handles))  # type: ignore
    for client_handle in client_handles:
        ray.get(client_handle.add_server.remote(server_handle))  # type: ignore

    # Send array from server to client
    print(f"Running {args.rounds} rounds...")
    server_handle.run.remote(args.rounds)  # type: ignore
    stop_flag_handle.poll(args.timeout, args.poll_int)

    mem_infos: List[pfullmem] = []

    # Print memory info for server
    server_mem: pfullmem = ray.get(server_handle.get_peak_mem.remote())  # type: ignore
    mem_infos.append(server_mem)
    log_memory_usage("Server peak usage", server_mem)

    # Print memory info for clients
    for i, client in enumerate(client_handles):
        client_mem: pfullmem = ray.get(client.get_peak_mem.remote())  # type: ignore
        mem_infos.append(client_mem)
        log_memory_usage(f"Client {i} peak usage", client_mem)

    # Print memory info for main process
    main_mem = get_mem_info()
    mem_infos.append(main_mem)
    log_memory_usage("Main", main_mem)

    # Print total peak memory usage
    total_rss = sum([mi.rss for mi in mem_infos])
    total_uss = sum([mi.uss for mi in mem_infos])
    total_pss = sum([mi.pss for mi in mem_infos])
    total_shr = sum([mi.shared for mi in mem_infos])
    print(
        f"Total memory report after all tasks completed\n"
        f"\tRSS: {total_rss / MB:.2f} MB\n"
        f"\tUSS: {total_uss / MB:.2f} MB\n"
        f"\tPSS: {total_pss / MB:.2f} MB\n"
        f"\tSHR: {total_shr / MB:.2f} MB\n"
    )

    ray.shutdown()


if __name__ == "__main__":
    main()
