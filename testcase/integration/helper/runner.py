import asyncio
import json
import logging
import subprocess
import signal
from typing import Optional, List

import aiohttp
import psutil

from .config import FilePath


class LoopchainRunner:
    def __init__(self):
        self._proc_list: List[subprocess.Popen] = []
        self._rest_ports = []

    @property
    def proc_list(self) -> List[subprocess.Popen]:
        return self._proc_list

    async def run(self, config: FilePath, peer_count=4) -> bool:
        for idx, peer_config in enumerate(config.peer_configs, start=1):
            cmd = ["loop", "-d", "-o", peer_config]
            print("Run loopchain cmd: ", cmd)

            rest_port = self._get_rest_port(peer_config)
            self._rest_ports.append(rest_port)

            proc = subprocess.Popen(cmd)
            self._proc_list.append(proc)

            if idx == peer_count:
                break

        return await ensure_run(self._rest_ports)

    def kill(self, order: Optional[int] = None):
        logging.debug(f"Kill accepted: {order}")
        if order is None:
            print("ALL!")
            for idx in range(len(self._proc_list)):
                self._kill_single_node(idx)
        else:
            print("SINGLE!")
            self._kill_single_node(order)

    def _kill_single_node(self, order: int):
        proc = self._proc_list[order]
        proc.send_signal(signal.SIGTERM)
        proc.wait()

        self._kill_channel_and_its_subprocs(order)

    def _kill_channel_and_its_subprocs(self, order: Optional[int] = None):
        logging.debug(f"Kill channel accepted: {order}")
        python_procs = (proc for proc in psutil.process_iter(["name"])
                        if "python" in proc.info["name"])

        for proc in python_procs:
            try:
                proc_info = proc.cmdline()[0]
            except psutil.AccessDenied:
                continue

            if "loopchain channel" in proc_info and f"test_{order}_conf.json" in proc_info:
                logging.debug(f"Kill channel this!: {proc_info}")
                try:
                    proc.send_signal(signal.SIGINT)
                except RuntimeError:
                    pass
                finally:
                    logging.debug(f"Kill channel this - END!: {proc_info}")

    def _get_rest_port(self, peer_config: str) -> int:
        with open(peer_config) as f:
            config: dict = json.load(f)

        port = config["PORT_PEER"]

        return port + 1900


async def ensure_run(ports, max_retry=60):
    tasks = (ensure_node_is_available(port, max_retry=max_retry)
             for node_num, port in enumerate(ports))
    res = await asyncio.gather(*tasks)

    if all(res):
        print(f"==========ALL GREEN ==========")
        return True
    else:
        return False


async def ensure_node_is_available(port: int, max_retry: int, channel_name: str = "icon_dex", timeout=0.1):
    endpoint = f"http://localhost:{port}/api/v1/avail/peer"
    count = 0

    while count < max_retry:
        print(f"Try {port}: {count}")
        if await get_status(endpoint, channel_name=channel_name, timeout=timeout):
            return True
        else:
            await asyncio.sleep(1)
            count += 1

    return False


async def get_status(endpoint: str, channel_name: str, timeout):
    is_available = False

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(endpoint, params={"channel": channel_name}, timeout=timeout) as response:
                res = await response.json()
                print("GETSTATUS: ", res)
                if res.get("service_available"):
                    print("Connected: ", endpoint)
                    is_available = True
        except Exception:
            is_available = False

    return is_available
