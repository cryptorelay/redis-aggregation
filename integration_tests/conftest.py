import os
import socket
import subprocess
import time

import pytest


# https://gist.github.com/bertjwregeer/0be94ced48383a42e70c3d9fff1f4ad0
@pytest.fixture(scope="session")
def find_free_port():
    """
    Returns a factory that finds the next free port that is available on the OS
    This is a bit of a hack, it does this by creating a new socket, and calling
    bind with the 0 port. The operating system will assign a brand new port,
    which we can find out using getsockname(). Once we have the new port
    information we close the socket thereby returning it to the free pool.
    This means it is technically possible for this function to return the same
    port twice (for example if run in very quick succession), however operating
    systems return a random port number in the default range (1024 - 65535),
    and it is highly unlikely for two processes to get the same port number.
    In other words, it is possible to flake, but incredibly unlikely.
    """

    def _find_free_port():
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", 0))
        portnum = s.getsockname()[1]
        s.close()

        return portnum

    return _find_free_port


def wait_for_port(port, host="127.0.0.1", timeout=40.0):
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(0.1)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError(
                    "Waited too long for the port {} on host {} to start accepting "
                    "connections.".format(port, host)
                ) from ex


@pytest.fixture(scope="session")
def redis_port(find_free_port, tmp_path_factory):
    port = find_free_port()
    data = tmp_path_factory.mktemp("redis")
    print("start redis at", port, data)
    with subprocess.Popen(
        [
            "redis-server",
            "--dir",
            data,
            "--port",
            str(port),
            "--loadmodule",
            os.environ["AGGREGATION_LIBRARY"],
            "--appendonly",
            "yes",
        ]
    ) as proc:
        wait_for_port(port)
        yield port
        proc.terminate()
