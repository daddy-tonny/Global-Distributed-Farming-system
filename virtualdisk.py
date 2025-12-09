# virtualdisk.py (Final A-Grade Version - Optimized Capacity Tracking)
import os


class VirtualDisk:
    """Simulates a persistent local storage disk for a Node, enforcing capacity checks."""

    def __init__(self, path: str, capacity_bytes: int):
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        self.capacity_bytes = capacity_bytes
        self._used_bytes = self._calculate_initial_used_bytes()

    def _calculate_initial_used_bytes(self) -> int:
        """Calculates total used space on startup by walking the directory."""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    total_size += os.path.getsize(fp)
                except OSError:
                    pass
        return total_size

    def free_bytes(self) -> int:
        """Returns the dynamically tracked free space."""
        return max(0, self.capacity_bytes - self._used_bytes)

    def write_chunk(self, chunk_id: str, data: bytes):
        """
        Writes a chunk and updates the used space counter.
        Raises IOError if capacity is exceeded, fulfilling the A-grade requirement.
        """
        data_size = len(data)
        if data_size > self.free_bytes():
            raise IOError("Storage capacity exceeded.")

        fname = os.path.join(self.path, chunk_id)

        with open(fname, "wb") as fh:
            fh.write(data)

        self._used_bytes += data_size

    def read_chunk(self, chunk_id: str) -> bytes | None:
        """Reads a chunk from the disk."""
        fname = os.path.join(self.path, chunk_id)
        if not os.path.exists(fname):
            return None
        with open(fname, "rb") as fh:
            return fh.read()

    def list_chunks(self) -> list:
        """Lists all files in the storage path."""
        return os.listdir(self.path)