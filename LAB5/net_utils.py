import base64, json, socket, time
from typing import Any, Dict, Generator

def to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode()

def from_b64(s: str) -> bytes:
    return base64.b64decode(s)

def write_msg(sock: socket.socket, obj: Dict[str, Any]) -> None:
    raw = (json.dumps(obj) + "\n").encode()
    sock.sendall(raw)

def read_msgs(sock: socket.socket) -> Generator[Dict, None, None]:
    reader = sock.makefile("r", encoding="utf-8")
    for line in reader:
        line = line.strip()
        if line:
            yield json.loads(line)

class TCPServer:
    def __init__(self, addr: tuple):
        self.addr = addr
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(addr)
        self._sock.listen(1)
        print(f"[TCPServer] Đang lắng nghe {addr}")

    def wait_for_client(self):
        conn, who = self._sock.accept()
        print(f"[TCPServer] Có kết nối từ {who}")
        return conn

    def close(self):
        self._sock.close()

class TCPClient:
    def __init__(self, addr: tuple, retry: int = 12, gap: float = 1.0):
        self.addr = addr
        self.conn = self._connect(retry, gap)

    def _connect(self, retry, gap) -> socket.socket:
        for i in range(1, retry + 1):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.addr)
                print(f"[TCPClient] Kết nối {self.addr}")
                return s
            except ConnectionRefusedError:
                print(f"[TCPClient] Thử {i}/{retry} …")
                time.sleep(gap)
        raise RuntimeError(f"Hết retry, không kết nối được {self.addr}")

    def send(self, obj: Dict[str, Any]) -> None:
        write_msg(self.conn, obj)

    def recv_all(self) -> Generator[Dict, None, None]:
        yield from read_msgs(self.conn)

    def close(self):
        self.conn.close()
