import time
from datetime import datetime, timezone
import cv2, numpy as np
import config
from net_utils import TCPClient, to_b64


class CaptureNode:
    def __init__(self, source: str, max_frames: int, fps: float):
        self.source     = source
        self.max_frames = max_frames
        self.interval   = 1.0 / fps if fps > 0 else 0

    def _fake(self):
        for i in range(self.max_frames):
            img = np.full((480, 640, 3), 30, dtype=np.uint8)
            ox  = 50 + (i * 15) % 480
            cv2.ellipse(img, (ox+40, 80), (40, 50), 0, 0, 360, (210,210,210), -1)
            cv2.rectangle(img, (ox, 130), (ox+80, 360), (150,150,150), -1)
            cv2.putText(img, f"SIM {i}", (ox, 400),
                        cv2.FONT_HERSHEY_PLAIN, 1.4, (0,255,100), 2)
            yield i, img

    def _real(self):
        src = int(self.source) if self.source.isdigit() else self.source
        cap = cv2.VideoCapture(src)
        if not cap.isOpened():
            print("Không mở được nguồn → dùng frame giả")
            yield from self._fake(); return
        idx = 0
        try:
            while idx < self.max_frames:
                ret, frame = cap.read()
                if not ret: break
                yield idx, frame
                idx += 1
        finally:
            cap.release()

    def _encode(self, frame: np.ndarray) -> str:
        flags = [cv2.IMWRITE_JPEG_QUALITY, config.JPG_QUALITY]
        _, buf = cv2.imencode(".jpg", frame, flags)
        return to_b64(buf.tobytes())

    def stream_to(self, client: TCPClient) -> None:
        for idx, frame in self._real():
            pkt = {
                "kind":   "frame",
                "cam":    "A1",
                "seq":    idx,
                "ts":     datetime.now(timezone.utc).isoformat(),
                "enc":    "jpg",
                "data":   self._encode(frame),
                "W":      frame.shape[1],
                "H":      frame.shape[0],
            }
            client.send(pkt)
            print(f"[Capture] seq={idx} gửi xong")
            if self.interval:
                time.sleep(self.interval)
        client.send({"kind": "done"})
        print("[Capture] Hoàn thành")


def run(source=config.DEFAULT_SOURCE,
        max_frames=config.MAX_FRAMES,
        fps=config.SEND_FPS):
    cli  = TCPClient(config.DETECT_ADDR)
    node = CaptureNode(source, max_frames, fps)
    try:
        node.stream_to(cli)
    finally:
        cli.close()
