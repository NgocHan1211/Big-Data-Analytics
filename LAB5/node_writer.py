import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import cv2, numpy as np
import config
from net_utils import TCPServer, read_msgs, from_b64


class VideoWriter:
    def __init__(self, path: Path, W: int, H: int, fps: float = 5.0):
        path.parent.mkdir(parents=True, exist_ok=True)
        # lưu AVI trước, sau đó convert sang MP4
        self.avi_path = path.with_suffix(".avi")
        self.mp4_path = path
        fourcc   = cv2.VideoWriter_fourcc(*"XVID")
        self._vw = cv2.VideoWriter(str(self.avi_path), fourcc, fps, (W, H))
        self.fps = fps

    def write(self, frame: np.ndarray) -> None:
        self._vw.write(frame)

    def close(self) -> None:
        self._vw.release()
        # convert AVI → MP4 bằng ffmpeg (có sẵn trên Colab)
        import subprocess
        cmd = [
            "ffmpeg", "-y",
            "-i", str(self.avi_path),
            "-vcodec", "libx264",
            "-crf", "23",
            str(self.mp4_path)
        ]
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode == 0:
            self.avi_path.unlink()  # xóa file AVI tạm
            print(f"[Writer] 🎬 Video: {self.mp4_path}")
        else:
            print(f"[Writer] ⚠ ffmpeg lỗi, dùng file AVI: {self.avi_path}")
            print(result.stderr.decode())


def draw_boxes(frame: np.ndarray, boxes: List[Dict]) -> np.ndarray:
    out = frame.copy()
    for b in boxes:
        x, y, w, h = b["x"], b["y"], b["w"], b["h"]
        cv2.rectangle(out, (x, y), (x+w, y+h), (0, 255, 80), 2)
        cv2.putText(out, f"{b.get('score',0):.2f}", (x, max(y-8,12)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,255,80), 2)
    cv2.putText(out, f"People: {len(boxes)}", (12, 35),
                cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0,200,255), 2)
    return out


def decode_frame(b64: str) -> np.ndarray:
    arr = np.frombuffer(from_b64(b64), np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


class WriterNode:
    def __init__(self, root: str):
        self.root = Path(root)
        self._vw  = None

    def _jsonl_path(self, ts: str) -> Path:
        try:
            day = datetime.fromisoformat(ts).date().isoformat()
        except Exception:
            day = datetime.now(timezone.utc).date().isoformat()
        return self.root / f"date={day}" / "results.jsonl"

    def _init_video(self, record: Dict) -> None:
        day   = datetime.now(timezone.utc).date().isoformat()
        vpath = self.root / f"date={day}" / "annotated.mp4"
        self._vw = VideoWriter(vpath, record.get("W",640),
                               record.get("H",480), config.SEND_FPS)

    def handle(self, record: Dict) -> None:
        if self._vw is None:
            self._init_video(record)

        if record.get("data"):
            frame = decode_frame(record["data"])
            self._vw.write(draw_boxes(frame, record.get("boxes", [])))

        row = {
            "camera_id":      record.get("cam"),
            "frame_id":       record.get("seq"),
            "people_count":   record.get("n_people"),
            "bounding_boxes": record.get("boxes", []),
            "source_ts":      record.get("src_ts"),
            "processed_ts":   record.get("det_ts"),
            "image_width":    record.get("W"),
            "image_height":   record.get("H"),
        }
        ts    = str(row["processed_ts"] or datetime.now(timezone.utc).isoformat())
        jpath = self._jsonl_path(ts)
        jpath.parent.mkdir(parents=True, exist_ok=True)
        with jpath.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def finish(self):
        if self._vw:
            self._vw.close()


def run(root: str = config.OUTPUT_DIR):
    srv   = TCPServer(config.WRITER_ADDR)
    conn  = srv.wait_for_client()
    node  = WriterNode(root)
    saved = 0
    try:
        for record in read_msgs(conn):
            if record.get("kind") == "done":
                print("[Writer] ✓ Nhận done.")
                break
            node.handle(record)
            saved += 1
            print(f"[Writer] seq={record.get('seq')} | {record.get('n_people')} người")
    finally:
        node.finish()
        conn.close()
        srv.close()
    print(f"[Writer] Đã lưu {saved} record.")
