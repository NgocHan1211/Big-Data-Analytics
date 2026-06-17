from datetime import datetime, timezone
from typing import Dict, List
import cv2, numpy as np
import config
from net_utils import TCPServer, TCPClient, from_b64, write_msg, read_msgs
from ultralytics import YOLO


class PersonDetector:
    def __init__(self):
        self._model = YOLO("yolov8n.pt")

    def run(self, frame: np.ndarray) -> List[Dict]:
        # class 0 = person trong COCO
        results = self._model(frame, classes=[0], verbose=False, conf=0.5, iou=0.4)[0]
        out = []
        for box in results.boxes:
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            conf = float(box.conf[0])
            out.append({
                "x": int(x1), "y": int(y1),
                "w": int(x2 - x1), "h": int(y2 - y1),
                "score": round(conf, 3),
            })
        return out


def _decode_frame(pkt: Dict) -> np.ndarray:
    raw = from_b64(pkt["data"])
    arr = np.frombuffer(raw, np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def run():
    writer_cli = TCPClient(config.WRITER_ADDR)
    cam_srv    = TCPServer(config.DETECT_ADDR)
    cam_conn   = cam_srv.wait_for_client()
    detector   = PersonDetector()
    print("[Detect] ✓ YOLO model loaded")

    try:
        for pkt in read_msgs(cam_conn):
            if pkt.get("kind") == "done":
                write_msg(writer_cli.conn, {"kind": "done"})
                print("[Detect] ✓ done → writer")
                break

            frame  = _decode_frame(pkt)
            boxes  = detector.run(frame)
            result = {
                "kind":     "result",
                "cam":      pkt.get("cam"),
                "seq":      pkt.get("seq"),
                "src_ts":   pkt.get("ts"),
                "det_ts":   datetime.now(timezone.utc).isoformat(),
                "n_people": len(boxes),
                "boxes":    boxes,
                "W":        pkt.get("W"),
                "H":        pkt.get("H"),
                "data":     pkt.get("data"),
            }
            write_msg(writer_cli.conn, result)
            print(f"[Detect] seq={result['seq']} → {result['n_people']} người")
    finally:
        cam_conn.close()
        writer_cli.close()
        cam_srv.close()
