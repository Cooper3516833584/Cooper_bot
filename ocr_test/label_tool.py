"""
label_tool.py — OCR 黑板作业识别真值标注工具
独立脚本，不依赖项目其他模块。

依赖：opencv-python（或 opencv-python-headless）、numpy、Pillow
    pip install opencv-python numpy Pillow
    （项目环境已有 opencv-python-headless 和 numpy，只需补装 Pillow）

使用方法：
    python label_tool.py

功能：
  1. 首次运行自动将文件夹内所有图片重命名为 1.jpg, 2.jpg ...
  2. 左侧显示透视校正后的黑板图
  3. 右侧文本框填写真值，格式示例：
       P200  4.5.12
       P212  5.5.6
  4. 点击 [保存] 或按 Ctrl+S 保存当前图片标注
  5. 左右方向键 / 上下按钮切换图片
  6. 所有标注保存到同目录的 ground_truth.json
"""

import json
import os
import re
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

import cv2
import numpy as np

try:
    from PIL import Image, ImageTk
except ImportError:
    raise SystemExit(
        "需要安装 Pillow：pip install Pillow"
    )

# ──────────────────────────────────────────
# 图像工具（复刻自 blackboard_ocr.py，完全独立）
# ──────────────────────────────────────────

BOARD_GREEN_LOWER = np.array([38, 35, 25],  dtype=np.uint8)
BOARD_GREEN_UPPER = np.array([92, 255, 255], dtype=np.uint8)


def _order_points(pts: np.ndarray) -> np.ndarray:
    rect = np.zeros((4, 2), dtype="float32")
    s    = pts.sum(axis=1)
    diff = np.diff(pts, axis=1).reshape(-1)
    rect[0] = pts[np.argmin(s)]
    rect[2] = pts[np.argmax(s)]
    rect[1] = pts[np.argmin(diff)]
    rect[3] = pts[np.argmax(diff)]
    return rect


def _four_point_transform(image: np.ndarray, pts: np.ndarray) -> np.ndarray:
    rect = _order_points(pts.astype(np.float32))
    tl, tr, br, bl = rect
    max_width  = int(max(np.linalg.norm(br - bl), np.linalg.norm(tr - tl)))
    max_height = int(max(np.linalg.norm(tr - br), np.linalg.norm(tl - bl)))
    max_width  = max(max_width,  10)
    max_height = max(max_height, 10)
    dst = np.array([[0, 0], [max_width-1, 0],
                    [max_width-1, max_height-1], [0, max_height-1]], dtype="float32")
    M = cv2.getPerspectiveTransform(rect, dst)
    return cv2.warpPerspective(image, M, (max_width, max_height))


def _detect_board(image: np.ndarray) -> np.ndarray:
    """自动检测绿色黑板并透视矫正，若失败则返回原图。"""
    hsv  = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(hsv, BOARD_GREEN_LOWER, BOARD_GREEN_UPPER)
    k    = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, k, iterations=2)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN,  k, iterations=1)

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    h, w = image.shape[:2]
    img_area = h * w
    candidates = []
    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < img_area * 0.08:
            continue
        x, y, bw, bh = cv2.boundingRect(cnt)
        aspect = bw / max(bh, 1)
        candidates.append((area + 1000 * min(aspect, 3.0), cnt))

    if not candidates:
        return image

    cnt = max(candidates, key=lambda x: x[0])[1]
    peri  = cv2.arcLength(cnt, True)
    approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
    pts   = approx.reshape(-1, 2) if len(approx) == 4 else cv2.boxPoints(cv2.minAreaRect(cnt))

    warped = _four_point_transform(image, pts)
    if warped.shape[0] > warped.shape[1]:
        warped = cv2.rotate(warped, cv2.ROTATE_90_CLOCKWISE)
    hh, ww = warped.shape[:2]
    mx, my = int(ww * 0.02), int(hh * 0.02)
    return warped[my:hh-my, mx:ww-mx]


# ──────────────────────────────────────────
# 文件重命名
# ──────────────────────────────────────────

IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
RENAME_RECORD = "rename_map.json"   # 保留原名 -> 新名的映射


def rename_images(folder: Path) -> list[Path]:
    """
    将文件夹内所有图片重命名为 1.jpg, 2.jpg ...
    如果已经全部是数字命名则跳过。
    返回排序后的图片路径列表。
    """
    record_path = folder / RENAME_RECORD

    # 先收集所有图片（排除自身脚本等）
    imgs = sorted(
        [p for p in folder.iterdir()
         if p.is_file() and p.suffix.lower() in IMG_EXTS],
        key=lambda p: p.name,
    )

    if not imgs:
        return []

    # 如果已全部都是 "<数字>.jpg" 格式则直接返回
    all_numeric = all(re.fullmatch(r"\d+", p.stem) for p in imgs)
    if all_numeric:
        return sorted(imgs, key=lambda p: int(p.stem))

    # 加载已有映射（防止二次重命名）
    rename_map: dict[str, str] = {}
    if record_path.exists():
        try:
            rename_map = json.loads(record_path.read_text(encoding="utf-8"))
        except Exception:
            rename_map = {}

    # 找出最大已用编号
    used_nums = set()
    for p in imgs:
        if re.fullmatch(r"\d+", p.stem):
            used_nums.add(int(p.stem))

    def next_num(n=1):
        while n in used_nums:
            n += 1
        return n

    counter = next_num()
    for p in imgs:
        if re.fullmatch(r"\d+", p.stem):
            continue
        if p.name in rename_map:
            continue
        new_name = f"{counter}.jpg"
        new_path = folder / new_name
        p.rename(new_path)
        rename_map[new_name] = p.name   # 新名 -> 原名（以便追溯）
        used_nums.add(counter)
        counter = next_num(counter + 1)

    record_path.write_text(
        json.dumps(rename_map, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 重新扫描
    imgs = sorted(
        [p for p in folder.iterdir()
         if p.is_file() and p.suffix.lower() in IMG_EXTS],
        key=lambda p: (int(p.stem) if re.fullmatch(r"\d+", p.stem) else 99999, p.name),
    )
    return imgs


# ──────────────────────────────────────────
# 主 GUI
# ──────────────────────────────────────────

GROUND_TRUTH_FILE = "ground_truth.json"
PREVIEW_W = 720   # 左侧预览最大宽度
PREVIEW_H = 540   # 左侧预览最大高度


class LabelApp:
    def __init__(self, root: tk.Tk, folder: Path):
        self.root   = root
        self.folder = folder
        self.gt_path = folder / GROUND_TRUTH_FILE

        self.images: list[Path] = []
        self.idx: int = 0
        self.ground_truth: dict[str, str] = {}
        self._photo = None   # 防止 GC

        self._load_gt()
        self._init_ui()
        self._load_images()

    # ── 数据 ──────────────────────────────

    def _load_gt(self):
        if self.gt_path.exists():
            try:
                self.ground_truth = json.loads(
                    self.gt_path.read_text(encoding="utf-8")
                )
            except Exception:
                self.ground_truth = {}

    def _save_gt(self):
        self.gt_path.write_text(
            json.dumps(self.ground_truth, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_images(self):
        self.images = rename_images(self.folder)
        if not self.images:
            messagebox.showerror("错误", f"在 {self.folder} 中没有找到图片")
            self.root.destroy()
            return
        self._show(0)

    # ── UI ───────────────────────────────

    def _init_ui(self):
        self.root.title("黑板作业真值标注工具")
        self.root.configure(bg="#1e1e2e")
        self.root.resizable(True, True)
        self.root.bind("<Control-s>", lambda e: self._on_save())
        self.root.bind("<Left>",  lambda e: self._on_prev())
        self.root.bind("<Right>", lambda e: self._on_next())

        # ── 顶部状态栏 ─────────────────────
        top = tk.Frame(self.root, bg="#181825", pady=6)
        top.pack(fill="x")

        self.lbl_status = tk.Label(
            top, text="", font=("Consolas", 11),
            bg="#181825", fg="#cdd6f4"
        )
        self.lbl_status.pack(side="left", padx=14)

        self.lbl_saved = tk.Label(
            top, text="", font=("Consolas", 10),
            bg="#181825", fg="#a6e3a1"
        )
        self.lbl_saved.pack(side="right", padx=14)

        # ── 主区域 ─────────────────────────
        main = tk.Frame(self.root, bg="#1e1e2e")
        main.pack(fill="both", expand=True, padx=10, pady=6)

        # 左：图片
        left = tk.Frame(main, bg="#1e1e2e")
        left.pack(side="left", fill="both", expand=True)

        tk.Label(
            left, text="透视校正图", font=("Consolas", 10),
            bg="#1e1e2e", fg="#89b4fa"
        ).pack(anchor="w", padx=4)

        self.canvas = tk.Canvas(
            left, width=PREVIEW_W, height=PREVIEW_H,
            bg="#11111b", highlightthickness=1,
            highlightbackground="#45475a"
        )
        self.canvas.pack(fill="both", expand=True, padx=4, pady=4)

        # 右：标注
        right = tk.Frame(main, bg="#1e1e2e", width=320)
        right.pack(side="right", fill="y", padx=(10, 0))
        right.pack_propagate(False)

        tk.Label(
            right, text="题号真值（每行一条）", font=("Consolas", 10),
            bg="#1e1e2e", fg="#89b4fa"
        ).pack(anchor="w", padx=4, pady=(0, 2))

        tk.Label(
            right,
            text="格式：P页码  章.节.题号\n例：P200  4.5.12\n    P212  5.5.6(1)(3)",
            font=("Consolas", 9), justify="left",
            bg="#1e1e2e", fg="#6c7086"
        ).pack(anchor="w", padx=4)

        txt_frame = tk.Frame(right, bg="#1e1e2e")
        txt_frame.pack(fill="both", expand=True, padx=4, pady=6)

        self.txt = tk.Text(
            txt_frame,
            font=("Consolas", 12), wrap="none",
            bg="#181825", fg="#cdd6f4",
            insertbackground="#f5c2e7",
            selectbackground="#313244",
            relief="flat", padx=8, pady=8,
        )
        sb = tk.Scrollbar(txt_frame, command=self.txt.yview)
        self.txt.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.txt.pack(side="left", fill="both", expand=True)

        # ── 底部按钮 ─────────────────────
        bot = tk.Frame(self.root, bg="#181825", pady=6)
        bot.pack(fill="x")

        btn_cfg = dict(
            font=("Consolas", 11, "bold"),
            bd=0, relief="flat", padx=18, pady=6, cursor="hand2"
        )

        tk.Button(
            bot, text="← 上一张", command=self._on_prev,
            bg="#313244", fg="#cdd6f4", activebackground="#45475a",
            **btn_cfg
        ).pack(side="left", padx=10)

        tk.Button(
            bot, text="保存  Ctrl+S", command=self._on_save,
            bg="#89b4fa", fg="#1e1e2e", activebackground="#74c7ec",
            **btn_cfg
        ).pack(side="left", padx=4)

        tk.Button(
            bot, text="下一张 →", command=self._on_next,
            bg="#313244", fg="#cdd6f4", activebackground="#45475a",
            **btn_cfg
        ).pack(side="left", padx=10)

        # 进度条
        self.progress_var = tk.DoubleVar()
        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "green.Horizontal.TProgressbar",
            troughcolor="#313244", background="#a6e3a1", thickness=6
        )
        self.progress = ttk.Progressbar(
            bot, variable=self.progress_var, maximum=100,
            style="green.Horizontal.TProgressbar", length=200
        )
        self.progress.pack(side="right", padx=14)

        tk.Label(
            bot, text="已标注:", font=("Consolas", 10),
            bg="#181825", fg="#6c7086"
        ).pack(side="right")

    # ── 导航 ─────────────────────────────

    def _on_prev(self):
        self._flush_current()
        if self.idx > 0:
            self._show(self.idx - 1)

    def _on_next(self):
        self._flush_current()
        if self.idx < len(self.images) - 1:
            self._show(self.idx + 1)

    def _on_save(self):
        self._flush_current()
        self._save_gt()
        self.lbl_saved.config(text=f"✓ 已保存  {self.gt_path.name}")
        self.root.after(2500, lambda: self.lbl_saved.config(text=""))

    def _flush_current(self):
        """将当前文本框内容写入 ground_truth。"""
        if not self.images:
            return
        key = self.images[self.idx].name
        val = self.txt.get("1.0", "end").strip()
        if val:
            self.ground_truth[key] = val
        elif key in self.ground_truth:
            del self.ground_truth[key]
        self._update_progress()

    def _update_progress(self):
        total   = len(self.images)
        labeled = sum(1 for p in self.images if p.name in self.ground_truth)
        pct     = labeled / total * 100 if total else 0
        self.progress_var.set(pct)

    # ── 显示 ─────────────────────────────

    def _show(self, idx: int):
        self.idx = idx
        img_path = self.images[idx]
        total    = len(self.images)
        labeled  = sum(1 for p in self.images if p.name in self.ground_truth)

        self.lbl_status.config(
            text=f"[{idx+1}/{total}]  {img_path.name}    已标注 {labeled}/{total}"
        )
        self._update_progress()

        # 读图 + 透视校正
        bgr = cv2.imread(str(img_path))
        if bgr is None:
            self.canvas.delete("all")
            self.canvas.create_text(
                PREVIEW_W // 2, PREVIEW_H // 2,
                text="读取图片失败", fill="#f38ba8", font=("Consolas", 14)
            )
        else:
            board = _detect_board(bgr)
            self._render_image(board)

        # 填写已保存的真值
        self.txt.delete("1.0", "end")
        val = self.ground_truth.get(img_path.name, "")
        if val:
            self.txt.insert("1.0", val)
        self.txt.focus_set()

    def _render_image(self, bgr: np.ndarray):
        """将 BGR ndarray 缩放后显示到 canvas。"""
        h, w = bgr.shape[:2]
        cw = self.canvas.winfo_width()  or PREVIEW_W
        ch = self.canvas.winfo_height() or PREVIEW_H

        scale = min(cw / max(w, 1), ch / max(h, 1), 1.0)
        nw, nh = max(int(w * scale), 1), max(int(h * scale), 1)

        resized = cv2.resize(bgr, (nw, nh), interpolation=cv2.INTER_AREA)
        rgb     = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB)
        pil_img = Image.fromarray(rgb)
        self._photo = ImageTk.PhotoImage(pil_img)

        self.canvas.delete("all")
        # 居中显示
        x0 = (cw - nw) // 2
        y0 = (ch - nh) // 2
        self.canvas.create_image(x0, y0, anchor="nw", image=self._photo)


# ──────────────────────────────────────────
# 入口
# ──────────────────────────────────────────

def main():
    folder = Path(__file__).parent.resolve()

    root = tk.Tk()
    root.geometry("1180x680")
    root.minsize(900, 560)

    app = LabelApp(root, folder)  # noqa: F841

    # 窗口关闭时自动保存
    def on_close():
        app._flush_current()
        app._save_gt()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
