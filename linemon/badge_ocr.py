from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np


# OCR output is typically digits or "99+"; keep it permissive but simple.
_DIGITS_RE = re.compile(r"(\d{1,3})(\+)?")


def _find_tesseract_cmd(configured: str | None) -> str | None:
    if configured:
        p = Path(configured)
        if p.exists():
            return str(p)
    w = shutil.which("tesseract")
    if w:
        return w
    candidates = [
        r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
        r"C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return None


@dataclass(frozen=True)
class BadgeOCRConfig:
    tesseract_cmd: str | None = None
    debug: bool = False
    debug_dir: str = "debug"


class BadgeOCR:
    """
    Local red-badge detection + OCR of digits.

    Uses:
      - mss for fast capture
      - opencv for red blob detection + preprocessing
      - pytesseract (Tesseract OCR) for digit OCR
    """

    def __init__(self, cfg: BadgeOCRConfig) -> None:
        self.cfg = cfg
        self._tesseract_cmd = _find_tesseract_cmd(cfg.tesseract_cmd)
        self._ensure_tesseract()
        self._sct = None

    def _ensure_tesseract(self) -> None:
        if not self._tesseract_cmd:
            raise RuntimeError(
                "tesseract.exe not found. Install Tesseract OCR or set config.tesseract_cmd."
            )
        import pytesseract

        pytesseract.pytesseract.tesseract_cmd = self._tesseract_cmd

    def _get_sct(self):
        if self._sct is None:
            import mss

            self._sct = mss.mss()
        return self._sct

    def unread_count_for_row_rect(
        self,
        *,
        left: int,
        top: int,
        right: int,
        bottom: int,
        debug_key: str = "",
    ) -> Optional[int]:
        """
        Return unread badge count for a chat row.
        - None: could not determine (no badge found)
        - int: parsed digit count (or 1 if badge found but OCR fails)
        """
        import cv2

        width = max(1, right - left)
        height = max(1, bottom - top)

        # Be generous: depending on WeChat build/theme, unread badges can appear
        # near the avatar (left) or near the time label (right). Capturing most of
        # the row and then detecting a small red blob is more robust than assuming
        # a fixed position.
        roi_left = left + int(width * 0.05)
        roi_top = top
        roi_right = right - int(width * 0.02)
        roi_bottom = top + int(height * 0.92)
        roi_w = max(1, roi_right - roi_left)
        roi_h = max(1, roi_bottom - roi_top)

        sct = self._get_sct()
        shot = sct.grab({"left": roi_left, "top": roi_top, "width": roi_w, "height": roi_h})
        img = np.asarray(shot)  # BGRA
        bgr = img[:, :, :3]

        hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
        # Red wraps around HSV hue=0.
        mask1 = cv2.inRange(hsv, (0, 80, 60), (10, 255, 255))
        mask2 = cv2.inRange(hsv, (160, 80, 60), (180, 255, 255))
        mask = cv2.bitwise_or(mask1, mask2)
        k = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, k, iterations=2)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, k, iterations=1)

        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            self._debug_save(debug_key, "roi.png", bgr)
            self._debug_save(debug_key, "mask.png", mask)
            return None

        best = None
        best_score = None
        for c in contours:
            x, y, w, h = cv2.boundingRect(c)
            area = w * h
            if area < 40 or area > 6000:
                continue
            ar = w / max(1, h)
            if ar < 0.5 or ar > 4.5:
                continue

            # Reject noisy false-positives by ensuring the blob is actually red in BGR.
            patch = bgr[y : y + h, x : x + w]
            if patch.size:
                mean_b, mean_g, mean_r = patch.reshape(-1, 3).mean(axis=0)
                # Typical unread badge is bright red; require R dominance.
                if mean_r < 110 or mean_r < (mean_g + 25) or mean_r < (mean_b + 25):
                    continue

            # Prefer blobs near likely badge positions (near left avatar or near right time).
            cx = x + w / 2.0
            cy = y + h / 2.0
            # Candidate centers in ROI coordinates.
            left_cx, left_cy = roi_w * 0.20, roi_h * 0.25
            right_cx, right_cy = roi_w * 0.90, roi_h * 0.25
            d_left = (cx - left_cx) ** 2 + (cy - left_cy) ** 2
            d_right = (cx - right_cx) ** 2 + (cy - right_cy) ** 2
            d = min(d_left, d_right)
            # Larger area is good; closer to a candidate center is good.
            score = area - (d * 0.002)
            if best is None or (best_score is not None and score > best_score):
                best = (x, y, w, h)
                best_score = score

        if best is None:
            self._debug_save(debug_key, "roi.png", bgr)
            self._debug_save(debug_key, "mask.png", mask)
            return None

        x, y, w, h = best
        badge = bgr[y : y + h, x : x + w].copy()

        # Sanity: badge should be a compact blob; if it's too large, it's likely a false-positive
        # (e.g. red logo/icon). Typical unread badges are small circles/rounded-rects.
        if w > max(42, int(roi_w * 0.20)) or h > max(42, int(roi_h * 0.55)):
            self._debug_save(debug_key, "roi.png", bgr)
            self._debug_save(debug_key, "mask.png", mask)
            self._debug_save(debug_key, "badge.png", badge)
            return None
        # Also reject blobs that are too tiny to be a numeric badge.
        if w < 12 or h < 12:
            self._debug_save(debug_key, "roi.png", bgr)
            self._debug_save(debug_key, "mask.png", mask)
            self._debug_save(debug_key, "badge.png", badge)
            return None
        # Unread badges are not tall skinny rectangles.
        if h > (w * 1.25):
            self._debug_save(debug_key, "roi.png", bgr)
            self._debug_save(debug_key, "mask.png", mask)
            self._debug_save(debug_key, "badge.png", badge)
            return None

        hsv_b = cv2.cvtColor(badge, cv2.COLOR_BGR2HSV)
        digit_mask = cv2.inRange(hsv_b, (0, 0, 150), (180, 115, 255))
        # Heuristic: real unread badges usually include a sizable chunk of near-white digit pixels.
        # This helps reject red avatars/logos that can look "badge-like" in HSV.
        digit_ratio = float(np.count_nonzero(digit_mask)) / float(digit_mask.size or 1)
        if digit_ratio < 0.22:
            self._debug_save(debug_key, "roi.png", bgr)
            self._debug_save(debug_key, "mask.png", mask)
            self._debug_save(debug_key, "badge.png", badge)
            self._debug_save(debug_key, "digit_mask.png", digit_mask)
            return None

        # Remove the outer red ring/edges by masking to a central region. This avoids
        # OCR confusing the badge outline as additional digits (common on "1").
        bh, bw = digit_mask.shape[:2]
        inner = digit_mask.copy()
        inner[: int(bh * 0.18), :] = 0
        inner[int(bh * 0.90) :, :] = 0
        inner[:, : int(bw * 0.18)] = 0
        inner[:, int(bw * 0.90) :] = 0

        # Keep only the largest connected component (should be the digit(s)).
        num, labels, stats, _ = cv2.connectedComponentsWithStats(inner)
        digit_only = inner
        if num > 1:
            best_i = 1
            best_area = 0
            for i in range(1, num):
                area = int(stats[i, cv2.CC_STAT_AREA])
                if area > best_area:
                    best_area = area
                    best_i = i
            digit_only = (labels == best_i).astype("uint8") * 255

        # Make OCR-friendly: black digits on white background, padded, scaled.
        ocr_img = 255 - digit_only
        ocr_img = cv2.copyMakeBorder(ocr_img, 8, 8, 8, 8, cv2.BORDER_CONSTANT, value=255)
        ocr_img = cv2.resize(ocr_img, None, fx=5, fy=5, interpolation=cv2.INTER_NEAREST)
        ocr_img = cv2.medianBlur(ocr_img, 3)
        _, ocr_bw = cv2.threshold(ocr_img, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

        self._debug_save(debug_key, "roi.png", bgr)
        self._debug_save(debug_key, "mask.png", mask)
        self._debug_save(debug_key, "badge.png", badge)
        self._debug_save(debug_key, "digit_mask.png", digit_mask)
        self._debug_save(debug_key, "digit_only.png", digit_only)
        self._debug_save(debug_key, "ocr.png", ocr_img)
        self._debug_save(debug_key, "ocr_bw.png", ocr_bw)

        text = self._ocr_digits(ocr_bw)
        if not text:
            # Badge present but OCR failed; treat as unread.
            return 1
        m = _DIGITS_RE.search(text)
        if not m:
            return 1
        try:
            n = int(m.group(1))
        except Exception:
            return 1
        if m.group(2):
            # "99+" -> treat as 99.
            return n
        # For single-digit badges, Tesseract sometimes confuses 6/9/0/8 with 3/5/etc.
        # Use simple topology (hole counting) to correct common errors.
        if 0 <= n <= 9:
            n2 = self._fix_single_digit_by_topology(n, ocr_bw)
            n = n2

        # Final sanity bounds for unread counts.
        if n <= 0:
            return 1
        if n > 200:
            # Extremely unlikely; treat as unread but unknown count.
            return 1
        return n

    def _ocr_digits(self, img: np.ndarray) -> str:
        import pytesseract

        # Badge digits are tiny and can confuse some PSM modes. Try a couple.
        cfgs = [
            "--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789+",
            "--psm 7 --oem 3 -c tessedit_char_whitelist=0123456789+",
            "--psm 8 --oem 3 -c tessedit_char_whitelist=0123456789+",
        ]
        for config in cfgs:
            try:
                out = pytesseract.image_to_string(img, config=config)
            except Exception:
                continue
            out = (out or "").strip()
            out = out.replace(" ", "").replace("\\n", "")
            if out:
                return out
        return ""

    def _fix_single_digit_by_topology(self, digit: int, ocr_bw: np.ndarray) -> int:
        """
        Correct common single-digit OCR mistakes by counting internal holes.

        We look at the binarized black-on-white digit image (ocr_bw).
        """
        import cv2

        # Convert to white digit on black background.
        inv = 255 - ocr_bw
        inv = cv2.morphologyEx(inv, cv2.MORPH_CLOSE, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)))

        contours, hierarchy = cv2.findContours(inv, cv2.RETR_CCOMP, cv2.CHAIN_APPROX_SIMPLE)
        if hierarchy is None or not contours:
            return digit
        hierarchy = hierarchy[0]

        # Pick the largest outer contour as "the digit".
        outer_idxs = [i for i, h in enumerate(hierarchy) if h[3] == -1]
        if not outer_idxs:
            return digit
        outer = max(outer_idxs, key=lambda i: cv2.contourArea(contours[i]))
        # Collect its hole contours (children).
        holes = []
        child = hierarchy[outer][2]
        while child != -1:
            holes.append(child)
            child = hierarchy[child][0]

        hole_count = len(holes)
        if hole_count == 0:
            # If OCR returned a "hole digit" but we see none, leave it.
            return digit
        if hole_count >= 2:
            return 8

        # Exactly one hole: decide 0 vs 6 vs 9 by the hole position.
        x, y, w, h = cv2.boundingRect(contours[outer])
        if h <= 0:
            return digit
        # Hole centroid in digit box coords.
        hx, hy, hw, hh = cv2.boundingRect(contours[holes[0]])
        hole_cy = (hy + hh / 2.0)
        rel = hole_cy / float(h)

        # rel ~0.35 => 9, rel ~0.6 => 6, center-ish => 0
        if rel < 0.45:
            return 9
        if rel > 0.58:
            return 6
        return 0

    def _debug_save(self, key: str, name: str, img) -> None:
        if not self.cfg.debug:
            return
        try:
            import cv2
        except Exception:
            return
        d = Path(self.cfg.debug_dir) / "badge_ocr"
        if key:
            # Keep debug paths ASCII-ish to avoid tooling hiccups on some shells.
            safe = re.sub(r"[^A-Za-z0-9._-]+", "_", key)[:80].strip("_") or "row"
            d = d / safe
        d.mkdir(parents=True, exist_ok=True)
        p = d / name
        try:
            if isinstance(img, np.ndarray) and img.ndim == 2:
                cv2.imwrite(str(p), img)
            else:
                cv2.imwrite(str(p), img)
        except Exception:
            pass
