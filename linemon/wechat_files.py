from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple


def _p(path_s: str) -> Path:
    return Path(path_s)


def _getenv(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _is_allowed_doc_ext(ext: str) -> bool:
    e = (ext or "").lower().lstrip(".")
    return e in {"pdf", "doc", "docx", "xls", "xlsx"}


def _is_allowed_image_ext(ext: str) -> bool:
    e = (ext or "").lower().lstrip(".")
    # "any image format" per user; keep common ones.
    return e in {"jpg", "jpeg", "png", "gif", "bmp", "webp", "tif", "tiff", "heic"}


@dataclass(frozen=True)
class DecodedDat:
    data: bytes
    ext: str
    media_type: str


class WeChatFiles:
    """
    Helper for locating received files from the local WeChat file structure.

    This does not hook/tamper with the WeChat process; it only reads files that
    WeChat already wrote to disk.
    """

    def __init__(self, root: str | Path | None = None, wxid_dir: str | None = None) -> None:
        root_s = str(root) if root is not None else _getenv("WECHAT_FILES_ROOT", "")
        self.root = _p(root_s) if root_s else None
        self.wxid_dir = (wxid_dir or _getenv("WECHAT_WXID_DIR", "")).strip() or None
        self._wx_root: Path | None = None

    def available(self) -> bool:
        return self.wx_root() is not None

    def wx_root(self) -> Path | None:
        if self._wx_root is not None:
            return self._wx_root
        if self.root is None:
            return None
        try:
            if not self.root.exists():
                return None
        except Exception:
            return None
        if self.wxid_dir:
            p = self.root / self.wxid_dir
            self._wx_root = p if p.exists() else None
            return self._wx_root
        # Auto-pick newest wxid_* folder.
        candidates = []
        try:
            for p in self.root.iterdir():
                if p.is_dir() and p.name.startswith("wxid_"):
                    try:
                        candidates.append((p.stat().st_mtime, p))
                    except Exception:
                        candidates.append((0.0, p))
        except Exception:
            candidates = []
        if not candidates:
            return None
        candidates.sort(reverse=True)
        self._wx_root = candidates[0][1]
        return self._wx_root

    def msg_file_root(self) -> Path | None:
        wx = self.wx_root()
        if wx is None:
            return None
        p = wx / "msg" / "file"
        return p if p.exists() else None

    def msg_attach_root(self) -> Path | None:
        wx = self.wx_root()
        if wx is None:
            return None
        p = wx / "msg" / "attach"
        return p if p.exists() else None

    def cache_root(self) -> Path | None:
        wx = self.wx_root()
        if wx is None:
            return None
        p = wx / "cache"
        return p if p.exists() else None

    def image_temp_root(self) -> Path | None:
        """
        Return the WeChat temp decoded-image directory when present.

        Observed layout:
          <wx_root>/temp/ImageTemp/YYYY-MM/<files...>
        """
        wx = self.wx_root()
        if wx is None:
            return None
        p = wx / "temp" / "ImageTemp"
        return p if p.exists() else None

    def temp_root(self) -> Path | None:
        wx = self.wx_root()
        if wx is None:
            return None
        p = wx / "temp"
        return p if p.exists() else None

    def _cache_message_roots(self) -> list[Path]:
        """
        Return cache/*/Message directories (newest first when possible).
        """
        base = self.cache_root()
        if base is None:
            return []
        roots: list[tuple[float, Path]] = []
        try:
            for p in base.iterdir():
                if not p.is_dir():
                    continue
                msg = p / "Message"
                if not msg.exists():
                    continue
                try:
                    roots.append((msg.stat().st_mtime, msg))
                except Exception:
                    roots.append((0.0, msg))
        except Exception:
            return []
        roots.sort(reverse=True)
        return [p for _mt, p in roots]

    def list_recent_bubble_cache_dats(
        self,
        *,
        since_epoch: float | None = None,
        message_dir_hint: str | None = None,
        max_files: int = 250,
    ) -> list[Path]:
        """
        List recently modified WeChat cache Bubble *.dat files.

        Layout observed:
          cache/YYYY-MM/Message/<hex>/Bubble/<token>_b.dat
        These Bubble cache files are typically lower-res, but the <token> often
        matches the corresponding msg/attach .dat filename, allowing us to locate
        the original image payload.
        """
        roots = self._cache_message_roots()
        if not roots:
            return []

        out: list[tuple[float, Path]] = []
        for root in roots:
            # root is cache/<ym>/Message
            try:
                msg_dirs = [p for p in root.iterdir() if p.is_dir()]
            except Exception:
                continue

            if message_dir_hint:
                msg_dirs = [p for p in msg_dirs if p.name == message_dir_hint]
                if not msg_dirs:
                    continue

            for md in msg_dirs:
                bubble = md / "Bubble"
                if not bubble.exists():
                    continue
                try:
                    for p in bubble.iterdir():
                        if not p.is_file():
                            continue
                        if p.suffix.lower() != ".dat":
                            continue
                        try:
                            mt = p.stat().st_mtime
                        except Exception:
                            continue
                        if since_epoch is not None and mt < since_epoch:
                            continue
                        out.append((mt, p))
                except Exception:
                    continue

        out.sort(key=lambda x: x[0], reverse=True)
        if max_files and len(out) > max_files:
            out = out[:max_files]
        return [p for _mt, p in out]

    def find_image_dat_by_token(self, token: str, *, attach_dir_hint: str | None = None) -> Path | None:
        """
        Find msg/attach/**/Img/<token>.dat (original image payload).
        """
        tok = (token or "").strip()
        if not tok:
            return None
        if tok.lower().endswith(".dat"):
            tok = tok[:-4]
        base = self.msg_attach_root()
        if base is None:
            return None

        # Prefer the attach dir hinted by cache/Message/<hex>.
        if attach_dir_hint:
            ad = base / attach_dir_hint
            if ad.exists():
                try:
                    for ym in sorted([p for p in ad.iterdir() if p.is_dir()], reverse=True):
                        cand = ym / "Img" / f"{tok}.dat"
                        if cand.exists() and cand.is_file():
                            return cand
                except Exception:
                    pass

        # Fallback: global search (can be slow on huge trees).
        try:
            for p in base.rglob(f"{tok}.dat"):
                if p.is_file() and p.parent.name == "Img":
                    return p
        except Exception:
            return None
        return None

    def find_doc_by_name(self, filename: str) -> Path | None:
        """
        Find a received document by exact filename under msg/file/**.
        """
        fn = (filename or "").strip()
        if not fn:
            return None
        base = self.msg_file_root()
        if base is None:
            return None
        # Fast path: msg/file/YYYY-MM/<filename> (common layout)
        try:
            for sub in sorted(base.iterdir(), reverse=True):
                if not sub.is_dir():
                    continue
                cand = sub / fn
                if cand.exists():
                    return cand
        except Exception:
            pass
        # Fallback: search deeper (can be slow if huge).
        try:
            for p in base.rglob(fn):
                if p.is_file():
                    return p
        except Exception:
            return None
        return None

    def list_recent_doc_files(self, *, since_epoch: float) -> list[Path]:
        base = self.msg_file_root()
        if base is None:
            return []
        out: list[tuple[float, Path]] = []
        # msg/file is typically partitioned by YYYY-MM; scan newest directories first.
        try:
            subs = [p for p in base.iterdir() if p.is_dir()]
            subs.sort(key=lambda p: getattr(p.stat(), "st_mtime", 0.0), reverse=True)
        except Exception:
            subs = []
        for sub in subs:
            try:
                for p in sub.iterdir():
                    if not p.is_file():
                        continue
                    ext = p.suffix.lower().lstrip(".")
                    if not _is_allowed_doc_ext(ext):
                        continue
                    try:
                        mt = p.stat().st_mtime
                    except Exception:
                        continue
                    if mt >= since_epoch:
                        out.append((mt, p))
            except Exception:
                continue
        out.sort()
        return [p for _mt, p in out]

    def list_recent_image_dats(
        self,
        *,
        since_epoch: float,
        include_thumbs: bool = False,
        attach_dir_hint: str | None = None,
    ) -> list[Path]:
        base = self.msg_attach_root()
        if base is None:
            return []
        out: list[tuple[float, Path]] = []
        # Layout: msg/attach/<hex>/<YYYY-MM>/Img/*.dat
        try:
            attach_dirs = [p for p in base.iterdir() if p.is_dir()]
        except Exception:
            attach_dirs = []
        if attach_dir_hint:
            attach_dirs = [p for p in attach_dirs if p.name == attach_dir_hint]
        for ad in attach_dirs:
            try:
                ym_dirs = [p for p in ad.iterdir() if p.is_dir()]
            except Exception:
                continue
            for ym in ym_dirs:
                img_dir = ym / "Img"
                if not img_dir.exists():
                    continue
                try:
                    for p in img_dir.iterdir():
                        if not p.is_file():
                            continue
                        if p.suffix.lower() != ".dat":
                            continue
                        if (not include_thumbs) and p.name.endswith("_t.dat"):
                            continue
                        try:
                            mt = p.stat().st_mtime
                        except Exception:
                            continue
                        if mt >= since_epoch:
                            out.append((mt, p))
                except Exception:
                    continue
        out.sort()
        return [p for _mt, p in out]

    def decode_wechat_dat(self, path: Path) -> DecodedDat | None:
        """
        Decode WeChat PC .dat images (simple XOR obfuscation).

        Returns bytes + inferred extension/mime, or None if it doesn't match known formats.
        """
        try:
            data = path.read_bytes()
        except Exception:
            return None
        if len(data) < 16:
            return None

        def valid_bmp(decoded: bytes) -> bool:
            # BMP header is only 2 bytes ("BM") so add structural validation to avoid false positives.
            if len(decoded) < 32:
                return False
            if decoded[:2] != b"BM":
                return False
            try:
                # BITMAPFILEHEADER
                bf_size = int.from_bytes(decoded[2:6], "little", signed=False)
                bf_off = int.from_bytes(decoded[10:14], "little", signed=False)
                dib_size = int.from_bytes(decoded[14:18], "little", signed=False)
            except Exception:
                return False
            # DIB header size is commonly 40, 108, or 124.
            if dib_size not in {40, 108, 124}:
                return False
            # Offset to pixel data should be within the file and after headers.
            if not (32 <= bf_off <= len(decoded)):
                return False
            # File size in header should be plausible.
            if bf_size and bf_size != len(decoded):
                # Allow small mismatches; some producers write 0 or slightly off values.
                if abs(bf_size - len(decoded)) > 256:
                    return False
            return True

        # Known signatures to attempt (bytes, ext, mime)
        sigs: list[tuple[bytes, str, str]] = [
            (b"\xff\xd8\xff", "jpg", "image/jpeg"),
            (b"\x89PNG\r\n\x1a\n", "png", "image/png"),
            (b"GIF87a", "gif", "image/gif"),
            (b"GIF89a", "gif", "image/gif"),
            (b"BM", "bmp", "image/bmp"),
            (b"RIFF", "webp", "image/webp"),  # also check WEBP marker below
        ]

        for sig, ext, mt in sigs:
            key = data[0] ^ sig[0]
            ok = True
            for i in range(min(len(sig), len(data))):
                if (data[i] ^ key) != sig[i]:
                    ok = False
                    break
            if not ok:
                continue
            decoded = bytes((b ^ key) for b in data)
            if ext == "webp":
                if len(decoded) < 12 or decoded[8:12] != b"WEBP":
                    continue
            if ext == "bmp":
                if not valid_bmp(decoded):
                    continue
            return DecodedDat(data=decoded, ext=ext, media_type=mt)

        return None

    def sniff_image_ext(self, path: Path) -> str | None:
        """
        Infer image extension from file header.

        Returns lowercase ext without dot (e.g. 'jpg'), or None.
        """
        try:
            with path.open("rb") as f:
                head = f.read(64)
        except Exception:
            return None
        if len(head) < 12:
            return None

        if head.startswith(b"\xff\xd8\xff"):
            return "jpg"
        if head.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
            return "gif"
        if head.startswith(b"BM"):
            return "bmp"
        if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
            return "webp"
        if head.startswith(b"II*\x00") or head.startswith(b"MM\x00*"):
            return "tif"
        # HEIC/HEIF: ISO BMFF with ftyp brand.
        if len(head) >= 16 and head[4:8] == b"ftyp":
            brand = head[8:12]
            if brand in {b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1"}:
                return "heic"
        return None

    def _iter_image_temp_files(self) -> Iterable[Path]:
        root = self.image_temp_root()
        if root is None:
            return []
        # Typical structure is shallow (YYYY-MM). Use rglob for robustness.
        try:
            return [p for p in root.rglob("*") if p.is_file()]
        except Exception:
            return []

    def _iter_temp_files(self) -> Iterable[Path]:
        root = self.temp_root()
        if root is None:
            return []
        # Exclude known non-image-heavy folders to reduce scan cost.
        skip_dirs = {"head_image"}
        out: list[Path] = []
        try:
            for p in root.rglob("*"):
                if not p.is_file():
                    continue
                try:
                    if any(part in skip_dirs for part in p.parts):
                        continue
                except Exception:
                    pass
                out.append(p)
        except Exception:
            return []
        return out

    def snapshot_image_temp_names(self) -> set[str]:
        """
        Return a set of existing ImageTemp filenames (not full paths).
        """
        names: set[str] = set()
        for p in self._iter_image_temp_files():
            try:
                names.add(p.name)
            except Exception:
                continue
        return names

    def snapshot_temp_names(self) -> set[str]:
        names: set[str] = set()
        for p in self._iter_temp_files():
            try:
                names.add(p.name)
            except Exception:
                continue
        return names

    def wait_for_new_image_temp_file(
        self,
        *,
        since_epoch: float,
        exclude_names: set[str] | None = None,
        timeout_seconds: float = 8.0,
        min_bytes: int = 4096,
    ) -> Path | None:
        """
        Poll ImageTemp for a newly written decoded image file.

        Returns the newest stable candidate (prefer larger files), or None.
        """
        exclude = exclude_names or set()
        deadline = time.time() + max(0.5, float(timeout_seconds))
        best: Path | None = None
        best_score: tuple[float, int] | None = None  # (mtime, size)

        while time.time() < deadline:
            candidates: list[Path] = []
            for p in self._iter_image_temp_files():
                try:
                    if p.name in exclude:
                        continue
                    st = p.stat()
                except Exception:
                    continue
                if st.st_mtime < (since_epoch - 0.25):
                    continue
                if st.st_size < int(min_bytes):
                    continue
                candidates.append(p)

            for p in candidates:
                try:
                    s1 = p.stat().st_size
                    time.sleep(0.12)
                    s2 = p.stat().st_size
                    if s2 != s1 or s2 <= 0:
                        continue
                    mt = p.stat().st_mtime
                    score = (float(mt), int(s2))
                except Exception:
                    continue
                if best_score is None or score > best_score:
                    best = p
                    best_score = score

            if best is not None:
                return best
            time.sleep(0.12)

        return None

    def wait_for_new_temp_image_file(
        self,
        *,
        since_epoch: float,
        exclude_names: set[str] | None = None,
        timeout_seconds: float = 8.0,
        min_bytes: int = 8192,
        max_scan_files: int = 4000,
    ) -> Path | None:
        """
        Broader fallback: poll <wx_root>/temp for a newly written decoded image file.

        Prefer files in ImageTemp, but accept any file that looks like an image by extension/header.
        """
        exclude = exclude_names or set()
        deadline = time.time() + max(0.5, float(timeout_seconds))

        best: Path | None = None
        best_score: tuple[int, float, int] | None = None  # (path_weight, mtime, size)

        while time.time() < deadline:
            files = list(self._iter_temp_files())
            if max_scan_files and len(files) > max_scan_files:
                # Heuristic: keep newest-ish by mtime to reduce work.
                try:
                    files.sort(key=lambda p: getattr(p.stat(), "st_mtime", 0.0), reverse=True)
                    files = files[:max_scan_files]
                except Exception:
                    files = files[:max_scan_files]

            for p in files:
                try:
                    if p.name in exclude:
                        continue
                    st = p.stat()
                except Exception:
                    continue
                if st.st_mtime < (since_epoch - 0.25):
                    continue
                if st.st_size < int(min_bytes):
                    continue
                ext = p.suffix.lower().lstrip(".")
                if ext == "dat":
                    continue
                if ext:
                    if not _is_allowed_image_ext(ext):
                        # Some WeChat builds write decoded images with generic extensions
                        # (e.g. .tmp). Accept them when the file header looks like an image.
                        ext2 = self.sniff_image_ext(p)
                        if not ext2:
                            continue
                else:
                    ext2 = self.sniff_image_ext(p)
                    if not ext2:
                        continue

                # Ensure it's done writing.
                try:
                    s1 = st.st_size
                    time.sleep(0.10)
                    s2 = p.stat().st_size
                    if s2 != s1 or s2 <= 0:
                        continue
                except Exception:
                    continue

                path_s = str(p).lower()
                path_weight = 0
                if "imagetemp" in path_s:
                    path_weight = 3
                elif "image" in path_s or "img" in path_s or "pic" in path_s:
                    path_weight = 2
                score = (path_weight, float(st.st_mtime), int(s2))
                if best_score is None or score > best_score:
                    best = p
                    best_score = score

            if best is not None:
                return best
            time.sleep(0.12)

        return None
