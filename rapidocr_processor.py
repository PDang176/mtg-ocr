import re

from config import Config
from rapidocr import RapidOCR

# ── OCR substitution table ────────────────────────────────────────────────────
OCR_SUBSTITUTIONS = {
    "0": "o",
    "1": "l",
    "I": "l",
    "@": "a",
    "8": "B",
    "|": "l",
    "!": "i",
    "$": "s",
    "5": "s",
    "6": "b",
    "9": "g",
    "2": "z",
}
# ─────────────────────────────────────────────────────────────────────────────

class RapidOCRProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.engine = RapidOCR(
          params={
              "Det.model_path": "models/det.onnx",
              "Cls.model_path": "models/cls.onnx",
              "Rec.model_path": "models/rec.onnx",
            }
          )

    def __clean_text(raw: str) -> str:
      if not raw:
          return ""

      substituted = "".join(OCR_SUBSTITUTIONS.get(ch, ch) for ch in raw)

      cleaned = re.sub(r"[^a-zA-Z0-9 \-']", "", substituted)
      cleaned = re.sub(r"\s+", " ", cleaned).strip()
      
      return cleaned.lower()

    def extract_text(self, image_array):
        result = self.engine(image_array)
        raw = " ".join(result.txts).strip() if result.txts else ""
        clean = self.__clean_text(raw)
        return raw, clean
