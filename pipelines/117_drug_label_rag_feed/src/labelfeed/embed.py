import hashlib
import re

import numpy as np

DIM = 384


class SentenceEmbedder:
    """all-MiniLM-L6-v2 on CPU. Loaded on first use so commands that don't embed stay fast."""

    def __init__(self, model: str, batch_size: int = 64):
        self.model = model
        self.batch_size = batch_size
        self._st = None

    def encode(self, texts: list[str]) -> np.ndarray:
        if self._st is None:
            from sentence_transformers import SentenceTransformer

            self._st = SentenceTransformer(self.model, device="cpu")
        if not texts:
            return np.zeros((0, DIM), dtype=np.float32)
        return self._st.encode(texts, batch_size=self.batch_size, normalize_embeddings=True,
                               convert_to_numpy=True, show_progress_bar=False)


class HashEmbedder:
    """Bag of hashed words. Only for tests, so they run without a model download."""

    model = "test-hash-384"

    def encode(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), DIM), dtype=np.float32)
        for i, text in enumerate(texts):
            for word in re.findall(r"[a-z0-9]+", text.lower()):
                out[i, int(hashlib.md5(word.encode()).hexdigest(), 16) % DIM] += 1.0
        norms = np.linalg.norm(out, axis=1, keepdims=True)
        return out / np.where(norms == 0, 1, norms)
