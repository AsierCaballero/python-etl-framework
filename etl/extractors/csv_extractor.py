"""
etl/extractors/csv_extractor.py
Extrae datos desde archivos CSV locales o desde un directorio completo.
Soporta: compresión gzip, detección de encoding, múltiples delimitadores.
"""
from __future__ import annotations
import csv
import gzip
import io
import logging
import os
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from etl.core.base import Extractor

log = logging.getLogger(__name__)


class CsvExtractor(Extractor[Dict]):
    """
    Lee uno o varios archivos CSV y los emite en chunks.

    Ejemplo:
        extractor = CsvExtractor("data/sales.csv", chunk_size=5000)
        for chunk in extractor.extract():
            print(len(chunk), "registros")
    """

    def __init__(
        self,
        path: str | Path,
        chunk_size: int = 1000,
        delimiter: str = ",",
        encoding: str = "utf-8",
        skip_blank_lines: bool = True,
        strip_whitespace: bool = True,
        glob_pattern: Optional[str] = None,
    ):
        self.path = Path(path)
        self.chunk_size = chunk_size
        self.delimiter = delimiter
        self.encoding = encoding
        self.skip_blank_lines = skip_blank_lines
        self.strip_whitespace = strip_whitespace
        self.glob_pattern = glob_pattern  # Si path es directorio

    @property
    def name(self) -> str:
        return f"CsvExtractor({self.path.name})"

    def _files(self) -> List[Path]:
        if self.path.is_dir():
            pattern = self.glob_pattern or "*.csv"
            files = sorted(self.path.glob(pattern))
            if not files:
                raise FileNotFoundError(
                    f"No se encontraron archivos '{pattern}' en {self.path}"
                )
            return files
        if not self.path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {self.path}")
        return [self.path]

    def _open(self, filepath: Path):
        """Abre el archivo, detectando si está comprimido con gzip."""
        if filepath.suffix == ".gz":
            return gzip.open(filepath, "rt", encoding=self.encoding)
        return open(filepath, encoding=self.encoding, newline="")

    def count(self) -> Optional[int]:
        """Cuenta líneas sin cargar todo en memoria. No abre archivos .gz."""
        total = 0
        for filepath in self._files():
            if filepath.suffix == ".gz":
                return None  # No contar en comprimidos (costoso)
            with open(filepath, encoding=self.encoding) as f:
                total += sum(1 for _ in f) - 1  # -1 por la cabecera
        return total

    def extract(self) -> Iterator[List[Dict]]:
        for filepath in self._files():
            log.info("Extrayendo de %s", filepath)
            chunk: List[Dict] = []

            with self._open(filepath) as fh:
                reader = csv.DictReader(fh, delimiter=self.delimiter)

                for row in reader:
                    if self.strip_whitespace:
                        row = {k.strip(): v.strip() for k, v in row.items() if k}
                    if self.skip_blank_lines and not any(row.values()):
                        continue

                    chunk.append(dict(row))

                    if len(chunk) >= self.chunk_size:
                        yield chunk
                        chunk = []

                if chunk:
                    yield chunk

            log.info("Archivo completado: %s", filepath.name)


class InMemoryCsvExtractor(Extractor[Dict]):
    """
    Lee un CSV desde un string en memoria (útil para tests y webhooks).

    Ejemplo:
        data = "name,age\\nAlice,30\\nBob,25"
        extractor = InMemoryCsvExtractor(data)
    """

    def __init__(self, csv_content: str, chunk_size: int = 1000, delimiter: str = ","):
        self.csv_content = csv_content
        self.chunk_size = chunk_size
        self.delimiter = delimiter

    @property
    def name(self) -> str:
        return "InMemoryCsvExtractor"

    def extract(self) -> Iterator[List[Dict]]:
        reader = csv.DictReader(io.StringIO(self.csv_content), delimiter=self.delimiter)
        chunk: List[Dict] = []
        for row in reader:
            chunk.append(dict(row))
            if len(chunk) >= self.chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
