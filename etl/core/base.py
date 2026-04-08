"""
etl/core/base.py
Clases base del framework ETL.
Cada pipeline es: Extractor → Transformer → Loader
"""
from __future__ import annotations
import abc
import dataclasses
import logging
import time
from typing import Any, Dict, Generic, Iterator, List, Optional, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


# ── Resultado de ejecución ────────────────────────────────────────
@dataclasses.dataclass
class RunResult:
    """Resultado de una ejecución de pipeline."""
    pipeline_name: str
    status: str            # "success" | "partial" | "failed"
    rows_extracted: int = 0
    rows_loaded: int = 0
    rows_failed: int = 0
    duration_seconds: float = 0.0
    error: Optional[str] = None
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.status == "success"

    def __str__(self) -> str:
        return (
            f"[{self.pipeline_name}] {self.status.upper()} | "
            f"extracted={self.rows_extracted} loaded={self.rows_loaded} "
            f"failed={self.rows_failed} duration={self.duration_seconds:.1f}s"
        )


# ── Extractor ─────────────────────────────────────────────────────
class Extractor(abc.ABC, Generic[T]):
    """
    Lee datos de una fuente y los devuelve en chunks.
    Implementar extract() que hace yield de listas de registros.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abc.abstractmethod
    def extract(self) -> Iterator[List[T]]:
        """
        Genera chunks de registros desde la fuente.
        Yield de listas para soportar fuentes grandes sin cargar todo en memoria.
        """

    def count(self) -> Optional[int]:
        """
        Número total de registros disponibles (si se puede saber de antemano).
        None si no es posible calcularlo eficientemente.
        """
        return None


# ── Transformer ───────────────────────────────────────────────────
class Transformer(abc.ABC, Generic[T]):
    """
    Transforma un chunk de registros.
    Puede filtrar, enriquecer, validar, cambiar tipo, etc.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abc.abstractmethod
    def transform(self, records: List[T]) -> List[T]:
        """
        Transforma una lista de registros.
        Retorna la lista transformada (puede ser de distinto tamaño si filtra).
        """

    def validate(self, records: List[T]) -> List[str]:
        """
        Valida registros. Retorna lista de errores (vacía si todo OK).
        Sobreescribir para validaciones específicas.
        """
        return []


class IdentityTransformer(Transformer[T]):
    """Transformer que no hace nada — útil para debug y tests."""

    def transform(self, records: List[T]) -> List[T]:
        return records


class ChainTransformer(Transformer[T]):
    """Encadena varios transformers aplicándolos en secuencia."""

    def __init__(self, *transformers: Transformer[T]):
        self.transformers = transformers

    @property
    def name(self) -> str:
        return " → ".join(t.name for t in self.transformers)

    def transform(self, records: List[T]) -> List[T]:
        result = records
        for t in self.transformers:
            result = t.transform(result)
        return result


# ── Loader ────────────────────────────────────────────────────────
class Loader(abc.ABC, Generic[T]):
    """Escribe registros en el destino."""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abc.abstractmethod
    def load(self, records: List[T]) -> int:
        """
        Carga los registros en el destino.
        Retorna el número de registros cargados con éxito.
        """

    def begin(self) -> None:
        """Llamado antes del primer chunk. Setup de conexiones, transacciones."""

    def commit(self) -> None:
        """Llamado al final si todo fue bien. Confirmar transacciones."""

    def rollback(self) -> None:
        """Llamado si hay un error. Deshacer cambios."""


# ── Pipeline ──────────────────────────────────────────────────────
class Pipeline:
    """
    Orquesta la ejecución de Extractor → Transformer → Loader.

    Características:
    - Procesa en chunks (no carga todo en memoria)
    - Continúa ante errores de chunks individuales (si fail_fast=False)
    - Retorna RunResult con métricas completas
    - Logging detallado de progreso
    """

    def __init__(
        self,
        name: str,
        extractor: Extractor,
        transformer: Transformer,
        loader: Loader,
        fail_fast: bool = True,
        log_every_n_chunks: int = 10,
    ):
        self.name = name
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.fail_fast = fail_fast
        self.log_every_n_chunks = log_every_n_chunks

    def run(self) -> RunResult:
        """Ejecuta el pipeline completo y retorna el resultado."""
        log.info(
            "Pipeline '%s' iniciado | extractor=%s transformer=%s loader=%s",
            self.name, self.extractor.name, self.transformer.name, self.loader.name
        )
        start = time.time()
        result = RunResult(pipeline_name=self.name, status="failed")
        chunk_num = 0
        total_extracted = 0
        total_loaded = 0
        total_failed = 0

        try:
            self.loader.begin()

            for chunk in self.extractor.extract():
                chunk_num += 1
                chunk_size = len(chunk)
                total_extracted += chunk_size

                try:
                    transformed = self.transformer.transform(chunk)
                    loaded = self.loader.load(transformed)
                    total_loaded += loaded
                    failed_in_chunk = len(transformed) - loaded
                    total_failed += failed_in_chunk

                    if chunk_num % self.log_every_n_chunks == 0:
                        log.info(
                            "Pipeline '%s' progreso: chunk=%d extracted=%d loaded=%d",
                            self.name, chunk_num, total_extracted, total_loaded
                        )

                except Exception as chunk_err:  # noqa: BLE001
                    total_failed += chunk_size
                    log.error(
                        "Pipeline '%s' error en chunk %d: %s",
                        self.name, chunk_num, chunk_err
                    )
                    if self.fail_fast:
                        raise

            self.loader.commit()
            status = "success" if total_failed == 0 else "partial"
            result.status = status

        except Exception as err:  # noqa: BLE001
            result.status = "failed"
            result.error = str(err)
            log.error("Pipeline '%s' falló: %s", self.name, err)
            try:
                self.loader.rollback()
            except Exception:  # noqa: BLE001
                pass
        finally:
            result.rows_extracted = total_extracted
            result.rows_loaded = total_loaded
            result.rows_failed = total_failed
            result.duration_seconds = round(time.time() - start, 2)

        log.info("Pipeline '%s' completado: %s", self.name, result)
        return result
