"""
etl/transformers/data_transformers.py
Transformers listos para usar en pipelines de producción.
"""
from __future__ import annotations
import logging
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

import pandas as pd

from etl.core.base import Transformer

log = logging.getLogger(__name__)


class PandasTransformer(Transformer[Dict]):
    """
    Transforma un chunk usando pandas DataFrame.
    Subclasear e implementar transform_df().

    Ejemplo de subclase:
        class SalesTransformer(PandasTransformer):
            def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
                df["revenue"] = df["units"].astype(float) * df["price"].astype(float)
                df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
                return df[df["revenue"] > 0]
    """

    def transform(self, records: List[Dict]) -> List[Dict]:
        if not records:
            return []
        df = pd.DataFrame(records)
        df_out = self.transform_df(df)
        return df_out.to_dict(orient="records")

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


class ColumnMapper(PandasTransformer):
    """
    Renombra columnas, elimina las no deseadas, añade valores por defecto.

    Ejemplo:
        mapper = ColumnMapper(
            rename={"CustomerID": "customer_id", "OrderDate": "order_date"},
            keep=["customer_id", "order_date", "total_amount"],
            defaults={"source": "legacy_crm"},
        )
    """

    def __init__(
        self,
        rename: Optional[Dict[str, str]] = None,
        keep: Optional[List[str]] = None,
        drop: Optional[List[str]] = None,
        defaults: Optional[Dict[str, Any]] = None,
    ):
        self.rename = rename or {}
        self.keep = keep
        self.drop = drop or []
        self.defaults = defaults or {}

    @property
    def name(self) -> str:
        return "ColumnMapper"

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.rename:
            df = df.rename(columns=self.rename)
        if self.drop:
            df = df.drop(columns=[c for c in self.drop if c in df.columns])
        if self.keep:
            missing = set(self.keep) - set(df.columns)
            if missing:
                log.warning("Columnas requeridas no encontradas: %s", missing)
            df = df[[c for c in self.keep if c in df.columns]]
        for col, value in self.defaults.items():
            if col not in df.columns:
                df[col] = value
            else:
                df[col] = df[col].fillna(value)
        return df


class TypeCaster(PandasTransformer):
    """
    Castea columnas a los tipos especificados.
    Registros que no se pueden castear van a la columna _cast_errors o se descartan.

    Ejemplo:
        caster = TypeCaster(
            types={
                "order_id":     "int",
                "total_amount": "float",
                "order_date":   "datetime:%Y-%m-%d",
                "is_active":    "bool",
            },
            on_error="drop",   # o "keep" para mantener el valor original
        )
    """

    def __init__(
        self,
        types: Dict[str, str],
        on_error: str = "drop",
    ):
        self.types = types
        self.on_error = on_error

    @property
    def name(self) -> str:
        return "TypeCaster"

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        errors_mask = pd.Series([False] * len(df))

        for col, type_spec in self.types.items():
            if col not in df.columns:
                log.warning("TypeCaster: columna '%s' no existe", col)
                continue

            try:
                if type_spec == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    errors_mask |= df[col].isna()
                elif type_spec == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    errors_mask |= df[col].isna()
                elif type_spec == "bool":
                    bool_map = {"true": True, "false": False, "1": True, "0": False,
                                "yes": True, "no": False, "y": True, "n": False}
                    df[col] = df[col].str.lower().map(bool_map)
                    errors_mask |= df[col].isna()
                elif type_spec.startswith("datetime:"):
                    fmt = type_spec.split(":", 1)[1]
                    df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
                    errors_mask |= df[col].isna()
                elif type_spec == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
                    errors_mask |= df[col].isna()
                elif type_spec == "str":
                    df[col] = df[col].astype(str).str.strip()

            except Exception as e:  # noqa: BLE001
                log.error("Error casteando columna '%s' a %s: %s", col, type_spec, e)

        error_count = errors_mask.sum()
        if error_count > 0:
            log.warning("TypeCaster: %d registros con errores de cast", error_count)
            if self.on_error == "drop":
                df = df[~errors_mask]

        return df


class DuplicateFilter(PandasTransformer):
    """
    Elimina duplicados basados en columnas clave.

    Ejemplo:
        dedup = DuplicateFilter(
            subset=["order_id"],
            keep="last",   # "first", "last" o False (elimina todos los duplicados)
        )
    """

    def __init__(self, subset: Optional[List[str]] = None, keep: str = "last"):
        self.subset = subset
        self.keep = keep  # type: ignore[assignment]

    @property
    def name(self) -> str:
        return f"DuplicateFilter(subset={self.subset})"

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=self.subset, keep=self.keep)  # type: ignore[arg-type]
        dropped = before - len(df)
        if dropped > 0:
            log.info("DuplicateFilter: %d duplicados eliminados", dropped)
        return df


class NullFilter(PandasTransformer):
    """
    Elimina registros con valores nulos en columnas obligatorias.

    Ejemplo:
        null_filter = NullFilter(required=["customer_id", "order_date"])
    """

    def __init__(self, required: List[str]):
        self.required = required

    @property
    def name(self) -> str:
        return f"NullFilter(required={self.required})"

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        existing = [c for c in self.required if c in df.columns]
        if not existing:
            return df
        # Tratar cadenas vacías como nulos
        df[existing] = df[existing].replace({"": None, "  ": None})
        before = len(df)
        df = df.dropna(subset=existing)
        dropped = before - len(df)
        if dropped > 0:
            log.warning("NullFilter: %d registros eliminados por nulos en %s", dropped, existing)
        return df


class ValueNormalizer(PandasTransformer):
    """
    Normaliza valores de texto: strip, lowercase/uppercase, reemplazos.

    Ejemplo:
        normalizer = ValueNormalizer(
            lowercase=["status", "country"],
            uppercase=["currency"],
            strip=["name", "email"],
            replacements={"status": {"Active": "active", "ACTIVE": "active"}},
        )
    """

    def __init__(
        self,
        lowercase: Optional[List[str]] = None,
        uppercase: Optional[List[str]] = None,
        strip: Optional[List[str]] = None,
        replacements: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        self.lowercase = lowercase or []
        self.uppercase = uppercase or []
        self.strip = strip or []
        self.replacements = replacements or {}

    @property
    def name(self) -> str:
        return "ValueNormalizer"

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.lowercase:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower().str.strip()
        for col in self.uppercase:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().str.strip()
        for col in self.strip:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        for col, mapping in self.replacements.items():
            if col in df.columns:
                df[col] = df[col].replace(mapping)
        return df


class ComputedColumns(PandasTransformer):
    """
    Añade columnas calculadas usando funciones lambda.

    Ejemplo:
        computed = ComputedColumns({
            "revenue":          lambda df: df["units"].astype(float) * df["price"].astype(float),
            "loaded_at":        lambda df: pd.Timestamp.utcnow(),
            "name_normalized":  lambda df: df["name"].str.lower().str.strip(),
        })
    """

    def __init__(self, columns: Dict[str, Callable[[pd.DataFrame], Any]]):
        self.columns = columns

    @property
    def name(self) -> str:
        return f"ComputedColumns({list(self.columns.keys())})"

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        for col_name, func in self.columns.items():
            try:
                df[col_name] = func(df)
            except Exception as e:  # noqa: BLE001
                log.error("Error calculando columna '%s': %s", col_name, e)
        return df
