"""Explicit source-name aliases for the project's S2 canonical districts."""

from __future__ import annotations

def _key(value: str) -> str:
    return " ".join(
        value.replace("_", " ").replace("’", "'").split()
    ).casefold()


DISTRICT_NAME_ALIASES = {
    _key("Barisal"): "Barishal",
    _key("Bogra"): "Bogura",
    _key("Chittagong"): "Chattogram",
    _key("Comilla"): "Cumilla",
    _key("Cox's_Bazar"): "Cox's Bazar",
    _key("Jessore"): "Jashore",
    _key("Maulvibazar"): "Moulvibazar",
    _key("Nawabganj"): "Chapainababganj",
    _key("Chapainawabganj"): "Chapainababganj",
}


def canonical_district_name(value: str | None) -> str | None:
    if value is None:
        return None
    value = " ".join(
        str(value).replace("_", " ").replace("’", "'").split()
    )
    return DISTRICT_NAME_ALIASES.get(_key(value), value)


def canonical_district_name_column(column):
    from pyspark.sql import functions as F

    normalized = F.lower(
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(column.cast("string"), "_", " "),
                    "’",
                    "'",
                ),
                r"\s+",
                " ",
            )
        )
    )
    mapping = F.create_map(
        *[
            item
            for alias, canonical in DISTRICT_NAME_ALIASES.items()
            for item in (F.lit(alias), F.lit(canonical))
        ]
    )
    return F.coalesce(mapping[normalized], F.trim(column.cast("string")))
