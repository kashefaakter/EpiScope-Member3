"""Explicit source-name aliases for the S1 administrative districts."""

from __future__ import annotations

def _key(value: str) -> str:
    return " ".join(
        value.replace("_", " ").replace("’", "'").split()
    ).casefold()


DISTRICT_NAME_ALIASES = {
    _key("Barishal"): "Barisal",
    _key("Bogura"): "Bogra",
    _key("Brahmanbaria"): "Brahamanbaria",
    _key("Chattogram"): "Chittagong",
    _key("Cumilla"): "Comilla",
    _key("Cox's_Bazar"): "Cox's Bazar",
    _key("Jashore"): "Jessore",
    _key("Moulvibazar"): "Maulvibazar",
    _key("Chapainababganj"): "Nawabganj",
    _key("Chapainawabganj"): "Nawabganj",
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
