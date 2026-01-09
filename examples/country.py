#!/usr/bin/env python3

import csv
import tempfile
from pathlib import Path

from ltseq import LTSeq


def most_official_language_countries(lang_table, t_country) -> any:
    """Return countries with the most official languages and their populations."""
    official_langs = lang_table.filter(lambda r: r.IsOfficial == "T")

    country_counts = (
        official_langs.sort(lambda r: r.CountryCode)
        .group_ordered(lambda r: r.CountryCode)
        .derive(lambda g: {"official_lang_count": g.count()})
        .select(lambda r: [r.CountryCode, r.official_lang_count])
        .distinct("CountryCode")
    )

    max_count = (
        country_counts.sort(lambda r: r.official_lang_count, desc=True)
        .slice(0, 1)
    )

    top_counts = (
        country_counts.link(
            max_count,
            on=lambda c, m: c.official_lang_count == m.official_lang_count,
            as_="max",
        )
        .select(lambda r: [r.CountryCode, r.official_lang_count])
    )

    result = (
        top_counts.link(
            t_country,
            on=lambda c, country: c.CountryCode == country.Code,
            as_="country",
        )
        .select(
            lambda r: [
                r.CountryCode,
                r.country_Name,
                r.country_Population,
                r.official_lang_count,
            ]
        )
        .sort(lambda r: r.CountryCode)
    )
    return result


def most_official_language_countries_top_language(lang_table, t_country) -> any:
    """
    Return countries with the most official languages and their top official language share.
    """
    official_langs = lang_table.filter(lambda r: r.IsOfficial == "T")

    country_counts = (
        official_langs.sort(lambda r: r.CountryCode)
        .group_ordered(lambda r: r.CountryCode)
        .derive(lambda g: {"official_lang_count": g.count()})
        .select(lambda r: [r.CountryCode, r.official_lang_count])
        .distinct("CountryCode")
    )

    max_count = (
        country_counts.sort(lambda r: r.official_lang_count, desc=True)
        .slice(0, 1)
    )

    top_countries = (
        country_counts.link(
            max_count,
            on=lambda c, m: c.official_lang_count == m.official_lang_count,
            as_="max",
        )
        .select(lambda r: [r.CountryCode, r.official_lang_count])
    )

    top_language = (
        official_langs.sort(
            lambda r: r.CountryCode,
            lambda r: r.Percentage,
            desc=[False, True],
        )
        .group_ordered(lambda r: r.CountryCode)
        .first()
        .select(lambda r: [r.CountryCode, r.Language, r.Percentage])
    )

    top_with_language = (
        top_countries.link(
            top_language,
            on=lambda c, lang: c.CountryCode == lang.CountryCode,
            as_="lang",
        )
        ._materialize()
    )

    result = (
        top_with_language.link(
            t_country,
            on=lambda c, country: c.CountryCode == country.Code,
            as_="country",
        )
        .select(
            lambda r: [
                r.country_Name,
                r.country_Population,
                r.lang_Language,
                r.lang_Percentage,
            ]
        )
        .sort(lambda r: r.country_Name)
    )
    return result


def most_official_language_countries_language_list(lang_table, t_country) -> any:
    """
    Return top countries with official languages listed on one line by share desc.
    """
    official_langs = lang_table.filter(lambda r: r.IsOfficial == "T")

    country_counts = (
        official_langs.sort(lambda r: r.CountryCode)
        .group_ordered(lambda r: r.CountryCode)
        .derive(lambda g: {"official_lang_count": g.count()})
        .select(lambda r: [r.CountryCode, r.official_lang_count])
        .distinct("CountryCode")
    )

    max_count = (
        country_counts.sort(lambda r: r.official_lang_count, desc=True)
        .slice(0, 1)
    )

    top_countries = (
        country_counts.link(
            max_count,
            on=lambda c, m: c.official_lang_count == m.official_lang_count,
            as_="max",
        )
        .select(lambda r: [r.CountryCode])
    )

    top_langs = (
        official_langs.link(
            top_countries,
            on=lambda lang, c: lang.CountryCode == c.CountryCode,
            as_="top",
        )
        .select(lambda r: [r.CountryCode, r.Language, r.Percentage])
        .link(
            t_country,
            on=lambda lang, country: lang.CountryCode == country.Code,
            as_="country",
        )
        .select(lambda r: [r.CountryCode, r.country_Name, r.Language, r.Percentage])
        .sort(
            lambda r: r.CountryCode,
            lambda r: r.Percentage,
            desc=[False, True],
        )
    )

    # Materialize via CSV to avoid requiring pandas for grouping.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        temp_path = Path(f.name)
    try:
        top_langs.write_csv(str(temp_path))
        with temp_path.open(newline="") as f:
            rows = list(csv.DictReader(f))
    finally:
        temp_path.unlink(missing_ok=True)

    results = []
    current_code = None
    current_name = None
    languages = []

    for row in rows:
        code = row["CountryCode"]
        name = row["country_Name"]
        lang = row["Language"]
        percent = row["Percentage"]
        if code != current_code:
            if current_code is not None:
                results.append(
                    {
                        "country_Name": current_name,
                        "official_languages": ", ".join(languages),
                    }
                )
            current_code = code
            current_name = name
            languages = []
        languages.append(f"{lang}({percent}%)")

    if current_code is not None:
        results.append(
            {"country_Name": current_name, "official_languages": ", ".join(languages)}
        )

    return results

def main():
    """Demonstrate LTSeq filtering and selection capabilities."""
    # Load language table relative to this script so cwd doesn't matter
    data_dir = Path(__file__).resolve().parent
    t_lang = LTSeq.read_csv(str(data_dir / "countrylanguage.csv"))
    t_country = LTSeq.read_csv(str(data_dir / "country.csv"))

    summary = most_official_language_countries(t_lang, t_country)
    summary.show()

    top_language_summary = most_official_language_countries_top_language(
        t_lang, t_country
    )
    top_language_summary.show()

    language_lines = most_official_language_countries_language_list(t_lang, t_country)
    for row in language_lines:
        print(f"{row['country_Name']}: {row['official_languages']}")


if __name__ == "__main__":
    main()
