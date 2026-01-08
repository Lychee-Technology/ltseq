#!/usr/bin/env python3
"""
Country Language Analysis Example

Demonstrates LTSeq's filtering and selection capabilities.

This is a simplified example that works with the current LTSeq API.
The full GROUP BY solution requires additional features.

SQL Equivalent (aspirational - requires GROUP BY features):
    SELECT cl.CountryCode, c.Name, cl.Language, cl.Percentage
    FROM countrylanguage cl
    JOIN country c ON cl.CountryCode = c.Code
    WHERE cl.IsOfficial = 'T'
    GROUP BY cl.CountryCode
    HAVING COUNT(*) = (SELECT MAX(cnt) FROM (
        SELECT COUNT(*) as cnt FROM countrylanguage
        WHERE IsOfficial = 'T' GROUP BY CountryCode
    ))
    ORDER BY cl.CountryCode, cl.Percentage DESC

TODO: Features needed for full GROUP BY solution:
- [ ] B1: Descending sort (desc=True)
- [ ] B2: group_ordered with __group_id__
- [ ] B3: NestedTable.count()
- [ ] B5: NestedTable.filter() for count predicates
- [ ] B6: max() scalar function
"""

from ltseq import LTSeq


def main():
    """Demonstrate LTSeq filtering and selection capabilities."""
    # Load language table
    t_lang = LTSeq.read_csv("examples/countrylanguage.csv")

    # Step 1: Filter for official languages (IsOfficial = 'T')
    official_langs = t_lang.filter(lambda r: r.IsOfficial == "T")

    # Step 2: Sort by CountryCode (ascending)
    sorted_result = official_langs.sort(lambda r: r.CountryCode)

    # Step 3: Select final columns
    result = sorted_result.select(lambda r: [r.CountryCode, r.Language, r.Percentage])

    # Display results
    print("Official Languages by Country")
    print("=" * 60)
    result.show(20)

    # --- ASPIRATIONAL: Full GROUP BY solution (not yet working) ---
    # When B1-B6 are implemented, this will work with linking:
    #
    # t_country = LTSeq.read_csv("examples/country.csv")
    #
    # # Find the maximum count of official languages for any country
    # max_lang_count = (
    #     official_langs
    #     .sort(lambda r: r.CountryCode)
    #     .group_ordered(lambda r: r.CountryCode)
    #     .count()
    #     .flatten()
    #     .max(lambda r: r.__group_count__)
    # )
    #
    # # Get countries with the most official languages
    # result = (
    #     official_langs
    #     .sort(lambda r: r.CountryCode)
    #     .group_ordered(lambda r: r.CountryCode)
    #     .filter(lambda g: g.count() == max_lang_count)
    #     .flatten()
    #     .link(t_country, on=lambda l, c: l.CountryCode == c.Code, as_="country")
    #     .sort(
    #         lambda r: r.CountryCode,
    #         lambda r: r.Percentage,
    #         desc=[False, True]  # CountryCode ASC, Percentage DESC
    #     )
    #     .select(lambda r: [
    #         r.CountryCode,
    #         r.country_Name,
    #         r.Language,
    #         r.Percentage
    #     ])
    # )
    # result.show()


if __name__ == "__main__":
    main()
