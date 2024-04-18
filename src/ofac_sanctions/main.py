import re
from datetime import datetime

from lxml import etree
import polars as pl


def parse_name(_, first_name, last_name):
    names = []

    if first_name:
        names.append(first_name.capitalize())
    if last_name:
        names.append(last_name.capitalize())

    return " ".join(names)


def parse_dates_of_birth(_, dates_of_birth):
    date_patterns = [
        ("(?:circa )?([0-9]{4})(?: to .*)?", "%Y"),
        ("(?:circa )?([A-Za-z]{3} [0-9]{4})(?: to .*)?", "%b %Y"),
        ("(?:circa )?([0-9]{2} [A-Za-z]{3} [0-9]{4})(?: to .*)?", '%d %b %Y'),
    ]

    dates_result = []

    for date_of_birth in dates_of_birth:
        for pattern, datetime_format in date_patterns:
            if match := re.fullmatch(pattern, date_of_birth):
                dates_result.append(str(datetime.strptime(match.group(1), datetime_format).year))

    return dates_result


ns = etree.FunctionNamespace(None)
ns['parse_name'] = parse_name
ns['parse_dates_of_birth'] = parse_dates_of_birth


class SanctionsDatasetFactory:
    def __init__(self, file_path, namespaces):
        self._file_path = file_path
        self._namespaces = namespaces

    def parse_name(self, sdn_entry) -> str:
        return self._run_xpath(
            sdn_entry,
            "parse_name(string(ns:firstName), string(ns:lastName))"
        )

    def parse_sdn_type(self, sdn_entry) -> str:
        return self._run_xpath(sdn_entry, "string(ns:sdnType/text())")

    def parse_programs(self, sdn_entry) -> list[str]:
        return self._run_xpath(sdn_entry, "ns:programList/ns:program/text()")

    def parse_akas(self, sdn_entry) -> list[dict[str, str]]:
        return [
            {"aka_" + self._remove_namespace(el.tag): el.text for el in aka}
            for aka in
            self._run_xpath(sdn_entry, "ns:akaList/ns:aka")
        ]

    def parse_addresses(self, sdn_entry) -> list[dict[str, str]]:
        return [
            {"address_" + self._remove_namespace(el.tag): el.text for el in
             address}
            for address in
            self._run_xpath(sdn_entry, "ns:addressList/ns:address")
        ]

    def parse_dates_of_birth(self, sdn_entry) -> list[str]:
        return self._run_xpath(
            sdn_entry,
            "parse_dates_of_birth(ns:dateOfBirthList/ns:dateOfBirthItem/ns:dateOfBirth/text())",
        )

    def parse_places_of_birth(self, sdn_entry) -> list[dict[str, str]]:
        place_of_birth_raw = self._run_xpath(
            sdn_entry,
            "ns:placeOfBirthList/ns:placeOfBirthItem/ns:placeOfBirth/text()"
        )

        place_of_birth = []
        for place in place_of_birth_raw:
            match str(place).split(","):
                case [country]:
                    place_of_birth.append(
                        {"city": None, "region": None, "country": country.strip()}
                    )
                case [region, country]:
                    place_of_birth.append(
                        {"city": None, "region": region, "country": country.strip()}
                    )
                case [city, region, country]:
                    place_of_birth.append(
                        {"city": city, "region": region, "country": country.strip()}
                    )

        return place_of_birth

    def parse_passport_country(self, sdn_entry) -> None:
        return sdn_entry.xpath(
            "string(ns:idList/ns:id/ns:idType[text()='Passport']/../ns:idCountry)",
            namespaces=self._namespaces
        )

    def create_dataset(self):
        namespaces = {'ns': 'http://tempuri.org/sdnList.xsd'}
        root = etree.parse(self._file_path).getroot()

        dataset = []
        for entry in root.xpath("/ns:sdnList/ns:sdnEntry", namespaces=namespaces):
            dataset.append({
                "name": self.parse_name(entry),
                "sdn_type": self.parse_sdn_type(entry),
                "passport_country": self.parse_passport_country(entry),
                "programs": self.parse_programs(entry),
                "akas": self.parse_akas(entry),
                "addresses": self.parse_addresses(entry),
                "dates_of_birth": self.parse_dates_of_birth(entry),
                "places_of_birth": self.parse_places_of_birth(entry)
            })

        return dataset

    def _run_xpath(self, entry, query):
        return entry.xpath(query, namespaces=self._namespaces)

    def _remove_namespace(self, value):
        return value.removeprefix("{" + self._namespaces['ns'] + "}")


def create_dataset(datafile_path):
    namespaces = {'ns': 'http://tempuri.org/sdnList.xsd'}
    return SanctionsDatasetFactory(datafile_path, namespaces).create_dataset()


def create_dataframe(dataset):
    dataframe = pl.from_records(dataset)

    list_fields = ["programs", "akas", "addresses", "dates_of_birth", "places_of_birth"]
    for field in list_fields:
        dataframe = dataframe.explode(field)

    dict_fields = ["places_of_birth", "addresses", "akas"]
    for field in dict_fields:
        dataframe = dataframe.unnest(field)

    return dataframe


def run_queries(dataframe):
    # country with the largest number of sanctioned entities/individuals
    (dataframe
     .filter(pl.col("country").is_not_null())
     .group_by("country")
     .agg(pl.len().alias("Country with most sanctioned individuals"))
     .sort("Country with most sanctioned individuals", descending=True)
     .limit(10))

    # country with the least number of sanctioned entities/individuals
    (dataframe
     .filter(pl.col("country").is_not_null())
     .group_by("country")
     .agg(pl.len().alias("Country with least # sanctioned individuals"))
     .sort("Country with least # sanctioned individuals")
     .limit(10))

    # sanctioned individuals with the largest number of akas
    (dataframe
     .filter(pl.col("name").is_not_null())
     .group_by("name")
     .agg(pl.col("aka_uid").count().alias("akas count"))
     .sort("akas count", descending=True)
     .limit(10))

    # most prevalent sanctions programs
    (dataframe
     .group_by("programs")
     .agg(pl.len().alias("Most prevalent sanctions programs"))
     .sort("Most prevalent sanctions programs", descending=True)
     .limit(10))

    # sanctioned individuals with Portuguese Passport
    (dataframe.filter(pl.col("passport_country") == "Portugal").select(pl.col("name")).unique())

    # sanctioned individuals/entities with a "Portuguese" address
    (dataframe
     .filter(pl.col("address_address1").str.contains("Portugal"))
     .select("name", "sdn_type", "address_address1", "address_country"))

    # oldest sanctioned individuals
    (dataframe
     .filter((pl.col("dates_of_birth").is_not_null()) & (pl.col("sdn_type") == "Individual"))
     .sort("dates_of_birth", descending=False)
     .limit(20)
     .select("name", "dates_of_birth")
     .unique(maintain_order=True))

    # youngest sanctioned individuals
    (dataframe
     .filter((pl.col("dates_of_birth").is_not_null()) & (pl.col("sdn_type") == "Individual"))
     .sort("dates_of_birth", descending=True)
     .limit(20)
     .select("name", "dates_of_birth")
     .unique(maintain_order=True))


if __name__ == '__main__':
    df = create_dataframe(create_dataset(r'../../sdn.xml'))
    run_queries(df)
