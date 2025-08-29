
# coding: utf-8

print("Script started")

with open("./Started.txt", "w") as file:
    file.write("Started")

def main():
    print("Script started2")
    # Your code here

if __name__ == "__main__":
    main()

# Import necessary libraries
import datetime
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import pandas as pd
import time
import re
import numpy as np
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

from email.mime.text import MIMEText
import base64

import sqlite3
import shutil




# Links collection
cities = [
    "kielce",
    "wroclaw",
    "warszawa",
]  # , "krakow", "gdansk", "poznan", "lodz", "lublin", "bialystok", "bydgoszcz", "katowice", "gorzow-wielkopolski", "szczecin", "olsztyn", "torun"]
types_of_agreement = ["wynajem", "sprzedaz"]


months_map = {
    "stycznia": "January",
    "lutego": "February",
    "marca": "March",
    "kwietnia": "April",
    "maja": "May",
    "czerwca": "June",
    "lipca": "July",
    "sierpnia": "August",
    "września": "September",
    "października": "October",
    "listopada": "November",
    "grudnia": "December"
}

def polish_to_eng(date_string):
    for pl_month, en_month in months_map.items():
        if pl_month in str(date_string):
            return date_string.replace(pl_month,en_month)
    return date_string




xf = datetime.datetime.now()
xf = xf.strftime("%Y_%m_%d_%H")
xf = xf.replace(":", "_")
print(xf)

links = {
    "parking_space": {
        "lack": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_parking%5D%5B0%5D=brak",
        "belonging_on_street": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_parking%5D%5B0%5D=przynale%C5%BCne%20na%20ulicy",
        "guarded": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_parking%5D%5B0%5D=parking%20strze%C5%BCony",
        "garage": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_parking%5D%5B0%5D=w%20gara%C5%BCu",
        "parking_zone_identifier": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_parking%5D%5B0%5D=identyfikator",
    },
    "pets": {
        "yes": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_pets%5D%5B0%5D=Tak",
        "no": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_pets%5D%5B0%5D=Nie",
    },
    "elevator": {
        "yes": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_winda%5D%5B0%5D=Tak",
        "no": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_winda%5D%5B0%5D=Nie",
    },
    "dwelling_type": {
        "block": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=blok",
        "tenement": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=kamienica",
        "detached_house": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=wolnostojacy",
        "terraced_house": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=szeregowiec",
        "apartment": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=apartamentowiec",
        "loft": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=loft",
        "remaining": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_builttype%5D%5B0%5D=pozostale",
    },
    "rooms_number": {
        "1": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_rooms%5D%5B0%5D=one",
        "2": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_rooms%5D%5B0%5D=two",
        "3": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_rooms%5D%5B0%5D=three",
        "4+": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_rooms%5D%5B0%5D=four",
    },
    "level": {
        "basement": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_-1",
        "0": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_0",
        "1": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_1",
        "2": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_2",
        "3": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_3",
        "4": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_4",
        "5": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_5",
        "6": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_6",
        "7": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_7",
        "8": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_8",
        "9": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_9",
        "10": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_10",
        "10+": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_11",
        "attic": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_17",
    },
    "furnishing": {
        "yes": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_furniture%5D%5B0%5D=yes",
        "no": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bfilter_enum_furniture%5D%5B0%5D=no",
    },
    "seller": {
        "developers": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bprivate_business%5D=business",
        "private": "https://www.olx.pl/nieruchomosci/mieszkania/{type_of_agreement}/{city}/?search%5Bprivate_business%5D=private",
    },
}


# Scrap data for all, and for each category option
x=xf[:-3]
print("File names: ",x)
file_name = "./../data/data_scrapped/interim/prime_" + x + ".csv"

with open(file_name, "a", newline="") as f:
    writer = pd.DataFrame(
        columns=[
            "ID",
            "Link",
            "Title",
            "Price",
            "Location",
            "Area",
            "City",
            "Scrapping_Date",
            "Type_of_Agreement",
        ]
    )

for city in cities:

    for type_of_agreement in types_of_agreement:

        print("####", city, type_of_agreement)
        # link_base = links[category][options[0]] if category in links else None

        link_base = (
            "https://www.olx.pl/nieruchomosci/mieszkania/"
            + type_of_agreement
            + "/"
            + city
            + "/"
        )
        if not link_base:
            print(
                f"No link found for data in city {city} with agreement type {type_of_agreement}."
            )
            continue
        # Primary database scrapping (ID, link, city, area, date, title, price)
        parsed = urlparse(link_base)
        query_params = parse_qs(parsed.query)

        headers = {"User-Agent": "Mozilla/5.0"}

        page = 1
        MAX_PAGES = 20  # Set to 1 for testing, increase for full scraping
        base_info = []
        while True:
            query_params["page"] = [str(page)]
            new_query = urlencode(query_params, doseq=True)
            new_url = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    new_query,
                    parsed.fragment,
                )
            )
            print(f"Scraping {new_url} ...")
            response = requests.get(new_url, headers=headers)

            if response.status_code != 200:
                print(
                    f"Failed to fetch page {page}, status {response.status_code}. Stopping."
                )
                break

            soup = BeautifulSoup(response.text, "html.parser")
            offers = soup.select("div[data-cy='l-card']")

            if not offers or page > MAX_PAGES:
                print("No offers")
                break

            for offer in offers:
                title = offer.select_one("h4")
                price = offer.select_one("p.css-uj7mm0")
                location = offer.select_one("p.css-vbz67q")
                area = None
                area_elements = offer.find_all("span", class_="css-6as4g5")
                for elem in area_elements:
                    text = elem.get_text(strip=True)
                    if "m²" in text:
                        area = text
                        break

                link = offer.select_one("a")
                link = "https://www.olx.pl" + link["href"] if link else None

                idd = link.split("-")[-1].replace(".html", "") if link else None
                idd = str(idd)

                base_info.append(
                    {
                        "ID": idd.strip() if idd else None,
                        "Link": link.strip() if link else None,
                        "Title": title.text.strip() if title else None,
                        "Price": price.text.strip() if price else None,
                        "Location": location.text.strip() if location else None,
                        "Area": area.strip() if area else "None",
                        "City": city.strip() if city else None,
                        "Scrapping_Date": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "Type_of_Agreement": (
                            type_of_agreement.strip() if type_of_agreement else None
                        ),
                    }
                )
            page += 1
            with open(file_name, "a", newline="") as f:
                pd.DataFrame(base_info).to_csv(f, header=True, index=False)

categories = list(links.keys())
# Print the collected data
# Data by category
print("Cities:", cities)
print("Types of agreement:", types_of_agreement)
for city in cities:
    print("####", city)

    for type_of_agreement in types_of_agreement:
        print("####", city, type_of_agreement)
        print(f"Scraping data for city: {city}, type of agreement: {type_of_agreement}")
        for c in range(len(categories)):
            category_data = []
            print(categories[c])
            category = categories[c]
            category_all = links.get(category)
            options = list(category_all.keys())

            options_number = len(options)

            for i in range(options_number):
                print(options[i])

                links = {
                    "parking_space": {
                        "lack": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_parking%5D%5B0%5D=brak",
                        "belonging_on_street": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_parking%5D%5B0%5D=przynale%C5%BCne%20na%20ulicy",
                        "guarded": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_parking%5D%5B0%5D=parking%20strze%C5%BCony",
                        "garage": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_parking%5D%5B0%5D=w%20gara%C5%BCu",
                        "parking_zone_identifier": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_parking%5D%5B0%5D=identyfikator",
                    },
                    "pets": {
                        "yes": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_pets%5D%5B0%5D=Tak",
                        "no": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_pets%5D%5B0%5D=Nie",
                    },
                    "elevator": {
                        "yes": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_winda%5D%5B0%5D=Tak",
                        "no": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_winda%5D%5B0%5D=Nie",
                    },
                    "dwelling_type": {
                        "block": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=blok",
                        "tenement": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=kamienica",
                        "detached_house": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=wolnostojacy",
                        "terraced_house": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=szeregowiec",
                        "apartment": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=apartamentowiec",
                        "loft": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=loft",
                        "remaining": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_builttype%5D%5B0%5D=pozostale",
                    },
                    "rooms_number": {
                        "1": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_rooms%5D%5B0%5D=one",
                        "2": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_rooms%5D%5B0%5D=two",
                        "3": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_rooms%5D%5B0%5D=three",
                        "4+": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_rooms%5D%5B0%5D=four",
                    },
                    "level": {
                        "basement": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_-1",
                        "0": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_0",
                        "1": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_1",
                        "2": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_2",
                        "3": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_3",
                        "4": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_4",
                        "5": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_5",
                        "6": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_6",
                        "7": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_7",
                        "8": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_8",
                        "9": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_9",
                        "10": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_10",
                        "10+": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_11",
                        "attic": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_floor_select%5D%5B0%5D=floor_17",
                    },
                    "furnishing": {
                        "yes": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_furniture%5D%5B0%5D=yes",
                        "no": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bfilter_enum_furniture%5D%5B0%5D=no",
                    },
                    "seller": {
                        "developers": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bprivate_business%5D=business",
                        "private": "https://www.olx.pl/nieruchomosci/mieszkania/"
                        + type_of_agreement
                        + "/"
                        + city
                        + "/?search%5Bprivate_business%5D=private",
                    },
                }

                url = links[category][options[i]]
                print(url)
                parsed = urlparse(url)
                query_params = parse_qs(parsed.query)
                headers = {"User-Agent": "Mozilla/5.0"}
                page = 1
                MAX_PAGES = 20
                print(url)
                while True:
                    query_params["page"] = [str(page)]
                    new_query = urlencode(query_params, doseq=True)
                    new_url = urlunparse(
                        (
                            parsed.scheme,
                            parsed.netloc,
                            parsed.path,
                            parsed.params,
                            new_query,
                            parsed.fragment,
                        )
                    )
                    print(f"Scraping {new_url} ...")
                    response = requests.get(new_url, headers=headers)

                    if response.status_code != 200:
                        print(
                            f"Failed to fetch page {page}, status {response.status_code}. Stopping."
                        )
                        break

                    soup = BeautifulSoup(response.text, "html.parser")
                    offers = soup.select("div[data-cy='l-card']")

                    if not offers or page > MAX_PAGES:
                        print("No offers")
                        break

                    for offer in offers:
                        link = offer.select_one("a")
                        link = "https://www.olx.pl" + link["href"] if link else None
                        idd = link.split("-")[-1].replace(".html", "") if link else None
                        idd = str(idd)
                        category_new_data = options[i]
                        category_name = category.title()

                        category_data.append(
                            {
                                "Link": link.strip() if link else None,
                                category_name: category_new_data,
                            }
                        )
                    page += 1

                category_additional_data = pd.DataFrame(category_data)
                category_additional_data = category_additional_data.drop_duplicates(
                    subset=["Link"]
                )
                category_additional_data.to_csv(
                    "./../data/data_scrapped/interim/categories/"
                    + city
                    + "_"
                    + type_of_agreement
                    + "_"
                    + category
                    + "_"
                    + x
                    + ".csv",
                    index=False,
                )


# Flag file with date information for next scrapping records filtering
# This file will be used to filter out records that have already been scraped in the next run

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "a") as f:
    f.write("Flag file with data information for next scrapping records filtering\n")
    f.write(f"Last scrapping date:{xf}\n")
    f.write(f"{xf}\n")


# Concentration category by city


categories_path = "./../data/data_scrapped/interim/categories/"

for c in range(len(categories)):
    print(categories[c])
    category = categories[c]
    category_file = [f for f in os.listdir(categories_path) if category in f]
    print(len(category_file))
    all_data = []
    for file in category_file:
        file_path = os.path.join(categories_path, file)
        if os.path.getsize(file_path) > 0:
            try:
                dataframe = pd.read_csv(file_path, index_col=False)
                all_data.append(dataframe)
            except pd.errors.EmptyDataError:
                print(f"Skipped file with no columns/data: {file_path}")
        else:
            print(f"Skipped empty file: {file_path}")
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        combined_data.to_csv(
            "./../data/data_scrapped/interim/categories/combined/"
            + category
            + "_"
            + x
            + ".csv",
            index=False,
        )


# Data frames joining Merged data !!!

file_names = os.listdir("./../data/data_scrapped/interim/categories/combined/")
print("Additional info:", file_names)

base_data = pd.read_csv(
    "./../data/data_scrapped/interim/prime_" + x + ".csv", index_col=False
)
base_data = base_data.drop_duplicates(subset=["Link"])
print("Number of primary records:", len(base_data))

for file in file_names:
    additional_data = pd.read_csv(
        "./../data/data_scrapped/interim/categories/combined/" + file, index_col=False
    )
    print("---------------After removal of duplicates", len(additional_data))

    base_data = base_data.merge(additional_data, on="Link", how="left")
    base_data.to_csv("./../data/data_scrapped/reads/" + x + ".csv", index=False)

now = datetime.datetime.now()
now_now = now.strftime("%Y_%m_%d_%H")
scrapping_date=now

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "r") as f:
    lines = f.readlines()
    dates = [i for i,item in enumerate(lines) if "last scrapping date" in item.lower()]
    new_data_line=dates[1]
    new_data=lines[new_data_line+1]
    new_data=new_data[:-4]
    print(new_data) ############## Print the date for verification





# Final data cleaning and processing
dataframe = pd.read_csv("./../data/data_scrapped/reads/"+new_data+".csv")
dataframe = dataframe.drop_duplicates()
dataframe = dataframe.drop(
    columns=[
        "ID",
        "Title",
    ]
)


dataframe["Refreshed"]=dataframe["Location"].astype(str).apply(lambda x: True if "Od" in x else False)
dataframe[["Location", "Date"]] = dataframe["Location"].str.extract(r"^(.*?)(\d.*)$")
dataframe["Location"] = dataframe["Location"].str.split("-").str[0].str.strip()
dataframe["Price"] = dataframe["Price"].astype(str).str.replace(",",".",regex=False)
dataframe["Price"] = dataframe["Price"].str.split(" zł").str[0].str.strip()
dataframe["Negotiations"] = dataframe["Price"].str.contains("do", na=False)
dataframe["Price"] = dataframe["Price"].str.split(" złdo").str[0].str.strip()
dataframe["Price"] = dataframe["Price"].astype(str).str.replace(" zł", "", regex=False)

dataframe["Area"] = dataframe["Area"].str.split("m²").str[0].str.strip()
dataframe["Area"] = dataframe["Area"].astype(str).str.replace("m²", "", regex=False)
dataframe["Area"] = dataframe["Area"].astype(str).str.replace(",",".",regex=False)
dataframe["Area"]=pd.to_numeric(dataframe["Area"],errors="coerce")

mask3 = (
    dataframe["Type_of_Agreement"].isin(["sprzedaz"])
    & dataframe["Pets"].isnull()
)
dataframe.loc[mask3, "Pets"] = "yes"
dataframe["Pets"] = dataframe["Pets"].fillna("no")

mask4 = (
    dataframe["Type_of_Agreement"].isin(["sprzedaz"])
    & (dataframe["Parking_Space"].isnull()) & (dataframe["Dwelling_Type"].isin(["detached_house", "terraced_house"]))  
)

dataframe.loc[mask4, "Parking_Space"] = "yes"

dataframe["Parking_Space"] = dataframe["Parking_Space"].fillna("no")
dataframe["Parking_Space"] = (
    dataframe["Parking_Space"].astype(str).str.replace("lack", "no", regex=False)
)

dataframe["Furnishing"] = dataframe["Furnishing"].fillna("no")

mask = (
    dataframe["Dwelling_Type"].isin(["terraced_house", "detached_house"])
    & dataframe["Level"].isnull()
)
dataframe.loc[mask, "Level"] = "0"

mask_2 = (
    dataframe["Dwelling_Type"].isin(["terraced_house", "detached_house", "tenement"])
    & dataframe["Elevator"].isnull()
)
dataframe.loc[mask_2, "Elevator"] = "no"

dataframe = dataframe.dropna(subset=["Seller", "Dwelling_Type", "Rooms_Number"])

dataframe["Pets"]=dataframe["Pets"].str.lower().map({"yes":True,"no":False})
dataframe["Furnishing"]=dataframe["Furnishing"].str.lower().map({"yes":True,"no":False})
dataframe["Elevator"]=dataframe["Elevator"].str.lower().map({"yes":True,"no":False})
# Levels to numerics
dataframe["Level"] = dataframe["Level"].astype(str).str.replace("10+", "11")
dataframe["Level"] = dataframe["Level"].astype(str).str.replace("nan", "")
dataframe["Level"]=pd.to_numeric(dataframe["Level"],errors="coerce")
dataframe["Level"]=dataframe["Level"].fillna(-1).astype(int)
dataframe.loc[(dataframe["Level"] == -1) & (dataframe["Elevator"]==1),"Level"]=1
dataframe.loc[(dataframe["Level"]==-1)&(dataframe["Dwelling_Type"]=="tenement"),"Level"]=4
dataframe.loc[(dataframe["Level"]==-1),"Level"] = 4
print(dataframe["Level"].unique())


dataframe["Rooms_Number"]=dataframe["Rooms_Number"].str.replace("4+","4",regex=False)
dataframe["Rooms_Number"]=pd.to_numeric(dataframe["Rooms_Number"],errors="coerce")
print(dataframe["Rooms_Number"].unique())

# Hour to day of the scrapping
dataframe["Date"]=dataframe["Date"].astype(str).apply(lambda x: scrapping_date if ":" in x else x)
dataframe["Date"]=dataframe["Date"].apply(polish_to_eng)
dataframe["Date"]=pd.to_datetime(dataframe["Date"], dayfirst=True,errors='coerce').dt.date

# all builings in Poland, which have more than 4 levels need to have elevator

dataframe["Elevator"]=dataframe["Elevator"].fillna(-1).astype(int)
dataframe.loc[(dataframe["Level"]>4) &(dataframe["Elevator"]==-1),"Elevator"] =1
dataframe["Elevator"]=dataframe["Elevator"].replace(-1,0)

# Number of records count
count = dataframe.shape[0]

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "a") as f:
    f.write(f"Downloaded records count: {count}\n")

# Older record filtering

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "r") as f:
    lines = f.readlines()
    print(lines[2])
last_scrapping_date = lines[2].strip()
last_scrapping_date = last_scrapping_date[:-3].replace("_", "-")

print(last_scrapping_date)
# Convert last_scrapping_date to datetime.date for comparison
from datetime import datetime
last_scrapping_date_dt = datetime.strptime(last_scrapping_date, "%Y-%m-%d").date()
dataframe = dataframe[dataframe["Date"] >= last_scrapping_date_dt]
cities = dataframe["City"].unique()

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "a") as f:
    f.write(f"Records count after filtering: {dataframe.shape[0]}\n")
    for city in cities:
        count_city = dataframe[dataframe["City"] == city].shape[0]
        f.write(f"City: {city}, Records count: {count_city}\n")

# Email

now = datetime.now()
n = now.strftime("%Y-%m-%d %H:%M:%S")

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "r") as f:
    lines = f.readlines()
    subject = lines[1].strip(),lines[4].strip()
    subject_data = " ".join(subject)  # Join the subject lines into a single string
    body_data = "\n".join(lines[3:])  # Join the rest of the lines into the body of the email
    
    body_data =body_data, n
    body_data = "\n".join(body_data)  # Join the rest of the lines into the body of the email
    
    print(subject)  
    print(lines)

SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def get_gmail_credentials():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('cred.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def create_message(to_email, from_email, subject, body):
    message = MIMEText(body)
    message['to'] = to_email
    message['from'] = from_email
    message['subject'] = subject
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw_message}

def send_email_via_gmail_api(subject, body, to_email, from_email):
    creds = get_gmail_credentials()
    service = build('gmail', 'v1', credentials=creds)

    message = create_message(to_email, from_email, subject, body)
    sent_message = service.users().messages().send(userId='me', body=message).execute()
    print(f"Message sent! ID: {sent_message['id']}")

# Send email with the subject and body
send_email_via_gmail_api(
    subject=subject_data,
    body=body_data,
    to_email="oktawiusz.receiver@gmail.com",
    from_email="oktawiusz.sender@gmail.com"
)
dataframe.to_csv("./../data/data_scrapped/reads/"+new_data+"_cleaned.csv", index=False)
print(dataframe.sort_values(by="Price", ascending=False).head(10))


with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "r") as f:
    lines = f.readlines()
    dates = [i for i,item in enumerate(lines) if "last scrapping date" in item.lower()]
    new_data_line=dates[1]
    new_data=lines[new_data_line+1]
    new_data=new_data[:-4]
    print(new_data) ############## Print the date for verification



link = "./../data/data_scrapped/reads/"
file_name = new_data+"_cleaned.csv"  # Construct the file name using the date

data = pd.read_csv(link + file_name)
print(data.head())  # Print the first few rows of the DataFrame for verification
conn = sqlite3.connect("./../data/databases/main.db")
cursor = conn.cursor()
data.to_sql("apartment_data", conn, if_exists="append", index=False)
conn.close()

# Backup copy
today = new_data
os.makedirs("./../data/backup/" + today, exist_ok=True)
shutil.copytree("./../data/data_scrapped", "./../data/backup/" + today, dirs_exist_ok=True)
# Cleaning


def clean_folder_except(folder_path, exceptions):

    for filename in os.listdir(folder_path):
        # Check if the file is not in the exceptions list
        if filename not in exceptions:
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

clean_folder_except("./../data/data_scrapped/interim/categories/combined/",["x"])
clean_folder_except("./../data/data_scrapped/interim/categories/",["combined"])
clean_folder_except("./../data/data_scrapped/interim/",["flag_date_scrapping.txt","categories"])
clean_folder_except("./../data/data_scrapped/reads",["flag_date_scrapping.txt","categories"])

# flag file cleaning


with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "r") as f:
    lines = f.readlines()
    dates = [i for i,item in enumerate(lines) if "last scrapping date" in item.lower()]

    if len(dates) < 2:
        print("Not enough dates found in the file.")
        exit(1) 
    new_data_line=dates[1]-1
    new_data=lines[new_data_line]
    new_data=new_data[:-2]
    print("Remove after line:",lines[new_data_line])  # Print the date for verification

with open("./../data/data_scrapped/interim/flag_date_scrapping.txt", "w") as f:
    f.writelines(lines[new_data_line:])


