#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import re
import os
import json
import time
import random
from datetime import datetime
import pytz

def scrape_press_releases(ticker, output_dir):
    # Convert ticker to lower case (e.g., AMD -> "amd")
    ticker_lower = ticker.lower()
    # Construct the API URL using the ticker lower-case.
    api_url = f"https://www.nasdaq.com/api/news/topic/press_release?q=symbol:{ticker_lower}|assetclass:stocks&limit=100"

    # Use a Google Chrome user agent
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/115.0.0.0 Safari/537.36")
    }

    print(f"Fetching API data from: {api_url}")
    response = requests.get(api_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch API data: {api_url}")
        return

    try:
        api_data = response.json()
    except Exception as e:
        print("Error parsing API JSON:", e)
        return

    rows = api_data.get("data", {}).get("rows", [])
    print(f"Found {len(rows)} press releases from API")

    # Create output directory for the ticker
    ticker_dir = os.path.join(output_dir, ticker_lower)
    os.makedirs(ticker_dir, exist_ok=True)

    # Process each press release row from the API response.
    for row in rows:
        title_from_api = row.get("title", "No Title")
        relative_url = row.get("url")
        if not relative_url:
            print("No URL found for press release:", title_from_api)
            continue
        # Construct full URL from relative path.
        pr_url = "https://www.nasdaq.com" + relative_url

        print(f"\nScraping press release page: {pr_url}")
        pr_response = requests.get(pr_url, headers=headers)
        if pr_response.status_code != 200:
            print(f"Failed to load press release: {pr_url}")
            continue

        pr_soup = BeautifulSoup(pr_response.text, "html.parser")

        # Extract the timestamp from <time class="timestamp__date">
        time_element = pr_soup.find("time", class_="timestamp__date")
        if time_element:
            timestamp_str = time_element.get_text(strip=True)
            # Remove "EST" and clean up the time string.
            timestamp_clean = timestamp_str.replace("EST", "").strip()
            # Ensure the time indicator (am/pm) is uppercase.
            timestamp_clean = re.sub(r'(\d{1,2}:\d{2})(am|pm)', lambda m: m.group(1) + m.group(2).upper(), timestamp_clean)
            try:
                dt = datetime.strptime(timestamp_clean, "%b %d, %Y %I:%M%p")
                eastern = pytz.timezone("America/New_York")
                dt_eastern = eastern.localize(dt)
                timestamp_iso = dt_eastern.isoformat()
            except Exception as e:
                print(f"Error parsing timestamp '{timestamp_clean}': {e}")
                timestamp_iso = None
        else:
            print("Timestamp <time class='timestamp__date'> not found.")
            timestamp_iso = None

        # Extract only the text from the <div class="body__content"> element.
        content_div = pr_soup.find("div", class_="body__content")
        if content_div:
            content = content_div.get_text(separator="\n", strip=True)
        else:
            print('Warning: <div class="body__content"> not found. Using empty content.')
            content = ""

        # Build the output JSON data using the API title.
        data = {
            "ticker": ticker,
            "timestamp": timestamp_iso,
            "title": title_from_api,
            "text": content
        }

        # Determine the file name from the API URL slug.
        slug = relative_url.rstrip("/").split("/")[-1]
        file_path = os.path.join(ticker_dir, f"{slug}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"Saved press release to {file_path}")

        # Wait a random delay of 5-15 seconds before fetching the next press release.
        delay = random.randint(5, 15)
        print(f"Waiting {delay} seconds before next press release...")
        time.sleep(delay)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Scrape Nasdaq press releases using the Nasdaq API for a given ticker."
    )
    parser.add_argument("ticker", help="Ticker symbol (e.g., AMD)")
    parser.add_argument("output_dir", help="Output directory for JSON files")
    args = parser.parse_args()
    scrape_press_releases(args.ticker, args.output_dir)

