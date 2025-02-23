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

def get_with_retry(url, headers, max_retries=5, backoff_factor=1):
    """Fetch URL with retries and exponential backoff."""
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response
            else:
                print(f"Got status code {response.status_code} for URL: {url}")
        except Exception as e:
            print(f"Error fetching {url}: {e} (attempt {attempt+1}/{max_retries})")
        sleep_time = backoff_factor * (2 ** attempt)
        print(f"Retrying in {sleep_time} seconds...")
        time.sleep(sleep_time)
        attempt += 1
    return None

def parse_timestamp(timestamp_str):
    """
    Parses a timestamp string of the form "Mar 18, 2024 5:58PM EDT" or "Feb 12, 2025 8:43am EST"
    and returns an ISO formatted string in New York time.
    """
    # Ensure the am/pm portion is uppercase.
    timestamp_str = re.sub(
        r'(\d{1,2}:\d{2})(am|pm)',
        lambda m: m.group(1) + m.group(2).upper(),
        timestamp_str,
        flags=re.IGNORECASE
    )
    # Expected pattern: "Mar 18, 2024 5:58PM EDT"
    pattern = r'^(?P<dt_part>[A-Za-z]{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}[AP]M)\s+(?P<tz>EDT|EST)$'
    match = re.match(pattern, timestamp_str)
    if not match:
        raise ValueError("Timestamp does not match expected format")
    dt_part = match.group('dt_part')
    tz_str = match.group('tz').upper()
    dt = datetime.strptime(dt_part, "%b %d, %Y %I:%M%p")
    eastern = pytz.timezone("America/New_York")
    if tz_str == "EDT":
        dt_eastern = eastern.localize(dt, is_dst=True)
    elif tz_str == "EST":
        dt_eastern = eastern.localize(dt, is_dst=False)
    else:
        raise ValueError("Unexpected timezone")
    return dt_eastern.isoformat()

def scrape_press_releases(ticker, output_dir):
    # Use lowercase for API requests and uppercase for output directory
    ticker_api = ticker.lower()
    ticker_output = ticker.upper()

    # Construct the API URL using the ticker in lowercase.
    api_url = f"https://www.nasdaq.com/api/news/topic/press_release?q=symbol:{ticker_api}|assetclass:stocks&limit=100"

    # Use a Google Chrome user agent
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/115.0.0.0 Safari/537.36")
    }

    print(f"Fetching API data from: {api_url}")
    api_response = get_with_retry(api_url, headers)
    if not api_response:
        print(f"Failed to fetch API data after retries: {api_url}")
        return

    try:
        api_data = api_response.json()
    except Exception as e:
        print("Error parsing API JSON:", e)
        return

    rows = api_data.get("data", {}).get("rows", [])
    print(f"Found {len(rows)} press releases for ticker {ticker} from API")

    # Create output directory for the ticker using the uppercase ticker.
    ticker_dir = os.path.join(output_dir, ticker_output)
    os.makedirs(ticker_dir, exist_ok=True)

    # Process each press release row from the API response.
    for row in rows:
        title_from_api = row.get("title", "No Title")
        relative_url = row.get("url")
        if not relative_url:
            print("No URL found for press release:", title_from_api)
            continue

        # Construct file path; if the JSON already exists, skip this press release.
        slug = relative_url.rstrip("/").split("/")[-1]
        file_path = os.path.join(ticker_dir, f"{slug}.json")
        if os.path.exists(file_path):
            print(f"File {file_path} already exists. Skipping...")
            continue

        # Construct full URL from relative path.
        pr_url = "https://www.nasdaq.com" + relative_url
        print(f"\nScraping press release page: {pr_url}")
        pr_response = get_with_retry(pr_url, headers)
        if not pr_response:
            print(f"Failed to load press release after retries: {pr_url}")
            continue

        pr_soup = BeautifulSoup(pr_response.text, "html.parser")

        # Extract the timestamp from <time class="timestamp__date">
        time_element = pr_soup.find("time", class_="timestamp__date")
        if time_element:
            timestamp_str = time_element.get_text(strip=True)
            try:
                timestamp_iso = parse_timestamp(timestamp_str)
            except Exception as e:
                print(f"Error parsing timestamp '{timestamp_str}': {e}")
                # Fallback: save the original timestamp string if parsing fails.
                timestamp_iso = timestamp_str
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
        description="Scrape Nasdaq press releases using the Nasdaq API for given tickers."
    )
    parser.add_argument("--output-dir", default="news", help="Output directory for JSON files")
    parser.add_argument("tickers", nargs="+", help="Ticker symbol(s) (e.g., AMD NVDA)")
    args = parser.parse_args()

    for ticker in args.tickers:
        print(f"\nProcessing ticker: {ticker}")
        scrape_press_releases(ticker, args.output_dir)
        # Optional: delay between processing tickers.
        delay = random.randint(5, 15)
        print(f"Waiting {delay} seconds before processing next ticker...")
        time.sleep(delay)

