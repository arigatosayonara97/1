import aiohttp
import asyncio
import json
import os
import m3u8
from tqdm import tqdm
from aiohttp import ClientTimeout, TCPConnector
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor
import time
from pathlib import Path
import logging
import sys
from fuzzywuzzy import fuzz
import shutil
from difflib import SequenceMatcher
import html
from datetime import date, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URLs
CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL = os.getenv("STREAMS_URL", "https://iptv-org.github.io/api/streams.json")
LOGOS_URL = os.getenv("LOGOS_URL", "https://iptv-org.github.io/api/logos.json")
KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"
ADDITIONAL_M3U = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u"
]

# File paths
WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"

# Settings
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 100))
INITIAL_TIMEOUT = 20
MAX_TIMEOUT = 30
RETRIES = 2
SCRAPER_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv']

def channels_to_m3u(channels):
    """Convert a list of channel dictionaries to M3U format string."""
    m3u_lines = ["#EXTM3U"]
    for ch in channels:
        name = ch.get("name", "Unknown")
        logo = ch.get("logo", "")
        url = ch.get("url", "")
        ch_id = ch.get("id", "")
        group = ",".join(ch.get("categories", ["General"]))
        inf_line = f'#EXTINF:-1 tvg-id="{ch_id}" tvg-logo="{logo}" group-title="{group}",{name}'
        m3u_lines.append(inf_line)
        m3u_lines.append(url)
    return "\n".join(m3u_lines)

def parse_m3u_to_list(m3u_content):
    """Parse M3U content back to list of dicts."""
    channels = []
    current_ch = {}
    for line in m3u_content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            tvg_id = re.search(r'tvg-id="([^"]*)"', line)
            tvg_logo = re.search(r'tvg-logo="([^"]*)"', line)
            group_title = re.search(r'group-title="([^"]*)"', line)
            name = line.split(",")[-1]
            current_ch = {
                "id": tvg_id.group(1) if tvg_id else "",
                "logo": tvg_logo.group(1) if tvg_logo else "",
                "categories": group_title.group(1).split(",") if group_title else ["General"],
                "name": name
            }
        elif line and not line.startswith("#"):
            if current_ch:
                current_ch["url"] = line
                channels.append(current_ch)
                current_ch = {}
    return channels

def save_m3u_file(base_name, channels):
    if not channels: return
    path = f"{base_name}.m3u"
    with open(path, 'w', encoding='utf-8') as f:
        f.write(channels_to_m3u(channels))

def load_m3u_file(base_name):
    path = f"{base_name}.m3u"
    if not os.path.exists(path): return []
    with open(path, 'r', encoding='utf-8') as f:
        return parse_m3u_to_list(f.read())

def remove_duplicates(channels):
    seen_urls = set()
    seen_ids = set()
    unique = []
    for ch in channels:
        url, cid = ch.get("url"), ch.get("id")
        if url and cid and url not in seen_urls and cid not in seen_ids:
            seen_urls.add(url)
            seen_ids.add(cid)
            unique.append(ch)
    return unique

def clear_directories():
    for dir_path in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)
    if os.path.exists(f"{WORKING_CHANNELS_BASE}.m3u"):
        os.remove(f"{WORKING_CHANNELS_BASE}.m3u")

def save_channels(channels, append=False):
    if not append: clear_directories()
    
    channels = remove_duplicates(channels)
    if append:
        existing = load_m3u_file(WORKING_CHANNELS_BASE)
        channels = remove_duplicates(existing + channels)
    
    save_m3u_file(WORKING_CHANNELS_BASE, channels)

    country_map = {}
    category_map = {}
    for ch in channels:
        country = ch.get("country", "Unknown")
        country_map.setdefault(country, []).append(ch)
        for cat in ch.get("categories", ["General"]):
            category_map.setdefault(cat, []).append(ch)

    for country, chs in country_map.items():
        safe = "".join(c for c in country if c.isalnum() or c in (' ', '_', '-')).strip()
        save_m3u_file(os.path.join(COUNTRIES_DIR, safe), chs)

    for cat, chs in category_map.items():
        safe = "".join(c for c in cat if c.isalnum() or c in (' ', '_', '-')).strip()
        save_m3u_file(os.path.join(CATEGORIES_DIR, safe), chs)

async def fetch_json(session, url):
    try:
        async with session.get(url) as response:
            return await response.json()
    except: return []

class FastChecker:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT)

    async def check_url(self, session, url):
        if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS): return False
        try:
            async with session.get(url, timeout=self.timeout, allow_redirects=True) as response:
                return response.status == 200
        except: return False

async def main():
    logging.info("Starting IPTV Scraper (M3U Output)...")
    checker = FastChecker()
    async with aiohttp.ClientSession(connector=TCPConnector(limit=MAX_CONCURRENT, ssl=False)) as session:
        logos_data = await fetch_json(session, LOGOS_URL)
        logos_dict = {l["channel"]: l["url"] for l in logos_data if l.get("channel")}
        
        # IPTV-org logic
        channels_data = await fetch_json(session, CHANNELS_URL)
        streams_data = await fetch_json(session, STREAMS_URL)
        streams_dict = {s["channel"]: s for s in streams_data if s.get("channel")}
        
        working_channels = []
        # Limit processing for efficiency in this example
        to_process = channels_data[:500] 

        async def process_one(ch):
            ch_id = ch.get("id")
            if ch_id in streams_dict:
                url = streams_dict[ch_id]["url"]
                async with checker.semaphore:
                    if await checker.check_url(session, url):
                        working_channels.append({
                            "name": ch.get("name", "Unknown"),
                            "id": ch_id,
                            "logo": logos_dict.get(ch_id, ""),
                            "url": url,
                            "categories": ch.get("categories", ["General"]),
                            "country": ch.get("country", "Unknown")
                        })

        logging.info(f"Checking {len(to_process)} channels...")
        await asyncio.gather(*[process_one(ch) for ch in to_process])
        
        save_channels(working_channels)
        logging.info(f"Process completed. Found {len(working_channels)} channels.")

if __name__ == "__main__":
    asyncio.run(main())
