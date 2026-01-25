import requests
import logging
from logging.handlers import RotatingFileHandler
import json
import re
from bs4 import BeautifulSoup

# =======================
# CONFIGURAÇÕES
# =======================

REPO_URLS = [
    "https://github.com/iprtl/m3u/raw/b8507db8229defeda88512eaaf66bfe0e385e81c/Freetv.m3u",
]

TEMP_FILE = "lista_temp.m3u"
FINAL_FILE = "lista_final.m3u"
MAX_LINES = 212

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/118.0"
}

# =======================
# LOGGER
# =======================

logger = logging.getLogger("m3u_logger")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    handler = RotatingFileHandler("log.txt", maxBytes=1_000_000, backupCount=5)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# =======================
# FUNÇÕES AUXILIARES
# =======================

def extract_epg_url(line):
    match = re.search(r'url-tvg=["\']([^"\']+)["\']', line)
    return match.group(1) if match else None


def check_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        return r.status_code == 200
    except requests.RequestException:
        return False


def parse_extinf(line):
    return {
        "group": re.search(r'group-title="([^"]*)"', line),
        "tvg_id": re.search(r'tvg-id="([^"]*)"', line),
        "logo": re.search(r'tvg-logo="([^"]*)"', line),
        "name": line.split(",")[-1].strip()
    }


def search_google_images(query):
    try:
        url = f"https://www.google.com/search?q={query}&tbm=isch&hl=pt-BR"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        imgs = soup.find_all("img")
        if len(imgs) > 1:
            return imgs[1].get("src")
    except Exception:
        pass
    return None

# =======================
# ETAPA 1 – BAIXAR E UNIR
# =======================

lists = []
epg_urls = []
line_count = 0
wrote_header = False

for url in REPO_URLS:
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code == 200 and "#EXTM3U" in r.text:
        lists.append((url.split("/")[-1], r.text))

with open(TEMP_FILE, "w", encoding="utf-8") as f:
    for name, content in lists:
        lines = content.splitlines()

        if lines and lines[0].startswith("#EXTM3U") and not wrote_header:
            f.write(lines[0] + "\n")
            wrote_header = True

            epg = extract_epg_url(lines[0])
            if epg:
                epg_urls.append(epg)

        i = 1
        while i < len(lines) and line_count < MAX_LINES:
            f.write(lines[i] + "\n")
            line_count += 1
            i += 1

# =======================
# ETAPA 2 – PROCESSAR CANAIS
# =======================

channels = []

with open(TEMP_FILE, encoding="utf-8") as f:
    lines = f.readlines()

i = 0
while i < len(lines):
    line = lines[i].strip()

    if line.startswith("#EXTINF"):
        info = parse_extinf(line)

        group = info["group"].group(1) if info["group"] else "Undefined"
        tvg_id = info["tvg_id"].group(1) if info["tvg_id"] else ""
        logo = info["logo"].group(1) if info["logo"] else ""

        i += 1
        extras = []

        while i < len(lines) and lines[i].startswith("#"):
            extras.append(lines[i].strip())
            i += 1

        if i < len(lines):
            url = lines[i].strip()
            if check_url(url):
                if not logo:
                    logo = search_google_images(info["name"]) or "NoLogo.png"

                channels.append({
                    "name": info["name"],
                    "group": group,
                    "tvg_id": tvg_id,
                    "logo": logo,
                    "url": url,
                    "extras": extras
                })
    i += 1

# =======================
# ETAPA 3 – GERAR FINAL
# =======================

with open(FINAL_FILE, "w", encoding="utf-8") as f:
    f.write("#EXTM3U\n")

    for ch in channels:
        f.write(
            f'#EXTINF:-1 tvg-id="{ch["tvg_id"]}" '
            f'tvg-logo="{ch["logo"]}" '
            f'group-title="{ch["group"]}",{ch["name"]}\n'
        )
        for e in ch["extras"]:
            f.write(e + "\n")
        f.write(ch["url"] + "\n")

with open("playlist.json", "w", encoding="utf-8") as f:
    json.dump(channels, f, indent=2, ensure_ascii=False)

print("✅ Playlist criada com sucesso:", FINAL_FILE)
