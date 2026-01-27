import os
import requests
#https://github.com/iprtl/m3u/raw/b8507db8229defeda88512eaaf66bfe0e385e81c/Freetv.m3u
# URLs dos repositórios que contêm os arquivos M3U
repo_urls = [
    "https://github.com/gratinomaster/1/raw/refs/heads/main/lista_10.M3U",
]

lists = []

# Buscar arquivos M3U de cada URL
for url in repo_urls:
    print(f"Processando URL: {url}")
    try:
        response = requests.get(url, allow_redirects=True)

        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            
            if url.lower().endswith(('.m3u', '.m3u8')) or '#EXTM3U' in response.text:
                print(f"  Detectado arquivo M3U direto: {url}")
                filename = url.split("/")[-1]
                lists.append((filename, response.text))
            elif 'application/json' in content_type:
                try:
                    contents = response.json()
                    print(f"  Processando resposta JSON com {len(contents)} itens")
                    m3u_files = [content for content in contents if content.get("name", "").lower().endswith(('.m3u', '.m3u8'))]

                    for m3u_file in m3u_files:
                        m3u_url = m3u_file["download_url"]
                        print(f"  Baixando arquivo M3U: {m3u_url}")
                        m3u_response = requests.get(m3u_url, allow_redirects=True)
                        if m3u_response.status_code == 200:
                            lists.append((m3u_file["name"], m3u_response.text))
                except ValueError:
                    print(f"  Erro ao processar JSON de {url}, tratando como arquivo M3U direto")
                    filename = url.split("/")[-1]
                    lists.append((filename, response.text))
            else:
                if '#EXTM3U' in response.text:
                    print(f"  Conteúdo detectado como M3U pelo cabeçalho #EXTM3U")
                    filename = url.split("/")[-1]
                    lists.append((filename, response.text))
                else:
                    print(f"  Tipo de conteúdo não reconhecido: {content_type}")
        else:
            print(f"  Erro ao acessar URL: {url}, código de status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"  Erro ao processar URL {url}: {e}")

# Ordenação dos arquivos M3U pelo nome
lists = sorted(lists, key=lambda x: x[0])

print(f"\nTotal de listas M3U encontradas: {len(lists)}")
for name, _ in lists:
    print(f"  - {name}")

# Limitação das linhas a serem escritas no arquivo final
line_count = 0
output_file = "lista1.M3U"
wrote_header = False  # Para garantir que só escreva uma vez o cabeçalho
epg_urls = []  # Lista para armazenar URLs de EPG encontradas

def extract_epg_url(extm3u_line):
    """Extrai a URL de EPG de uma linha #EXTM3U se presente"""
    if 'url-tvg=' in extm3u_line:
        # Procura por url-tvg="..." ou url-tvg='...'
        import re
        match = re.search(r'url-tvg=["\']([^"\']+)["\']', extm3u_line)
        if match:
            return match.group(1)
    return None

def is_simple_extm3u_header(line):
    """Verifica se é um cabeçalho #EXTM3U simples (sem atributos importantes)"""
    line = line.strip()
    if not line.startswith("#EXTM3U"):
        return False
    
    # Se contém apenas #EXTM3U ou #EXTM3U com espaços, é simples
    if line == "#EXTM3U" or line.replace("#EXTM3U", "").strip() == "":
        return True
    
    # Se contém atributos importantes como url-tvg, não é simples
    important_attributes = ['url-tvg=', 'tvg-url=', 'x-tvg-url=']
    for attr in important_attributes:
        if attr in line.lower():
            return False
    
    return True

with open(output_file, "w") as f:
    for list_name, list_content in lists:
        print(f"Processando lista: {list_name}")
        lines = list_content.split("\n")

        start_idx = 0

        # Verifica se a primeira linha é um cabeçalho #EXTM3U
        if lines and lines[0].strip().startswith("#EXTM3U"):
            if not wrote_header:
                f.write(lines[0].strip() + "\n")
                line_count += 1
                wrote_header = True

                epg_url = extract_epg_url(lines[0])
                if epg_url and epg_url not in epg_urls:
                    epg_urls.append(epg_url)
                    print(f"  URL de EPG encontrada: {epg_url}")

            start_idx = 1

        for i in range(start_idx, len(lines)):
            line = lines[i].strip()
            if not line:
                continue

            if line.startswith("#EXTM3U"):
                if is_simple_extm3u_header(line):
                    continue
                else:
                    epg_url = extract_epg_url(line)
                    if epg_url and epg_url not in epg_urls:
                        epg_urls.append(epg_url)
                        print(f"  URL de EPG encontrada: {epg_url}")

                    f.write(line + "\n")
                    line_count += 1
                    continue

            f.write(line + "\n")
            line_count += 1


print(f"\nArquivo {output_file} criado com {line_count} linhas")
print(f"URLs de EPG encontradas e preservadas:")
for epg_url in epg_urls:
    print(f"  - {epg_url}")




import requests
import logging
from logging.handlers import RotatingFileHandler
import json
from bs4 import BeautifulSoup
import re

# =========================
# CONFIGURAÇÃO DE LOG
# =========================
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_file = "log.txt"
file_handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=5)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)
logger.addHandler(file_handler)

# =========================
# FUNÇÕES AUXILIARES
# =========================
def check_url(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        return r.status_code == 200
    except requests.exceptions.RequestException:
        return False

def parse_extinf_line(line):
    group = re.search(r'group-title="([^"]+)"', line)
    tvg_id = re.search(r'tvg-id="([^"]+)"', line)
    tvg_logo = re.search(r'tvg-logo="([^"]+)"', line)

    name = line.split(",")[-1].strip()

    return (
        name,
        group.group(1) if group else "Undefined",
        tvg_id.group(1) if tvg_id else "Undefined",
        tvg_logo.group(1) if tvg_logo else "Undefined.png"
    )

def search_google_images(query):
    url = f"https://www.google.com/search?hl=pt-BR&q={query}&tbm=isch"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")
        imgs = soup.find_all("img")
        if len(imgs) > 1:
            return imgs[1]["src"]
    except Exception as e:
        logger.error(f"Erro ao buscar imagem: {e}")

    return None

# =========================
# PROCESSAMENTO FINAL
# =========================
def process_m3u_file(input_file, output_file):
    with open(input_file, encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    extm3u_headers = []
    channels = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("#EXTM3U"):
            if line not in extm3u_headers:
                extm3u_headers.append(line)
            i += 1
            continue

        if line.startswith("#EXTINF"):
            name, group, tvg_id, logo = parse_extinf_line(line)
            extras = []
            link = None

            while i + 1 < len(lines):
                i += 1
                nxt = lines[i].strip()
                if nxt.startswith("#"):
                    extras.append(nxt)
                else:
                    link = nxt
                    break

            if link and check_url(link):
                if logo in ["Undefined.png", "", "N/A"]:
                    found_logo = search_google_images(name)
                    logo = found_logo if found_logo else logo

                channels.append({
                    "name": name,
                    "group": group,
                    "tvg_id": tvg_id,
                    "logo": logo,
                    "url": link,
                    "extra": extras
                })

        i += 1

    with open(output_file, "w", encoding="utf-8") as f:
        for h in extm3u_headers:
            f.write(h + "\n")

        if not extm3u_headers:
            f.write("#EXTM3U\n")

        for ch in channels:
            f.write(
                f'#EXTINF:-1 group-title="{ch["group"]}" '
                f'tvg-id="{ch["tvg_id"]}" '
                f'tvg-logo="{ch["logo"]}",{ch["name"]}\n'
            )
            for e in ch["extra"]:
                f.write(e + "\n")
            f.write(ch["url"] + "\n")

    with open("playlist.json", "w", encoding="utf-8") as f:
        json.dump(channels, f, indent=2, ensure_ascii=False)

# =========================
# EXECUÇÃO
# =========================
process_m3u_file("lista1.M3U", "lista1.M3U")

print("Processamento concluído ✔")



