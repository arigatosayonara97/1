import re
import requests
import xml.etree.ElementTree as ET
from time import sleep

# ================= CONFIGURAÇÃO =================
M3U_URL = "https://github.com/caliwyr/Software/raw/00c10301dc4a4b6ceba7eaebcc1c4171f17192f6/IPTV/lista1.m3u"
LOCAL_M3U = "listacomepg.m3u"
SLEEP_BETWEEN_DOWNLOADS = 3  # segundos para não sobrecarregar servidores
# =================================================

# 1️⃣ Baixar M3U original
print("Baixando M3U original...")
r = requests.get(M3U_URL)
r.raise_for_status()
m3u_content = r.text

# 2️⃣ Extrair URLs de EPG do cabeçalho #EXTM3U
epg_urls = []
header_match = re.search(r'(#EXTM3U.*)', m3u_content)
if header_match:
    header_line = header_match.group(1)
    # Extrai x-tvg-url e url-tvg
    epg_urls += re.findall(r'x-tvg-url="([^"]+)"', header_line)
    url_tvg_matches = re.findall(r'url-tvg="([^"]+)"', header_line)
    for match in url_tvg_matches:
        epg_urls += match.split(',')  # algumas vezes são múltiplas URLs separadas por vírgula

# 3️⃣ Baixar EPGs na memória
epg_data = {}
for url in epg_urls:
    print(f"Baixando EPG: {url}")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        epg_data[url] = r.text
        sleep(SLEEP_BETWEEN_DOWNLOADS)  # evita sobrecarregar
    except Exception as e:
        print(f"Falha ao baixar EPG {url}: {e}")

# 4️⃣ Criar mapa tvg-id -> nome do canal a partir dos EPGs
tvgid_to_title = {}
for url, xml_text in epg_data.items():
    try:
        root = ET.fromstring(xml_text)
        for channel in root.findall("channel"):
            tvgid = channel.attrib.get("id")
            display_name_elem = channel.find("display-name")
            if tvgid and display_name_elem is not None:
                tvgid_to_title[tvgid] = display_name_elem.text
    except ET.ParseError:
        print(f"Não foi possível parsear EPG {url}")

# 5️⃣ Corrigir M3U
new_lines = []
lines = m3u_content.splitlines()
for line in lines:
    if line.startswith("#EXTINF"):
        # Procurar tvg-id
        tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)
        if tvg_id_match:
            current_tvg = tvg_id_match.group(1)
            # Se vazio ou "N/A", tentar preencher
            if current_tvg.strip() in ("", "N/A"):
                channel_name_match = re.search(r',(.+)$', line)
                if channel_name_match:
                    channel_name = channel_name_match.group(1).strip()
                    # Busca no epg_data
                    best_tvg = None
                    for tid, title in tvgid_to_title.items():
                        if title.lower() == channel_name.lower():
                            best_tvg = tid
                            break
                    if best_tvg:
                        line = re.sub(r'tvg-id="[^"]*"', f'tvg-id="{best_tvg}"', line)
        else:
            # Se não houver tvg-id, tenta adicionar
            channel_name_match = re.search(r',(.+)$', line)
            if channel_name_match:
                channel_name = channel_name_match.group(1).strip()
                best_tvg = None
                for tid, title in tvgid_to_title.items():
                    if title.lower() == channel_name.lower():
                        best_tvg = tid
                        break
                if best_tvg:
                    line = line.replace("#EXTINF:-1", f'#EXTINF:-1 tvg-id="{best_tvg}"')
    new_lines.append(line)

# 6️⃣ Salvar M3U corrigido
with open(LOCAL_M3U, "w", encoding="utf-8") as f:
    f.write("\n".join(new_lines))

print(f"M3U corrigido salvo em {LOCAL_M3U}")
