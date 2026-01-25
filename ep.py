import re
import requests
import xml.etree.ElementTree as ET
from time import sleep

# ================= CONFIGURAÇÃO =================
M3U_URL = "https://github.com/caliwyr/Software/raw/00c10301dc4a4b6ceba7eaebcc1c4171f17192f6/IPTV/lista1.m3u"
LOCAL_M3U = "listacomepg.m3u"
SLEEP_BETWEEN_DOWNLOADS = 2  # segundos para não sobrecarregar servidores

# Cabeçalho para simular um navegador real (evita bloqueios 403)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
# =================================================

def download_file(url):
    """Faz o download com tratamento de erro e timeout."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Erro ao baixar {url}: {e}")
        return None

# 1️⃣ Baixar M3U original
print("Baixando M3U original...")
m3u_content = download_file(M3U_URL)
if not m3u_content:
    print("Falha crítica: não foi possível baixar o M3U original.")
    exit()

# 2️⃣ Extrair URLs de EPG do cabeçalho #EXTM3U
epg_urls = []
header_match = re.search(r'(#EXTM3U.*)', m3u_content)
if header_match:
    header_line = header_match.group(1)
    # Extrai x-tvg-url (formato mais comum)
    epg_urls += re.findall(r'x-tvg-url="([^"]+)"', header_line)
    # Extrai url-tvg (formato antigo/alternativo)
    url_tvg_matches = re.findall(r'url-tvg="([^"]+)"', header_line)
    for match in url_tvg_matches:
        epg_urls += match.split(',')  # separa se houver múltiplas URLs vírguladas

# Remove duplicatas mantendo a ordem
epg_urls = list(dict.fromkeys(epg_urls))
print(f"Encontrados {len(epg_urls)} links de EPG.")

# 3️⃣ Baixar EPGs e criar mapa de Nome -> ID (Otimizado)
print("Baixando e processando EPGs...")
name_to_id_map = {} # Mapa para busca rápida: nome_minusculo -> tvg_id

for url in epg_urls:
    if not url: continue
    print(f"  -> Processando EPG: {url}")
    xml_text = download_file(url)
    
    if not xml_text: continue

    try:
        root = ET.fromstring(xml_text)
        for channel in root.findall("channel"):
            tvg_id = channel.attrib.get("id")
            if tvg_id:
                # Pega todos os 'display-names' possíveis para aumentar a chance de match
                for dname in channel.findall("display-name"):
                    if dname.text:
                        clean_name = dname.text.strip()
                        # Salva mapeamento em minúsculo para facilitar comparação
                        name_to_id_map[clean_name.lower()] = tvg_id
    except ET.ParseError:
        print(f"     (Aviso: Não foi possível ler o XML deste link)")

    sleep(SLEEP_BETWEEN_DOWNLOADS)

print(f"Mapa de canais criado com {len(name_to_id_map)} entradas.")

# 4️⃣ Corrigir M3U
print("Corrigindo a lista M3U...")
new_lines = []
lines = m3u_content.splitlines()

for line in lines:
    if line.startswith("#EXTINF"):
        # 1. Extrair o nome do canal (parte após a vírgula)
        name_match = re.search(r',(.+)$', line)
        if not name_match:
            new_lines.append(line)
            continue
        
        channel_name = name_match.group(1).strip()
        lookup_name = channel_name.lower()
        
        # 2. Verificar se encontramos o tvg-id no mapa
        correct_tvg_id = name_to_id_map.get(lookup_name)
        
        if correct_tvg_id:
            # Verifica se já existe um tvg-id na linha
            existing_tvg_match = re.search(r'tvg-id="([^"]*)"', line)
            
            if existing_tvg_match:
                current_id = existing_tvg_match.group(1).strip()
                # Só substitui se estiver vazio, "N/A" ou indefinido
                if not current_id or current_id.upper() == "N/A":
                    line = re.sub(r'tvg-id="[^"]*"', f'tvg-id="{correct_tvg_id}"', line)
            else:
                # Se não existe o atributo tvg-id, precisamos inserir
                # Divide a linha entre cabeçalho (#EXTINF...) e nome do canal
                parts = line.split(',', 1)
                if len(parts) == 2:
                    header = parts[0]
                    # Insere o novo ID no cabeçalho
                    line = f'{header} tvg-id="{correct_tvg_id}",{parts[1]}'
        
        new_lines.append(line)
    else:
        new_lines.append(line)

# 5️⃣ Salvar M3U corrigido
try:
    with open(LOCAL_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(new_lines))
    print(f"Sucesso! Lista salva em {LOCAL_M3U}")
except IOError as e:
    print(f"Erro ao salvar arquivo: {e}")
