import os
import requests

# URLs dos repositórios que contêm os arquivos M3U
repo_urls = [
    "https://github.com/iprtl/m3u/raw/b8507db8229defeda88512eaaf66bfe0e385e81c/Freetv.m3u",
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
                # Escreve o cabeçalho completo com atributos, se presente
                f.write(lines[0].strip() + "\n")
                line_count += 1
                wrote_header = True
                
                # Extrai URL de EPG se presente
                epg_url = extract_epg_url(lines[0])
                if epg_url and epg_url not in epg_urls:
                    epg_urls.append(epg_url)
                    print(f"  URL de EPG encontrada: {epg_url}")
            start_idx = 1  # Pular esta linha nas próximas listas

        for i in range(start_idx, len(lines)):
            line = lines[i].strip()
            if not line:
                continue  # Ignorar linhas em branco

            # CORREÇÃO: Distinguir entre cabeçalhos simples e com atributos importantes
            if line.startswith("#EXTM3U"):
                if is_simple_extm3u_header(line):
                    # Ignora apenas cabeçalhos simples duplicados
                    continue
                else:
                    # Preserva cabeçalhos com atributos importantes (como url-tvg)
                    epg_url = extract_epg_url(line)
                    if epg_url and epg_url not in epg_urls:
                        epg_urls.append(epg_url)
                        print(f"  URL de EPG encontrada: {epg_url}")
                    
                    f.write(line + "\n")
                    line_count += 1
                    continue

            f.write(line + "\n")
            line_count += 1

            if line_count >= 212:
                print(f"Limite de 212 linhas atingido")
                break

        if line_count >= 212:
            break

print(f"\nArquivo {output_file} criado com {line_count} linhas")
print(f"URLs de EPG encontradas e preservadas:")
for epg_url in epg_urls:
    print(f"  - {epg_url}")

import os
import requests
import logging
from logging.handlers import RotatingFileHandler
import json
from bs4 import BeautifulSoup

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_file = "log.txt"
file_handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=5)
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Cabeçalho do arquivo M3U
banner = "#EXTM3U\n"

# Função para verificar URLs via requisição HTTP com o agente de usuário do Firefox
def check_url(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Firefox/89.0"
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)  # Usando GET para verificar a URL
        if response.status_code == 200:
            logger.info("URL OK: %s", url)
            return True
        else:
            logger.warning("URL Error %s: Status Code %d", url, response.status_code)
            return False
    except requests.exceptions.RequestException as e:
        logger.error("Request Error %s: %s", url, str(e))
        return False

# Função para processar uma linha #EXTINF
def parse_extinf_line(line):
    group_title = "Undefined"
    tvg_id = "Undefined"
    tvg_logo = "Undefined.png"
    ch_name = "Undefined"
    
    if 'group-title="' in line:
        group_title = line.split('group-title="')[1].split('"')[0]
    if 'tvg-id="' in line:
        tvg_id = line.split('tvg-id="')[1].split('"')[0]
    if 'tvg-logo="' in line:
        tvg_logo = line.split('tvg-logo="')[1].split('"')[0]
    if ',' in line:
        ch_name = line.split(',')[-1].strip()
    
    return ch_name, group_title, tvg_id, tvg_logo

# Função principal para processar o arquivo de entrada
def process_m3u_file(input_file, output_file):
    with open(input_file) as f:
        lines = f.readlines()

    channel_data = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        if line.startswith('#EXTINF'):
            ch_name, group_title, tvg_id, tvg_logo = parse_extinf_line(line)
            extra_lines = []
            link = None
            
            # Procura pela URL e ignora linhas intermediárias (#EXTVLCOPT, #KODIPROP, etc.)
            while i + 1 < len(lines):
                i += 1
                next_line = lines[i].strip()
                if next_line.startswith('#'):  # Verifica se a linha começa com '#'
                    extra_lines.append(next_line)  # Armazena a linha extra
                else:
                    link = next_line  # Caso contrário, é a URL do canal
                    break
            
            # Verifica a URL antes de adicionar
            if link and check_url(link):
                # Se o canal não tiver logotipo, buscar o logo automaticamente
                if tvg_logo in ["", "N/A", "Undefined.png"]:  # Condição para logo vazio ou "N/A"
                    logo_url = search_google_images(ch_name)
                    if logo_url:
                        tvg_logo = logo_url
                    else:
                        tvg_logo = "NoLogoFound.png"  # Caso não encontre logo
                
                channel_data.append({
                    'name': ch_name,
                    'group': group_title,
                    'tvg_id': tvg_id,
                    'logo': tvg_logo,
                    'url': link,
                    'extra': extra_lines
                })
        i += 1

    # Gera o arquivo de saída M3U
    with open(output_file, "w") as f:  # Certifique-se de usar "w" e não "a" para sobrescrever
        f.write(banner)
        for channel in channel_data:
            extinf_line = (
                f'#EXTINF:-1 group-title="{channel["group"]}" '
                f'tvg-id="{channel["tvg_id"]}" '
                f'tvg-logo="{channel["logo"]}",{channel["name"]}'
            )
            f.write(extinf_line + '\n')
            for extra in channel['extra']:
                f.write(extra + '\n')
            f.write(channel['url'] + '\n')

    # Salva os dados em JSON para análise posterior
    with open("playlist.json", "w") as f:
        json.dump(channel_data, f, indent=2)

# Função para buscar imagem no Google
def search_google_images(query):
    search_url = f"https://www.google.com/search?hl=pt-BR&q={query}&tbm=isch"  # URL de busca de imagens
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    
    try:
        response = requests.get(search_url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        # Buscar a primeira imagem
        img_tags = soup.find_all("img")
        if img_tags:
            # A primeira imagem no Google geralmente é a mais relevante
            img_url = img_tags[1]['src']  # O primeiro item é o logo do Google
            return img_url
    except Exception as e:
        logger.error("Error searching Google images: %s", e)
    
    return None

# Configuração dos arquivos de entrada e saída
input_file = "lista1.M3U"
output_file = "lista1.M3U"

# Executa o processamento
process_m3u_file(input_file, output_file)
