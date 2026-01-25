import requests
import re

M3U_URL = "https://github.com/caliwyr/Software/raw/00c10301dc4a4b6ceba7eaebcc1c4171f17192f6/IPTV/lista1.m3u"
OUTPUT_FILE = "listacomepg.m3u"


# -------------------------
# HELPERS
# -------------------------
def normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"\(.*?\)", "", name)     # remove (HD), (FHD), etc
    name = re.sub(r"[^a-z0-9]", "", name)   # remove símbolos
    return name


def generate_tvg_id(name: str) -> str:
    return normalize_name(name)


def is_valid_tvg_id(tvg_id: str) -> bool:
    if not tvg_id:
        return False
    tvg_id = tvg_id.strip().lower()
    return tvg_id not in ("", "n/a", "na", "null", "none")


def normalize_extinf_line(line: str) -> str:
    """
    Corrige EXTINF malformado:
    "...png"group-title="HOME" -> "...png" group-title="HOME"
    """
    line = re.sub(r'"(?=\w)', '" ', line)
    return line


def parse_extinf(line: str):
    attrs = dict(re.findall(r'(\S+?)="(.*?)"', line))
    name = line.split(",", 1)[-1].strip()
    return attrs, name


# -------------------------
# MAIN
# -------------------------
def main():
    print("⬇️  Baixando playlist...")
    resp = requests.get(M3U_URL, timeout=30)
    resp.raise_for_status()

    lines = resp.text.splitlines()

    # ------------------------------------------------
    # 1️⃣ PRIMEIRO PASSE: descobrir MELHOR tvg-id
    # ------------------------------------------------
    best_tvg_ids = {}  # normalized_name -> tvg-id

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("#EXTINF"):
            line = normalize_extinf_line(line)
            attrs, name = parse_extinf(line)
            norm_name = normalize_name(name)

            tvg_id = attrs.get("tvg-id", "")

            if is_valid_tvg_id(tvg_id):
                if (
                    norm_name not in best_tvg_ids
                    or len(tvg_id) > len(best_tvg_ids[norm_name])
                ):
                    best_tvg_ids[norm_name] = tvg_id

            i += 2  # EXTINF + URL
        else:
            i += 1

    # ------------------------------------------------
    # 2️⃣ SEGUNDO PASSE: reescrever playlist
    # ------------------------------------------------
    print("🛠️  Corrigindo tvg-id...")
    output_lines = []
    i = 0

    while i < len(lines):
        line = lines[i].rstrip()

        if line.strip().startswith("#EXTINF"):
            original_line = line
            line = normalize_extinf_line(line)

            attrs, name = parse_extinf(line)
            norm_name = normalize_name(name)

            # decide tvg-id final
            if norm_name in best_tvg_ids:
                final_tvg_id = best_tvg_ids[norm_name]
            else:
                final_tvg_id = generate_tvg_id(name)

            # remove QUALQUER tvg-id existente (inclusive vazio)
            line = re.sub(r'\s*tvg-id=".*?"', "", line)

            # injeta tvg-id logo após EXTINF:-1
            line = line.replace(
                "#EXTINF:-1",
                f'#EXTINF:-1 tvg-id="{final_tvg_id}"',
                1,
            )

            output_lines.append(line)
            # preserva URL original
            if i + 1 < len(lines):
                output_lines.append(lines[i + 1])

            i += 2
        else:
            # comentários, #EXTM3U, #####, etc
            output_lines.append(line)
            i += 1

    # ------------------------------------------------
    # 3️⃣ SALVAR
    # ------------------------------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))

    print(f"✅ Playlist final salva em: {OUTPUT_FILE}")
    print(f"📺 Canais com tvg-id resolvido: {len(best_tvg_ids)}")


if __name__ == "__main__":
    main()
