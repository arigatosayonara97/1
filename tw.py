import aiohttp
import asyncio
import logging

M3U_INPUT_URL = "https://github.com/LITUATUI/M3UPT/raw/fdbf3b5fb4728c0647b8918aa6048be2532bf987/M3U/M3UPT.m3u"
OUTPUT_FILE = "lista2.m3u"

MAX_CONCURRENT = 100
TIMEOUT = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class FastChecker:
    def __init__(self):
        self.timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def check(self, session, url):
        try:
            async with session.get(url, timeout=self.timeout) as r:
                if r.status == 200:
                    data = await r.content.read(1024)
                    if b"#EXT" in data or b".ts" in data:
                        return True
        except:
            pass
        return False


async def fetch_m3u(session):
    async with session.get(M3U_INPUT_URL) as r:
        r.raise_for_status()
        return await r.text()


def parse_m3u(content):
    entries = []
    extinf = None

    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            extinf = line
        elif line and not line.startswith("#") and extinf:
            entries.append((extinf, line))
            extinf = None

    return entries


async def main():
    checker = FastChecker()
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        logging.info("Baixando M3U...")
        content = await fetch_m3u(session)

        channels = parse_m3u(content)
        logging.info(f"Total de canais encontrados: {len(channels)}")

        working = []

        async def test_channel(extinf, url):
            async with checker.semaphore:
                ok = await checker.check(session, url)
                if ok:
                    logging.info(f"OK: {url}")
                    working.append((extinf, url))

        tasks = [test_channel(extinf, url) for extinf, url in channels]
        await asyncio.gather(*tasks)

    logging.info(f"Canais funcionando: {len(working)}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for extinf, url in working:
            f.write(extinf + "\n")
            f.write(url + "\n")

    logging.info(f"Arquivo gerado: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
