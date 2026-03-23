import requests
from playwright.sync_api import sync_playwright

# ===== ТВОИ ДАННЫЕ =====
TELEGRAM_TOKEN = "8653069356:AAHagjetWZYbr1-_h9474WNMcjCWuTmJc9Q"
CHAT_ID = "1064307495"

URL = "https://www.olx.pl/nieruchomosci/mieszkania/sprzedaz/wroclaw/?search%5Bfilter_float_price%3Afrom%5D=500000&search%5Bfilter_float_price%3Ato%5D=650000&search%5Bfilter_enum_rooms%5D%5B0%5D=three&search%5Bfilter_enum_rooms%5D%5B1%5D=four&search%5Bfilter_float_m%3Afrom%5D=40&search%5Bfilter_float_m%3Ato%5D=60"

# ===== TELEGRAM =====
def send(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": msg
    })

# ===== ЗАГРУЗКА УЖЕ ОТПРАВЛЕННЫХ =====
def load_seen():
    try:
        with open("seen.txt") as f:
            return set(f.read().splitlines())
    except:
        return set()

def save_seen(seen):
    with open("seen.txt", "w") as f:
        f.write("\n".join(seen))

# ===== ПАРСИНГ =====
def get_offers():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(URL)
        page.wait_for_timeout(5000)

        offers = []

        cards = page.locator('div[data-cy="l-card"]').all()

        print("📦 Карточек найдено:", len(cards))

        for card in cards:
            try:
                link = card.locator("a").first.get_attribute("href")

                if link:
                    if link.startswith("/"):
                        link = "https://www.olx.pl" + link

                    link = link.split("?")[0]
                    offers.append(link)

            except:
                continue

        browser.close()

        return list(set(offers))

# ===== MAIN =====
def main():
    print("🚀 Проверка новых объявлений")

    seen = load_seen()
    offers = get_offers()

    new = 0

    for link in offers:
        if link not in seen:
            send(f"🏠 Новое объявление:\n{link}")
            seen.add(link)
            new += 1

    save_seen(seen)

    print(f"Новых объявлений: {new}")

if __name__ == "__main__":
    main()