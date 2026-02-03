from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

# Modo 2: navegador visivel para resolver o challenge manualmente
url = "https://betsapi.com/"

options = Options()
# Nao usar headless aqui
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")


def is_cloudflare_page(driver) -> bool:
    title = (driver.title or "").lower()
    html = (driver.page_source or "").lower()
    markers = (
        "um momento",
        "just a moment",
        "cf-challenge",
        "cloudflare",
        "checking your browser",
    )
    return any(m in title or m in html for m in markers)


def main() -> None:
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        if is_cloudflare_page(driver):
            print("Cloudflare detectado. Resolva manualmente no navegador aberto...")
            print("Aguardando ate 180s para liberar acesso.")

            try:
                WebDriverWait(driver, 180).until(lambda d: not is_cloudflare_page(d))
            except TimeoutException:
                print("Tempo esgotado: challenge nao foi liberado.")
                return

        print("Acesso liberado. Conteudo real carregado.")
        print(f"Titulo: {driver.title}")
        html = driver.page_source
        print("Primeiros 500 caracteres do HTML:")
        print(html[:500])

        # Exemplo: pause para inspecao manual antes de fechar
        input("\nPressione Enter para fechar o navegador...")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
