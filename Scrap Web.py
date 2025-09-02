from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import json
import time


def scrape_polotab_with_selenium():
    """
    Scraper con Selenium para contenido dinámico de PoloTab API
    """
    print("🚀 Iniciando Selenium scraper para PoloTab API...")

    # Configurar Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Comentar esta línea si quieres ver el navegador
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # Inicializar el driver
    # Nota: Asegúrate de tener ChromeDriver instalado y en el PATH
    # O especifica la ruta: Service('/path/to/chromedriver')
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Navegar a la página
        url = "https://developer.polotab.com/"
        print(f"📍 Navegando a {url}")
        driver.get(url)

        # Esperar a que la página cargue completamente
        print("⏳ Esperando que cargue el contenido...")
        time.sleep(5)  # Espera inicial para que cargue JavaScript

        # Resultados
        results = {
            'url': url,
            'title': driver.title,
            'sections': {},
            'endpoints': [],
            'authentication_info': {},
            'menu_items': []
        }

        # Intentar obtener el menú lateral
        try:
            print("🔍 Buscando menú de navegación...")
            # Buscar elementos del menú por diferentes selectores
            menu_selectors = [
                "nav",
                "[role='navigation']",
                ".sidebar",
                ".menu",
                ".navigation",
                "[class*='sidebar']",
                "[class*='menu']",
                "[class*='nav']"
            ]

            for selector in menu_selectors:
                try:
                    menu_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if menu_elements:
                        for menu in menu_elements:
                            menu_text = menu.text
                            if menu_text and len(menu_text) > 50:  # Si tiene contenido sustancial
                                results['menu_items'].append(menu_text)
                                print(f"✅ Menú encontrado con selector: {selector}")
                                break
                except:
                    continue
        except Exception as e:
            print(f"⚠️ Error buscando menú: {e}")

        # Buscar y hacer clic en "Authentication"
        try:
            print("🔐 Buscando sección de Authentication...")
            auth_clicked = False

            # Diferentes formas de encontrar el elemento de autenticación
            auth_selectors = [
                "//button[contains(text(), 'Authentication')]",
                "//a[contains(text(), 'Authentication')]",
                "//div[contains(text(), 'Authentication')]",
                "//*[contains(@class, 'authentication')]",
                "//span[contains(text(), 'Authentication')]"
            ]

            for selector in auth_selectors:
                try:
                    auth_element = driver.find_element(By.XPATH, selector)
                    driver.execute_script("arguments[0].scrollIntoView(true);", auth_element)
                    time.sleep(1)
                    auth_element.click()
                    print("✅ Click en Authentication")
                    auth_clicked = True
                    time.sleep(3)  # Esperar que cargue el contenido
                    break
                except:
                    continue

            if auth_clicked:
                # Obtener el contenido expandido
                page_content = driver.find_element(By.TAG_NAME, "body").text
                if '/auth/v1/restaurants/token' in page_content:
                    results['authentication_info']['endpoint_found'] = True
                    print("✅ Endpoint de autenticación encontrado!")

                    # Buscar más detalles específicos
                    content_lines = page_content.split('\n')
                    for i, line in enumerate(content_lines):
                        if '/auth/v1/restaurants/token' in line:
                            # Obtener contexto alrededor del endpoint
                            context_start = max(0, i - 5)
                            context_end = min(len(content_lines), i + 10)
                            results['authentication_info']['context'] = '\n'.join(
                                content_lines[context_start:context_end])
                            break

        except Exception as e:
            print(f"⚠️ Error en sección Authentication: {e}")

        # Buscar todos los endpoints visibles
        print("🔍 Buscando endpoints en la página...")
        page_text = driver.find_element(By.TAG_NAME, "body").text

        # Patrones de endpoints
        import re
        endpoint_patterns = [
            r'(GET|POST|PUT|DELETE|PATCH)\s+(/[^\s]+)',
            r'`(GET|POST|PUT|DELETE|PATCH)\s+(/[^\s`]+)`',
            r'/auth/v1/[^\s]+',
            r'/api/v1/[^\s]+',
            r'/v1/[^\s]+'
        ]

        found_endpoints = set()
        for pattern in endpoint_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    found_endpoints.add(f"{match[0]} {match[1]}")
                else:
                    found_endpoints.add(match)

        results['endpoints'] = list(found_endpoints)
        print(f"📍 Encontrados {len(results['endpoints'])} endpoints")

        # Intentar hacer clic en otras secciones importantes
        sections_to_explore = ['Restaurants', 'Orders', 'Menu', 'Items']

        for section in sections_to_explore:
            try:
                print(f"🔍 Explorando sección: {section}")
                section_selectors = [
                    f"//button[contains(text(), '{section}')]",
                    f"//a[contains(text(), '{section}')]",
                    f"//div[contains(text(), '{section}')]",
                    f"//span[contains(text(), '{section}')]"
                ]

                for selector in section_selectors:
                    try:
                        element = driver.find_element(By.XPATH, selector)
                        driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(1)
                        element.click()
                        time.sleep(2)

                        # Obtener contenido de la sección
                        section_content = driver.find_element(By.TAG_NAME, "body").text
                        # Guardar solo si hay contenido nuevo
                        if len(section_content) > 100:
                            results['sections'][section.lower()] = section_content[:2000]  # Primeros 2000 chars
                            print(f"✅ Contenido de {section} obtenido")
                        break
                    except:
                        continue

            except Exception as e:
                print(f"⚠️ No se pudo explorar {section}: {e}")

        # Obtener todo el contenido final de la página
        final_content = driver.find_element(By.TAG_NAME, "body").text
        results['full_content_preview'] = final_content[:5000]  # Primeros 5000 caracteres

        # Buscar información específica de PoloTab
        if 'https://api.polotab.com' in final_content:
            results['api_info'] = {
                'base_url_confirmed': True,
                'base_url': 'https://api.polotab.com'
            }

        # Guardar resultados
        filename = 'polotab_selenium_results.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"\n✅ Resultados guardados en '{filename}'")

        # Mostrar resumen
        print("\n📊 RESUMEN DE RESULTADOS:")
        print(f"   - Título: {results['title']}")
        print(f"   - Endpoints encontrados: {len(results['endpoints'])}")
        print(f"   - Secciones exploradas: {len(results['sections'])}")
        print(f"   - Menú items: {len(results['menu_items'])}")

        if results['endpoints']:
            print("\n🔗 ALGUNOS ENDPOINTS ENCONTRADOS:")
            for endpoint in results['endpoints'][:10]:
                print(f"   - {endpoint}")

        return results

    except Exception as e:
        print(f"❌ Error general: {e}")
        return {'error': str(e)}

    finally:
        # Cerrar el navegador
        driver.quit()
        print("\n🔚 Navegador cerrado")


if __name__ == "__main__":
    # Instalar primero:
    # pip install selenium
    # Descargar ChromeDriver de: https://chromedriver.chromium.org/

    results = scrape_polotab_with_selenium()

    # Mostrar preview del contenido
    if 'full_content_preview' in results:
        print("\n📝 PREVIEW DEL CONTENIDO:")
        print("-" * 50)
        print(results['full_content_preview'][:500])
        print("-" * 50)