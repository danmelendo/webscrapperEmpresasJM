import json
import queue
import random
import re
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import tkinter as tk
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tkinter import messagebox, ttk


OUTPUT_DIR = Path("resultados")
OUTPUT_DIR.mkdir(exist_ok=True)
# Deshabilitado: la detección automática de captcha estaba dando falsos positivos.
CAPTCHA_CHECK_ENABLED = False
EXCLUDE_HTML_PAGES = {
    "faqs.html",
    "faq.html",
    "cookies.html",
    "politica-cookies.html",
    "politica-privacidad.html",
    "privacidad.html",
    "aviso-legal.html",
    "terminos.html",
    "terminos-condiciones.html",
    "terminos-y-condiciones.html",
    "terminos-condiciones-de-uso.html",
    "condiciones.html",
    "condiciones-de-uso.html",
    "condiciones-generales.html",
    "condiciones-generales-de-uso.html",
    "politica-privacidad-y-cookies.html",
    "politica-de-privacidad.html",
    "politica-de-cookies.html",
    # Variantes en inglés observadas en empresite:
    "privacy_policy.html",
    "terms_of_use.html",
    "contacto.html",
    "about.html",
}

# Bloqueo por patrones (evita que entren paginas legales/soporte aunque cambie el slug exacto).
EXCLUDE_HTML_PATTERNS = re.compile(
    r"(?:^|/)(?:"
    r"terminos|condiciones|privacidad|politica|cookies|cookie|aviso-legal|legal|rgpd|gdpr|"
    r"privacy|terms|use"
    r"contacto|about|faqs?|sitemap|mapa|help|ayuda"
    r")(?:-|_|/|$)",
    re.IGNORECASE,
)

CHROME_PROFILE_DIR = Path("selenium_profile_empresite")
DETAIL_DELAY_SECONDS = (0.0, 0.0)
PAGE_DELAY_SECONDS = (0.0, 0.0)
COOLDOWN_EVERY_N_DETAILS = 0
COOLDOWN_SECONDS = (0.0, 0.0)
# Para testing/interaccion con captcha: mantener navegador visible.
BROWSER_HIDE_ENABLED = False
BROWSER_HIDDEN_POS = (-32000, -32000)  # Windows: off-screen (si se activa)
BROWSER_VISIBLE_POS = (60, 60)
BROWSER_VISIBLE_SIZE = (1200, 900)

def aplicar_filtros_empresite(base_url, solo_con_email):
    """
    Aplica filtros de empresite vía querystring.
    - Checkbox "Con email" en la web añade: emp_email=true
    - El sitio suele usar testfiltros=1 al activar filtros.
    """
    if not solo_con_email:
        return base_url

    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    qs["testfiltros"] = ["1"]
    qs["emp_email"] = ["true"]

    query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=query))

telefono_regex = re.compile(r"(\+34\s?\d{9}|\b\d{9}\b)")
email_regex = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")


def limpiar_email(email):
    return email.strip().rstrip(".,;:")


def normalizar_telefono(telefono):
    telefono = telefono.replace(" ", "").replace("-", "")
    if telefono.startswith("+34"):
        return telefono
    if telefono.isdigit() and len(telefono) == 9:
        return f"+34{telefono}"
    return telefono


def obtener_dominio(url):
    try:
        dominio = urlparse(url).netloc.lower()
        if dominio.startswith("www."):
            dominio = dominio[4:]
        return dominio if "." in dominio else None
    except Exception:
        return None


def obtener_dominio_fiable(data):
    if data["web"] != "No disponible":
        dominio = obtener_dominio(data["web"])
        if dominio:
            return dominio

    if data["email"] != "No disponible":
        partes = data["email"].split("@")
        if len(partes) == 2:
            return partes[1].lower()
    return None


def datosvalidos(data):
    return (
        data["nombre"] != "No disponible"
        or data["telefono"] != "No disponible"
        or data["email"] != "No disponible"
        or data["web"] != "No disponible"
    )


def construir_url_empresite(base_url, pagina):
    parsed = urlparse(base_url)
    path = parsed.path or "/"
    path = re.sub(r"/PgNum-\d+/?", "/", path).rstrip("/")
    if pagina <= 1:
        path = f"{path}/"
    else:
        path = f"{path}/PgNum-{pagina}/"
    return urlunparse(parsed._replace(path=path))


def extraer_tipo_localidad_empresite(base_url):
    try:
        partes = [p for p in urlparse(base_url).path.split("/") if p]
        tipo = "No disponible"
        localidad = "No disponible"
        if len(partes) >= 2:
            tipo = partes[0].replace("-", " ")
            localidad = partes[1].replace("-", " ")
        return tipo, localidad.title()
    except Exception:
        return "No disponible", "No disponible"


def generar_nombre_archivo(base_url):
    try:
        parsed = urlparse(base_url)
        path_name = "_".join([p for p in parsed.path.split("/") if p])
        path_name = re.sub(r"[^a-zA-Z0-9_]", "", path_name)
        return f"{path_name or 'resultados'}.json"
    except Exception:
        return "resultados.json"


def new_empresa(localidad_default="No disponible"):
    return {
        "nombre": "No disponible",
        "telefono": "No disponible",
        "email": "No disponible",
        "email_posible_info": "No disponible",
        "email_posible_contacto": "No disponible",
        "email_posible_administracion": "No disponible",
        "web": "No disponible",
        "direccion": "No disponible",
        "codigo_postal": "No disponible",
        "localidad": localidad_default,
    }


def normalizar_web_empresite(raw_url):
    if not raw_url:
        return "No disponible"
    web = raw_url.strip()
    if web.startswith("//"):
        web = "https:" + web
    elif web.startswith("www."):
        web = "https://" + web
    if not web.lower().startswith(("http://", "https://")):
        return "No disponible"
    return web.split("?")[0]


def es_pagina_captcha_html(html):
    # Mantener API por compatibilidad, pero deshabilitada para testing.
    return False


def extraer_url_ficha_empresite(anchor):
    href = (anchor.get("href") or "").strip()
    onclick = (anchor.get("onclick") or "").strip()
    url = None

    if href:
        url = href
    elif onclick:
        m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick, re.I)
        if m:
            url = m.group(1).strip()

    if not url:
        return None
    if url.startswith("/"):
        url = f"https://empresite.eleconomista.es{url}"
    if not url.lower().startswith("http"):
        return None

    parsed = urlparse(url)
    if "empresite.eleconomista.es" not in parsed.netloc.lower():
        return None

    path = (parsed.path or "").lower()
    segmentos = [p for p in path.split("/") if p]
    ultimo = segmentos[-1] if segmentos else ""

    # Fichas típicas: https://empresite.eleconomista.es/NOMBRE-EMPRESA.html (en raíz)
    # También aceptamos rutas /empresa/... cuando existan.
    es_html = ultimo.endswith(".html")
    es_ficha = (len(segmentos) == 1 and es_html) or ("/empresa/" in path and es_html)
    if not es_ficha:
        return None

    if ultimo in EXCLUDE_HTML_PAGES:
        return None

    # Excluir paginas no-empresa por patrones (ej. "terminos-y-condiciones", "politica-de-privacidad", etc.)
    if EXCLUDE_HTML_PATTERNS.search(path):
        return None

    return url


def nombre_desde_url_ficha(detail_url):
    try:
        ultimo = urlparse(detail_url).path.split("/")[-1]
        base = re.sub(r"\.html?$", "", ultimo, flags=re.I).replace("-", " ").strip()
        return base.title() if base else "No disponible"
    except Exception:
        return "No disponible"


def crear_driver(use_profile=True):
    options = Options()
    # Mantener visible para poder interactuar con cookies/captcha.
    options.add_argument("--start-maximized")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option(
        "excludeSwitches", ["enable-automation", "enable-logging"]
    )
    options.add_experimental_option("useAutomationExtension", False)

    if use_profile:
        # Persistir cookies/sesión reduce banners repetidos y a veces baja captchas.
        CHROME_PROFILE_DIR.mkdir(exist_ok=True)
        options.add_argument(f"--user-data-dir={CHROME_PROFILE_DIR.resolve()}")
        options.add_argument("--profile-directory=Default")

    service = Service(log_output=subprocess.DEVNULL)
    return webdriver.Chrome(options=options, service=service)


def ocultar_navegador(driver):
    if not BROWSER_HIDE_ENABLED:
        return
    try:
        driver.set_window_position(BROWSER_HIDDEN_POS[0], BROWSER_HIDDEN_POS[1])
        driver.minimize_window()
    except Exception:
        return


def mostrar_navegador(driver):
    try:
        driver.set_window_position(BROWSER_VISIBLE_POS[0], BROWSER_VISIBLE_POS[1])
        driver.set_window_size(BROWSER_VISIBLE_SIZE[0], BROWSER_VISIBLE_SIZE[1])
        driver.maximize_window()
    except Exception:
        return


def intentar_aceptar_cookies(driver, log_func, timeout=6):
    """
    Intenta cerrar/aceptar banners de cookies comunes.
    """
    labels = [
        "Aceptar",
        "Aceptar todo",
        "Aceptar todas",
        "Aceptar cookies",
        "Estoy de acuerdo",
        "Acepto",
        "Allow all",
        "Accept all",
        "I agree",
    ]

    for texto in labels:
        xpath = (
            "//button[contains(translate(normalize-space(.),"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚÜ',"
            "'abcdefghijklmnopqrstuvwxyzáéíóúü'),"
            f"'{texto.lower()}')]"
            "|"
            "//a[contains(translate(normalize-space(.),"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚÜ',"
            "'abcdefghijklmnopqrstuvwxyzáéíóúü'),"
            f"'{texto.lower()}')]"
        )
        try:
            elems = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.XPATH, xpath))
            )
            for elem in elems:
                if elem.is_displayed() and elem.is_enabled():
                    try:
                        driver.execute_script("arguments[0].click();", elem)
                    except Exception:
                        elem.click()
                    log_func("Banner de cookies aceptado automaticamente.")
                    return True
        except Exception:
            continue
    return False


def humanizar_pagina(driver):
    """
    Pequeñas acciones para parecer navegación humana (sin ser intrusivo).
    """
    # Deshabilitado: máxima velocidad sin sleeps.
    return


def esperar_y_obtener_html(driver, url, log_func, timeout=25, esperar_email=False):
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except Exception:
        pass
    intentar_aceptar_cookies(driver, log_func, timeout=3)
    humanizar_pagina(driver)
    if esperar_email:
        try:
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.email[href^='mailto:']"))
            )
        except Exception:
            log_func("Email no encontrado en 3s. Pausa 7s para resolver posible captcha/cookies...")
            time.sleep(7)
    html = driver.page_source or ""
    return html, True


def extraer_datos_ficha_desde_html(html):
    datos = {"email": "No disponible", "web": "No disponible", "telefono": "No disponible"}
    soup = BeautifulSoup(html, "html.parser")

    email_a = soup.select_one("a.email[href^='mailto:']")
    if email_a:
        href = email_a.get("href", "")
        correo = href.replace("mailto:", "").split("?")[0].strip()
        if correo:
            datos["email"] = limpiar_email(correo)

    web_a = soup.select_one("a.url[href]")
    if web_a:
        datos["web"] = normalizar_web_empresite(web_a.get("href", ""))

    tel_a = soup.select_one("a[href^='tel:']")
    if tel_a:
        datos["telefono"] = normalizar_telefono(tel_a.get("href", "").replace("tel:", "").strip())
    else:
        txt = soup.get_text(" ", strip=True)
        mt = telefono_regex.search(txt)
        if mt:
            datos["telefono"] = normalizar_telefono(mt.group())

    return datos


def iniciar_scraping_empresite(base_url, max_paginas, log_func, use_profile=True, pagina_inicio=1):
    tipo, localidad = extraer_tipo_localidad_empresite(base_url)
    empresas_totales = []
    vistas = set()
    procesadas = set()  # url_detalle normalizada para no repetir entre paginas

    driver = crear_driver(use_profile=use_profile)
    try:
        detalles_ok = 0
        pagina_inicio = max(1, int(pagina_inicio or 1))
        for pagina in range(pagina_inicio, max_paginas + 1):
            list_url = construir_url_empresite(base_url, pagina)
            log_func(f"Scrapeando pagina {pagina}: {list_url}")
            html, ok = esperar_y_obtener_html(driver, list_url, log_func, esperar_email=False)
            if not ok:
                break

            soup = BeautifulSoup(html, "html.parser")
            anchors = soup.select(
                'a[onclick*="location.href"], a[href$=".html"], a[href*="/empresa/"], a[href*="/EMPRESA/"]'
            )
            if not anchors:
                log_func("No se encontraron fichas en esta pagina.")
                break

            detail_urls = []
            seen_page = set()
            for a in anchors:
                detail = extraer_url_ficha_empresite(a)
                if detail and detail not in seen_page:
                    seen_page.add(detail)
                    detail_urls.append((detail, a.get_text(" ", strip=True), a.get("title", "")))

            empresas_pagina = []
            for detail_url, txt, title in detail_urls:
                url_norm = (detail_url or "").strip().lower()
                if not url_norm or url_norm in procesadas:
                    continue
                procesadas.add(url_norm)

                data = new_empresa(localidad_default=localidad)
                data["url_detalle"] = detail_url

                nombre = (txt or "").strip() or (title or "").strip()
                if nombre and nombre.lower() != "ver ficha":
                    data["nombre"] = nombre
                else:
                    data["nombre"] = nombre_desde_url_ficha(detail_url)

                detail_html, ok_detail = esperar_y_obtener_html(driver, detail_url, log_func, esperar_email=True)
                if not ok_detail:
                    break
                ficha = extraer_datos_ficha_desde_html(detail_html)
                data["email"] = ficha["email"]
                data["web"] = ficha["web"]
                data["telefono"] = ficha["telefono"]

                dominio = obtener_dominio_fiable(data)
                if dominio:
                    data["email_posible_info"] = f"info@{dominio}"
                    data["email_posible_contacto"] = f"contacto@{dominio}"
                    data["email_posible_administracion"] = f"administracion@{dominio}"

                # Deduplicacion global: la URL de ficha es el identificador mas estable.
                clave = url_norm
                if clave not in vistas and datosvalidos(data):
                    vistas.add(clave)
                    empresas_totales.append(data)
                    empresas_pagina.append(data)

                detalles_ok += 1
                # Sin sleeps/cooldowns: máximo ritmo. Si aparece captcha, se esperará en esperar_y_obtener_html.

            log_func(f"Pagina {pagina} procesada ({len(empresas_totales)} empresas acumuladas)")
            guardar_resultado_pagina(base_url, tipo, localidad, pagina, empresas_pagina, log_func)
            guardar_resultado_acumulado_parcial(base_url, tipo, localidad, pagina, empresas_totales, log_func)
            # Sin sleep entre paginas.
    finally:
        driver.quit()

    return tipo, localidad, empresas_totales


def guardar_resultado(base_url, tipo, localidad, empresas_totales, log_func):
    resultado_final = {
        "localidad": localidad,
        "tipo_empresa": tipo,
        "resultados": empresas_totales,
    }
    output = OUTPUT_DIR / generar_nombre_archivo(base_url)
    tmp = output.with_suffix(output.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(resultado_final, f, ensure_ascii=False, indent=4)
    tmp.replace(output)
    log_func(f"Scraping finalizado. Total empresas: {len(empresas_totales)}")
    log_func(f"Guardado en: {output}")


def guardar_resultado_pagina(base_url, tipo, localidad, pagina, empresas_pagina, log_func):
    """
    Checkpoint: guarda resultados de UNA pagina para no perder datos si se corta el proceso.
    """
    output_base = OUTPUT_DIR / generar_nombre_archivo(base_url)
    output = output_base.with_name(f"{output_base.stem}_pg{pagina:03d}{output_base.suffix}")

    payload = {
        "localidad": localidad,
        "tipo_empresa": tipo,
        "pagina": pagina,
        "resultados": empresas_pagina,
    }

    tmp = output.with_suffix(output.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)
    tmp.replace(output)
    log_func(f"Checkpoint pagina {pagina}: {len(empresas_pagina)} empresas guardadas en: {output}")


def guardar_resultado_acumulado_parcial(base_url, tipo, localidad, pagina, empresas_totales, log_func):
    """
    Checkpoint: actualiza el JSON acumulado tras cada pagina.
    """
    output = OUTPUT_DIR / generar_nombre_archivo(base_url)
    payload = {
        "localidad": localidad,
        "tipo_empresa": tipo,
        "pagina_hasta": pagina,
        "resultados": empresas_totales,
    }
    tmp = output.with_suffix(output.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)
    tmp.replace(output)
    log_func(f"Checkpoint acumulado hasta pagina {pagina}: {len(empresas_totales)} empresas en: {output}")


def iniciar_scraping(base_url, max_paginas, log_func, use_profile=True, pagina_inicio=1):
    dominio = obtener_dominio(base_url) or ""
    if "empresite.eleconomista.es" not in dominio:
        log_func("Este scraper es exclusivo para empresite.eleconomista.es")
        return
    tipo, localidad, empresas_totales = iniciar_scraping_empresite(
        base_url, max_paginas, log_func, use_profile=use_profile, pagina_inicio=pagina_inicio
    )
    guardar_resultado(base_url, tipo, localidad, empresas_totales, log_func)


def lanzar_gui():
    log_queue = queue.Queue()
    running = {"value": False}

    def log(msg):
        log_queue.put(msg)

    def flush_logs():
        while True:
            try:
                msg = log_queue.get_nowait()
            except queue.Empty:
                break
            text_log.insert(tk.END, msg + "\n")
            text_log.see(tk.END)
        if running["value"] or not log_queue.empty():
            root.after(120, flush_logs)

    def set_running_state(is_running):
        running["value"] = is_running
        btn_scrap.config(state=("disabled" if is_running else "normal"))

    def worker(url, pagina_inicio, paginas, solo_email, use_profile):
        try:
            url_filtrada = aplicar_filtros_empresite(url, solo_email)
            iniciar_scraping(
                url_filtrada,
                paginas,
                log,
                use_profile=use_profile,
                pagina_inicio=pagina_inicio,
            )
            root.after(0, lambda: messagebox.showinfo("Finalizado", "Scraping completado"))
        except Exception as exc:
            err_msg = str(exc)
            log(f"Error inesperado: {err_msg}")
            root.after(0, lambda m=err_msg: messagebox.showerror("Error", m))
        finally:
            root.after(0, lambda: set_running_state(False))

    def ejecutar():
        if running["value"]:
            return
        try:
            pagina_inicio = int(entry_pagina_inicio.get())
            paginas = int(entry_paginas.get())
        except ValueError:
            messagebox.showerror("Error", "Numero de paginas invalido")
            return
        set_running_state(True)
        threading.Thread(
            target=worker,
            args=(
                entry_url.get().strip(),
                pagina_inicio,
                paginas,
                var_solo_email.get(),
                var_use_profile.get(),
            ),
            daemon=True,
        ).start()
        root.after(120, flush_logs)

    root = tk.Tk()
    root.title("WebScraper Empresite (Selenium)")
    root.geometry("760x520")

    frame = ttk.Frame(root, padding=10)
    frame.pack(fill="both", expand=True)

    ttk.Label(frame, text="URL Empresite:").pack(anchor="w")
    entry_url = ttk.Entry(frame)
    entry_url.pack(fill="x")
    entry_url.insert(0, "https://empresite.eleconomista.es/localidad/COSLADA-MADRID/")

    ttk.Label(frame, text="Numero de paginas:").pack(anchor="w")
    entry_paginas = ttk.Entry(frame)
    entry_paginas.insert(0, "3")
    entry_paginas.pack(fill="x")

    ttk.Label(frame, text="Pagina inicial:").pack(anchor="w")
    entry_pagina_inicio = ttk.Entry(frame)
    entry_pagina_inicio.insert(0, "1")
    entry_pagina_inicio.pack(fill="x")

    var_solo_email = tk.BooleanVar(value=True)
    ttk.Checkbutton(
        frame,
        text="Solo empresas con email (emp_email=true)",
        variable=var_solo_email,
    ).pack(anchor="w")

    var_use_profile = tk.BooleanVar(value=True)
    ttk.Checkbutton(
        frame,
        text="Reusar perfil de Chrome (menos cookies/captcha)",
        variable=var_use_profile,
    ).pack(anchor="w")

    btn_scrap = ttk.Button(frame, text="Iniciar scraping", command=ejecutar)
    btn_scrap.pack(pady=10)

    text_log = tk.Text(frame, height=22)
    text_log.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    lanzar_gui()
