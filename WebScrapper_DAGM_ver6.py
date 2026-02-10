import requests
from bs4 import BeautifulSoup
import time
import random
import re
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import tkinter as tk
from tkinter import ttk, messagebox
import unicodedata

# ---------------- CONFIGURACIÓN ----------------

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "es-ES,es;q=0.9"
}

OUTPUT_DIR = Path("resultados")
OUTPUT_DIR.mkdir(exist_ok=True)

telefono_regex = re.compile(r"(\+34\s?\d{9}|\b\d{9}\b)")
email_regex = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")

# ---------------- FUNCIONES AUXILIARES ----------------

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
    except:
        return None

def obtener_email_web(url):
    """Extrae email SOLO si coincide con el dominio de la web"""
    try:
        dominio = obtener_dominio(url)
        if not dominio:
            return None

        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None

        for email in email_regex.findall(r.text):
            email = limpiar_email(email)
            if dominio in email:
                return email
        return None
    except:
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

def construir_url(base_url, pagina):
    if "page=" in base_url:
        return re.sub(r"page=\d+", f"page={pagina}", base_url)
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={pagina}"

def extraer_info_url(url):
    try:
        params = parse_qs(urlparse(url).query)
        what = params.get("what", ["No disponible"])[0].replace("+", " ")
        where = params.get("where", ["No disponible"])[0].replace("+", " ")
        return what, where
    except:
        return "No disponible", "No disponible"

def generar_nombre_archivo(base_url):
    try:
        params = parse_qs(urlparse(base_url).query)
        what = params.get("what", ["Resultados"])[0]
        where = params.get("where", [""])[0]

        def norm(t):
            return re.sub(r"[^a-zA-Z0-9]", "", t.replace("+", " ")).capitalize()

        return f"{norm(what)}{norm(where)}.json"
    except:
        return "resultados.json"

# ---------------- SCRAPING ----------------

def iniciar_scraping(base_url, max_paginas, scrapear_email_web, log_func):

    for pagina in range(1, max_paginas + 1):
        url = construir_url(base_url, pagina)
        log_func(f"📄 Scrapeando página {pagina}")

        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            log_func(f"❌ Error HTTP {r.status_code}")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        empresas_html = soup.find_all("div", class_="box")

        if not empresas_html:
            log_func("⚠️ No hay más empresas")
            break

        empresas = []

        for empresa in empresas_html:
            time.sleep(random.uniform(0.3, 0.7))

            data = {
                "nombre": "No disponible",
                "telefono": "No disponible",
                "email": "No disponible",
                "email_posible_info": "No disponible",
                "email_posible_contacto": "No disponible",
                "email_posible_administracion": "No disponible",
                "web": "No disponible",
                "direccion": "No disponible",
                "codigo_postal": "No disponible",
                "localidad": "No disponible"
            }

            # ---------------- NOMBRE ----------------
            tag = empresa.select_one("span[itemprop='name']")
            if tag:
                data["nombre"] = tag.get_text(strip=True)

            # ---------------- TELÉFONO (HIBRIDO) ----------------
            tel_tag = empresa.find("a", href=re.compile(r"^tel:"))
            if tel_tag:
                data["telefono"] = normalizar_telefono(
                    tel_tag["href"].replace("tel:", "")
                )
            else:
                texto = empresa.get_text(" ", strip=True)
                match = telefono_regex.search(texto)
                if match:
                    data["telefono"] = normalizar_telefono(match.group())

            # ---------------- EMAIL DIRECTO ----------------
            email_tag = empresa.find("a", href=re.compile(r"^mailto:"))
            if email_tag:
                data["email"] = limpiar_email(
                    email_tag["href"].replace("mailto:", "")
                )

            # ---------------- WEB / MÁS INFO (ROBUSTO) ----------------
            web_tag = empresa.find("a", class_=re.compile("web|website", re.I))
            if not web_tag:
                for a in empresa.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("http") and "paginasamarillas" not in href:
                        web_tag = a
                        break

            if web_tag:
                data["web"] = web_tag["href"].split("?")[0]

            # ---------------- EMAIL DESDE WEB ----------------
            if scrapear_email_web and data["email"] == "No disponible" and data["web"] != "No disponible":
                email_web = obtener_email_web(data["web"])
                if email_web:
                    data["email"] = email_web

            # ---------------- EMAILS POSIBLES ----------------
            dominio = obtener_dominio_fiable(data)
            if dominio:
                data["email_posible_info"] = f"info@{dominio}"
                data["email_posible_contacto"] = f"contacto@{dominio}"
                data["email_posible_administracion"] = f"administracion@{dominio}"

            # ---------------- DIRECCIÓN ----------------
            tag = empresa.select_one("span[itemprop='streetAddress']")
            if tag:
                data["direccion"] = tag.get_text(strip=True)

            tag = empresa.select_one("span[itemprop='postalCode']")
            if tag:
                data["codigo_postal"] = tag.get_text(strip=True)

            tag = empresa.select_one("span[itemprop='addressLocality']")
            if tag:
                data["localidad"] = tag.get_text(strip=True)

            if datosvalidos(data):
                empresas.append(data)

        tipo, localidad = extraer_info_url(base_url)

        resultado = {
            "localidad": localidad,
            "tipo_empresa": tipo,
            "resultados": empresas
        }

        nombre_archivo = generar_nombre_archivo(base_url)
        output = OUTPUT_DIR / nombre_archivo.replace(".json", f"_pagina_{pagina}.json")

        with open(output, "w", encoding="utf-8") as f:
            json.dump(resultado, f, ensure_ascii=False, indent=4)

        log_func(f"✅ Página {pagina} guardada ({len(empresas)} empresas)")

    log_func("🎉 Scraping finalizado")

# ---------------- GUI ----------------

def lanzar_gui():

    def log(msg):
        text_log.insert(tk.END, msg + "\n")
        text_log.see(tk.END)
        root.update()

    def ejecutar():
        try:
            iniciar_scraping(
                entry_url.get().strip(),
                int(entry_paginas.get()),
                var_email_web.get(),
                log
            )
            messagebox.showinfo("Finalizado", "Scraping completado")
        except ValueError:
            messagebox.showerror("Error", "Número de páginas inválido")

    root = tk.Tk()
    root.title("WebScraper Páginas Amarillas")
    root.geometry("600x450")

    frame = ttk.Frame(root, padding=10)
    frame.pack(fill="both", expand=True)

    ttk.Label(frame, text="URL:").pack(anchor="w")
    entry_url = ttk.Entry(frame)
    entry_url.pack(fill="x")

    ttk.Label(frame, text="Número de páginas:").pack(anchor="w")
    entry_paginas = ttk.Entry(frame)
    entry_paginas.insert(0, "3")
    entry_paginas.pack(fill="x")

    var_email_web = tk.BooleanVar(value=True)
    ttk.Checkbutton(frame, text="Buscar email en web externa", variable=var_email_web).pack(anchor="w")

    ttk.Button(frame, text="Iniciar scraping", command=ejecutar).pack(pady=10)

    text_log = tk.Text(frame, height=15)
    text_log.pack(fill="both", expand=True)

    root.mainloop()

# ---------------- MAIN ----------------

if __name__ == "__main__":
    lanzar_gui()
