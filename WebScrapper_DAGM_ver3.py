import requests
from bs4 import BeautifulSoup
import time
import random
import re
import json

# =======================
# CONFIGURACIÓN
# =======================
URL = "https://www.paginasamarillas.es/search/asesorias-y-gestorias/all-ma/madrid/all-is/coslada/all-ba/all-pu/all-nc/1?what=asesorias+y+gestorias&where=coslada&qc=true"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "es-ES,es;q=0.9"
}

# Aquí pones la URL de tu Webhook de n8n (copia la URL del nodo Webhook)
N8N_WEBHOOK_URL = "https://danmelendo.app.n8n.cloud/webhook-test/4f3a83ba-d9e4-4fea-958d-85051bed2559"

# =======================
# SCRAPING
# =======================
response = requests.get(URL, headers=HEADERS, timeout=15)
response.raise_for_status()

soup = BeautifulSoup(response.text, "html.parser")
empresas_html = soup.find_all("div", class_="box")
print(f"Empresas encontradas: {len(empresas_html)}")

empresas = []

telefono_regex = re.compile(r"(\+34\d{9}|\b\d{9}\b)")

for empresa in empresas_html:
    time.sleep(random.uniform(0.3, 0.8))

    nombre_empresa = "No disponible"
    telefono = "No disponible"
    sitio_web = "No disponible"

    # Nombre empresa
    nombre_tag = empresa.select_one("span[itemprop='name']")
    if nombre_tag:
        nombre_empresa = nombre_tag.get_text(strip=True)

    # Teléfono
    tel_tag = empresa.find("a", href=re.compile(r"^tel:"))
    if tel_tag:
        telefono = tel_tag['href'].replace("tel:", "").strip()
    else:
        texto_empresa = empresa.get_text(" ", strip=True)
        match = telefono_regex.search(texto_empresa)
        if match:
            telefono = match.group()

    # Sitio web
    for a_tag in empresa.find_all("a", href=True):
        href = a_tag['href'].strip()
        if href.startswith("http") and "paginasamarillas" not in href and not href.startswith("tel:") and not href.startswith("mailto:"):
            sitio_web = href
            break

    empresas.append({
        "nombre_empresa": nombre_empresa,
        "telefono": telefono,
        "web": sitio_web
    })

# =======================
# ENVIAR DATOS A N8N
# =======================
try:
    response_webhook = requests.post(N8N_WEBHOOK_URL, json=empresas, timeout=15)
    response_webhook.raise_for_status()
    print(f"Datos enviados correctamente al Webhook de n8n ({len(empresas)} registros).")
except Exception as e:
    print("Error al enviar datos al Webhook de n8n:", e)
