# WebScrapper Empresas JM

Herramientas de captación comercial para **JMOrdenadores**: scrapers que extraen datos de contacto de empresas locales (Empresite y Páginas Amarillas) y una aplicación de escritorio que gestiona esas empresas en MySQL y envía campañas de email HTML con límites de envío progresivos (warm-up).

📄 **Documentación técnica completa (PDF):** [`docs/Documentacion_Tecnica_WebScrapperEmpresasJM.pdf`](docs/Documentacion_Tecnica_WebScrapperEmpresasJM.pdf)

## Flujo de trabajo

```
Scraper (Empresite / P. Amarillas) ──► resultados/*.json ──► carga en MySQL* ──► Consultor_db_v5.py ──► SMTP
```

\* La carga de los JSON en la base de datos no forma parte de este repositorio (se hacía vía n8n / manualmente). El esquema que espera el consultor está en [`docs/schema_referencia.sql`](docs/schema_referencia.sql).

## Archivos principales

| Archivo | Descripción |
|---|---|
| `WebScrapper_DAGM_ver1_empresite.py` | Scraper de empresite.eleconomista.es (Selenium + Chrome). Checkpoints por página y deduplicación. |
| `WebScrapper_DAGM_ver6.py` | Scraper de paginasamarillas.es (requests + BeautifulSoup). |
| `Consultor_db_v5.py` | Consultor MySQL, envío de emails con warm-up, estados de envío y mantenimiento de datos. |
| `warmup_state.json` / `recontact_state.json` | Estado persistente del warm-up y del recontacto mensual (se generan solos). |
| `WebScrapper_DAGM_ver1/2/3/5.py`, `consultor_db_v3.py` | Versiones anteriores, se conservan como histórico. |

## Instalación

Requisitos: Python 3.10+, Google Chrome (solo para Empresite), MySQL/MariaDB y una cuenta SMTP (en Gmail, con contraseña de aplicación).

```bash
python -m venv .venv
.venv\Scripts\activate            # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
copy .env.example .env             # Linux/macOS: cp .env.example .env
```

Rellena `.env` (debe estar en la misma carpeta que `Consultor_db_v5.py`) con los datos de la base de datos (`DB_*`) y del servidor SMTP (`SMTP_*`). En Linux, Tkinter se instala aparte: `sudo apt install python3-tk`.

## Uso rápido

### 1. Scraper de Empresite

```bash
python WebScrapper_DAGM_ver1_empresite.py
```

1. Pega la URL de un listado de Empresite (p. ej. `https://empresite.eleconomista.es/localidad/COSLADA-MADRID/`).
2. Indica la página inicial y la final; marca «Solo empresas con email» si quieres filtrar.
3. Pulsa **Iniciar scraping**. Se abre Chrome; si aparece un captcha o banner de cookies, resuélvelo en la propia ventana (el scraper espera 7 s cuando una ficha no muestra email).
4. Resultado en `resultados/<ruta>.json` (acumulado) y `resultados/<ruta>_pgNNN.json` (un checkpoint por página). Para reanudar tras un corte, pon como página inicial la siguiente a la última guardada.

### 2. Scraper de Páginas Amarillas

```bash
python WebScrapper_DAGM_ver6.py
```

Pega una URL de resultados (con `what=` y `where=`), el número de páginas y, opcionalmente, «Buscar email en web externa». Genera `resultados/<What><Where>_pagina_N.json`.

> ⚠️ Error conocido: la paginación no funciona (todas las páginas descargan la de la URL original). Ver la sección 4 del PDF.

### 3. Consultor y envío de emails

```bash
python Consultor_db_v5.py
```

- **Empresas con Email** → *Cargar empresas con email* → *Enviar email*: elige destinatarios reales (RE) o posibles (info@/contacto@/administracion@), revisa asunto y plantilla HTML y pulsa *Enviar emails*.
- Marcadores disponibles: `{empresa}`, `{tipo_empresa}`, `{localidad}` y, solo en el cuerpo, `{saludo}`. **No uses llaves sueltas en el HTML** (por ejemplo CSS dentro de `<style>`): rompen el envío. Usa estilos en línea.
- **Warm-up** (activado por defecto): 10 emails el primer día, subiendo hasta 150/día, máximo 15 por hora y esperas de 25–75 s entre envíos. Al llegar al límite el envío se detiene; vuelve a lanzarlo más tarde.
- Cada email queda marcado como `PE` (pendiente), `EN` (enviado) o `ER` (error). Al empezar un mes nuevo, los `EN` vuelven a `PE` para permitir recontactar.
- **Mantenimiento**: deduplicar emails, fusionar empresas duplicadas y eliminar empresas sin datos de contacto. Son operaciones irreversibles: haz copia de la BD antes.

## Formato del JSON de salida

```json
{
  "localidad": "Coslada Madrid",
  "tipo_empresa": "localidad",
  "resultados": [
    {
      "nombre": "TALLERES PEREZ SL",
      "telefono": "+34916123456",
      "email": "info@talleresperez.es",
      "email_posible_info": "info@talleresperez.es",
      "email_posible_contacto": "contacto@talleresperez.es",
      "email_posible_administracion": "administracion@talleresperez.es",
      "web": "https://www.talleresperez.es",
      "direccion": "No disponible",
      "codigo_postal": "No disponible",
      "localidad": "Coslada Madrid",
      "url_detalle": "https://empresite.eleconomista.es/TALLERES-PEREZ-SL.html"
    }
  ]
}
```

## Avisos

- La carpeta `selenium_profile_empresite/` (perfil de Chrome, ~360 MB con cookies e historial) está versionada y **no debería estarlo**. Ver la sección 8 del PDF.
- Nunca subas el fichero `.env` real.
- El scraping y el envío de emails comerciales deben respetar los términos de uso de los sitios de origen, el RGPD y la LSSI (identificar al remitente y ofrecer un medio de baja).
