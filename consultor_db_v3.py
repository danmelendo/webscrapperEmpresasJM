import mysql.connector
import tkinter as tk
from tkinter import ttk, messagebox
import smtplib
from email.message import EmailMessage

# ---------------- CONFIG ----------------
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# Cargar un solo .env
load_dotenv(BASE_DIR / ".env")

# Función segura para convertir strings a int
def env_int(key, default):
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


# ---------------- DB ----------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": env_int("DB_PORT", 3306),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset": os.getenv("DB_CHARSET", "utf8mb4")
}
print("DB_CONFIG =", DB_CONFIG)
# ---------------- SMTP ----------------
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = env_int("SMTP_PORT", 587)
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

print("SMTP_CONFIG:", SMTP_SERVER, SMTP_USER)
PLANTILLA_EMAIL = """Hola,

Me pongo en contacto con vosotros tras revisar vuestra empresa.

Ofrecemos soluciones informáticas a medida para negocios.

Si os interesa, estaré encantado de ampliar la información.

Un saludo,
José Miguel
JMOrdenadores
"""

# ---------------- DB ----------------

def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)

def obtener_empresas():
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT id_empresa, nombre
        FROM empresa
        ORDER BY nombre
    """)
    empresas = cursor.fetchall()

    cursor.close()
    conn.close()
    return empresas

def obtener_emails_empresas(ids_empresas):
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)

    formato = ",".join(["%s"] * len(ids_empresas))
    sql = f"""
        SELECT e.email, te.descripcion AS tipo, em.nombre AS empresa
        FROM email e
        JOIN tipo_email te ON e.id_tipo_email = te.id_tipo_email
        JOIN empresa em ON e.id_empresa = em.id_empresa
        WHERE e.id_empresa IN ({formato})
        ORDER BY em.nombre
    """

    cursor.execute(sql, ids_empresas)
    emails = cursor.fetchall()

    cursor.close()
    conn.close()
    return emails

# ---------------- EMAIL ----------------

def enviar_email(destinatario, asunto, cuerpo):
    msg = EmailMessage()
    msg["From"] = SMTP_USER
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.set_content(cuerpo)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# ---------------- GUI ----------------

def lanzar_gui():

    empresas = []
    emails_actuales = []

    def cargar_empresas():
        nonlocal empresas
        listbox_empresas.delete(0, tk.END)
        empresas = obtener_empresas()
        for e in empresas:
            listbox_empresas.insert(tk.END, e["nombre"])

    def ver_emails():
        nonlocal emails_actuales
        seleccion = listbox_empresas.curselection()
        if not seleccion:
            messagebox.showwarning("Aviso", "Selecciona al menos una empresa")
            return

        ids = [empresas[i]["id_empresa"] for i in seleccion]
        emails_actuales = obtener_emails_empresas(ids)

        text_resultado.delete(1.0, tk.END)
        for e in emails_actuales:
            text_resultado.insert(
                tk.END,
                f"{e['empresa']} | {e['tipo']} | {e['email']}\n"
            )

    def abrir_envio_email():
        if not emails_actuales:
            messagebox.showwarning("Aviso", "Primero consulta los emails")
            return

        win = tk.Toplevel(root)
        win.title("Enviar Email")
        win.geometry("500x500")

        ttk.Label(win, text="Destinatarios:").pack(anchor="w", padx=10)

        listbox_emails = tk.Listbox(win, selectmode=tk.MULTIPLE, height=8)
        listbox_emails.pack(fill="x", padx=10)

        for e in emails_actuales:
            listbox_emails.insert(
                tk.END,
                f"{e['empresa']} | {e['email']}"
            )

        ttk.Label(win, text="Asunto:").pack(anchor="w", padx=10, pady=(10, 0))
        entry_asunto = ttk.Entry(win)
        entry_asunto.insert(0, "Contacto JMOrdenadores")
        entry_asunto.pack(fill="x", padx=10)

        ttk.Label(win, text="Mensaje:").pack(anchor="w", padx=10, pady=(10, 0))
        text_cuerpo = tk.Text(win, height=12)
        text_cuerpo.insert("1.0", PLANTILLA_EMAIL)
        text_cuerpo.pack(fill="both", expand=True, padx=10)

        def enviar():
            seleccion = listbox_emails.curselection()
            if not seleccion:
                messagebox.showwarning("Aviso", "Selecciona al menos un email")
                return

            asunto = entry_asunto.get().strip()
            cuerpo = text_cuerpo.get("1.0", tk.END)

            errores = []

            for i in seleccion:
                email = emails_actuales[i]["email"]
                try:
                    enviar_email(email, asunto, cuerpo)
                except Exception as e:
                    errores.append(f"{email}: {e}")

            if errores:
                messagebox.showerror("Errores", "\n".join(errores))
            else:
                messagebox.showinfo("OK", "Emails enviados correctamente")
                win.destroy()

        ttk.Button(win, text="Enviar emails", command=enviar).pack(pady=10)

    root = tk.Tk()
    root.title("Consultor de Empresas")
    root.geometry("700x500")

    frame = ttk.Frame(root, padding=10)
    frame.pack(fill="both", expand=True)

    ttk.Button(frame, text="Cargar empresas", command=cargar_empresas).pack()

    listbox_empresas = tk.Listbox(frame, selectmode=tk.MULTIPLE, height=12)
    listbox_empresas.pack(fill="x", pady=10)

    ttk.Button(
        frame,
        text="Ver emails de seleccionadas",
        command=ver_emails
    ).pack(pady=5)

    ttk.Button(
        frame,
        text="📧 Enviar email",
        command=abrir_envio_email
    ).pack(pady=5)

    text_resultado = tk.Text(frame, height=10)
    text_resultado.pack(fill="both", expand=True)

    root.mainloop()

# ---------------- MAIN ----------------

if __name__ == "__main__":
    lanzar_gui()
