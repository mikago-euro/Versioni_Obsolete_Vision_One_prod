#!/usr/bin/env python3
import os, mysql.connector, sys, smtplib, logging, ssl
from datetime import datetime
from openpyxl import Workbook
from mysql.connector import Error
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv("/srv/Progetti_Pyhton/Versioni_Obsolete_Vision_One_prod/.Controllo_versioni.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# Dichiarazione variabili di ambiente
# Configurazione SMTP email (relay senza autenticazione, filtrato per IP)
SMTP_SERVER    = os.getenv("SMTP_SERVER")
SMTP_PORT     = int(os.getenv("SMTP_PORT"))
SMTP_USER     = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_STARTTLS  = (os.getenv("SMTP_STARTTLS") or "").strip().lower() in {"1", "true", "yes", "on"}
SMTP_VERIFY_TLS = (os.getenv("SMTP_VERIFY_TLS") or "1").strip().lower() in {"1", "true", "yes", "on"}
SMTP_ALLOW_INSECURE_FALLBACK = (os.getenv("SMTP_ALLOW_INSECURE_FALLBACK") or "1").strip().lower() in {"1", "true", "yes", "on"}
SMTP_MODE      = (os.getenv("SMTP_MODE") or "auto").strip().lower()  # auto|starttls|ssl|plain
SMTP_TIMEOUT   = int((os.getenv("SMTP_TIMEOUT") or "20").strip())
SMTP_DEBUG     = (os.getenv("SMTP_DEBUG") or "0").strip().lower() in {"1", "true", "yes", "on"}
SMTP_CA_FILE   = (os.getenv("SMTP_CA_FILE") or "/usr/local/share/ca-certificates/relay_chain.pem").strip()
SMTP_ENVELOPE_FROM = (os.getenv("SMTP_ENVELOPE_FROM") or "").strip()
EMAIL_FROM     = os.getenv("EMAIL_FROM")
DESTINATARI    = os.getenv("DESTINATARI")
DBUSER=os.getenv("USER")
DBNAME=os.getenv("DATABASE")

# Configurazione MySQL database
DB_CONFIG = {
    "unix_socket": "/var/run/mysqld/mysqld.sock",
    "user":        DBUSER,
    "database":    DBNAME,
    "autocommit":  False
}

def _build_tls_context(verify_tls: bool) -> ssl.SSLContext:
    context = ssl.create_default_context()
    if not verify_tls:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    if SMTP_CA_FILE and os.path.exists(SMTP_CA_FILE):
        context.load_verify_locations(cafile=SMTP_CA_FILE)
    else:
        logging.warning("CA file non trovato (%s); uso i CA di sistema", SMTP_CA_FILE)
    return context


def _resolve_smtp_mode() -> str:
    if SMTP_MODE in {"starttls", "ssl", "plain"}:
        return SMTP_MODE
    if SMTP_PORT == 465:
        return "ssl"
    if SMTP_PORT in {587, 25}:
        return "starttls" if SMTP_STARTTLS else "plain"
    return "plain"


def send_email(
    subject: str,
    body_text: str,
    *,
    rcpt: list[str] | str,
    body_html: str | None = None,
    attachments: list[str] | None = None,
    timeout: int = SMTP_TIMEOUT,
):
    """Invia una email e ritorna True se il relay accetta almeno un destinatario."""
    subject = subject.strip()
    if isinstance(rcpt, str):
        rcpt = [item.strip() for item in rcpt.split(",") if item.strip()]
    if not rcpt:
        raise ValueError("Nessun destinatario valido configurato per l'invio email")

    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(rcpt)
    msg["Subject"] = subject

    msg.set_content(body_text, subtype="plain", charset="utf-8")
    if body_html:
        msg.add_alternative(body_html, subtype="html")
    if attachments:
        for attachment_path in attachments:
            with open(attachment_path, "rb") as attachment_file:
                attachment_data = attachment_file.read()
            filename = os.path.basename(attachment_path)
            msg.add_attachment(
                attachment_data,
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=filename,
            )

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=timeout) as server:
            server.ehlo()
            if SMTP_STARTTLS:
                context = ssl.create_default_context()
                try:
                    context.load_verify_locations(cafile="/usr/local/share/ca-certificates/relay_chain.pem")
                except Exception:
                    pass
                server.starttls(context=context)
                server.ehlo()
            server.send_message(msg)
    except Exception as e:
        logging.error(f"Errore invio e-mail: {e}", exc_info=True)

def connect_to_mysql():
    """Esegue la connessione al database MySQL e restituisce l'oggetto connection."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("Connessione al database MySQL stabilita con successo")
        return connection
    except Error as e:
        print(f"Errore durante la connessione a MySQL: {e}")
        return None

def main():
    conn = connect_to_mysql()
    if not conn:
        sys.exit(1)
    
    customers_query = "SELECT customer_name, api_url FROM customers"
    cursor = conn.cursor()
    print(f"customers_query: {customers_query}")
    cursor.execute(customers_query)
    customers = cursor.fetchall()
    cursor.close()

    if not customers:
        print("Nessun cliente trovato nella tabella customers.")
        return

    def safe_filename(value):
        return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value).strip("_")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for customer_name, api_key in customers:
        cursor = conn.cursor()
        agents_query = (
            "SELECT DISTINCT clientProgram FROM agents WHERE api_url = %s ORDER BY clientProgram DESC"
        )
        print(f"agents_query: {agents_query} params: {(api_key,)}")
        cursor.execute(agents_query, (api_key,))
        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            print(f"Nessun agent trovato per il cliente {customer_name}.")
            continue

        def version_key(version):
            parts = version.split(".")
            return tuple(int(part) for part in parts)

        client_programs = sorted({row[0] for row in rows if row[0] is not None}, key=version_key)
        print(f"Versioni trovate per {customer_name}: {', '.join(client_programs)}")
        highest_three_client_programs = client_programs[-3:] if len(client_programs) >= 3 else client_programs
        print(
            f"Versioni escluse per {customer_name}: "
            f"{', '.join(highest_three_client_programs) if highest_three_client_programs else 'Nessuna'}"
        )
        placeholders = ", ".join(["%s"] * len(highest_three_client_programs))
        exclusions_clause = f"AND clientProgram NOT IN ({placeholders})" if placeholders else ""
        details_query = (
            "SELECT endpointHost, endpointIP, logonUser, platform, clientProgram, lastConnected "
            "FROM agents "
            f"WHERE api_url = %s AND clientProgram IS NOT NULL {exclusions_clause}"
        )

        cursor = conn.cursor()
        params = (api_key, *highest_three_client_programs)
        print(f"details_query: {details_query} params: {params}")
        cursor.execute(details_query, params)
        details_rows = cursor.fetchall()
        cursor.close()

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "Client Data"
        headers = [
            "customer_name",
            "endpointHost",
            "endpointIP",
            "logonUser",
            "platform",
            "clientProgram",
            "lastConnected",
        ]
        sheet.append(headers)
        for row in details_rows:
            sheet.append([customer_name, *row])

        safe_customer_name = safe_filename(str(customer_name)) or "cliente_senza_nome"
        output_file = f"client_data_{safe_customer_name}_{timestamp}.xlsx"
        workbook.save(output_file)
        print(f"File Excel creato per {customer_name}: {output_file}")

        email_subject = f"Report versioni {customer_name}"
        email_body = (
            f"Ciao,\n\n"
            f"in allegato trovi il report versioni per il cliente {customer_name}.\n\n"
            f"Ciao"
        )
        destinatari_list = [item.strip() for item in DESTINATARI.split(",") if item.strip()]
        send_email(
            email_subject,
            email_body,
            rcpt=destinatari_list,
            attachments=[output_file],
        )

    conn.close()


if __name__ == "__main__":
    main()
