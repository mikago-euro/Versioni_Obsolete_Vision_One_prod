#!/usr/bin/env python3
import os, mysql.connector, sys, smtplib, logging, ssl, json
from datetime import datetime
from openpyxl import Workbook
from mysql.connector import Error
from email.message import EmailMessage
from email.utils import parseaddr
from dotenv import load_dotenv

load_dotenv("/srv/Progetti_Pyhton/Versioni_Obsolete_Vision_One_prod/.Controllo_versioni.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
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
SMTP_CA_FILE   = (os.getenv("SMTP_CA_FILE") or "/usr/local/share/ca-certificates/relay_chain.pem").strip()
SMTP_ENVELOPE_FROM = (os.getenv("SMTP_ENVELOPE_FROM") or "").strip()
EMAIL_FROM     = os.getenv("EMAIL_FROM")
DESTINATARI    = os.getenv("DESTINATARI")
DBUSER=os.getenv("USER")
DBNAME=os.getenv("DATABASE")

#77 Commento test


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




def _unwrap_quoted_text(value: str) -> str:
    """Rimuove un eventuale wrapper di apici esterni preservando il contenuto interno."""
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"\"", "'"}:
        inner = text[1:-1].strip()
        if inner:
            return inner
    return text


def _is_valid_email_address(candidate: str) -> bool:
    _, parsed = parseaddr(candidate)
    return bool(parsed) and parsed == candidate and "@" in parsed

def _parse_recipients(raw_recipients: list[str] | str | None) -> list[str]:
    """Normalizza i destinatari supportando CSV e array JSON-like."""
    if raw_recipients is None:
        return []

    if isinstance(raw_recipients, list):
        candidates = raw_recipients
    else:
        raw_text = _unwrap_quoted_text(str(raw_recipients).strip())
        if not raw_text:
            return []

        candidates = None
        if raw_text.startswith("[") and raw_text.endswith("]"):
            try:
                parsed = json.loads(raw_text)
                if isinstance(parsed, list):
                    candidates = parsed
            except json.JSONDecodeError:
                candidates = None

        if candidates is None:
            normalized = raw_text.replace(";", ",").replace("[", "").replace("]", "")
            candidates = normalized.split(",")

    cleaned: list[str] = []
    for item in candidates:
        recipient = str(item).strip().strip('\"').strip("'")
        if not recipient:
            continue
        if _is_valid_email_address(recipient):
            cleaned.append(recipient)
            continue
        logging.warning("Destinatario non valido ignorato: %s", recipient)
    return cleaned


def _resolve_recipients_for_customer(raw_recipients: str | None, customer_name: str) -> list[str]:
    """Restituisce i destinatari per cliente da JSON object o fallback statico."""
    if raw_recipients is None:
        return []

    raw_text = _unwrap_quoted_text(str(raw_recipients).strip())
    if not raw_text:
        return []

    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            customer_map = {str(key).strip().lower(): value for key, value in parsed.items()}
            customer_recipients = customer_map.get(str(customer_name).strip().lower())
            if customer_recipients is None:
                return []
            return _parse_recipients(customer_recipients)
    except json.JSONDecodeError:
        logging.warning("DESTINATARI non contiene JSON valido, uso il parsing statico")

    return _parse_recipients(raw_text)


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
    rcpt = _parse_recipients(rcpt)
    if not SMTP_SERVER or not SMTP_PORT:
        raise ValueError("SMTP_SERVER/SMTP_PORT non configurati")
    if not EMAIL_FROM:
        raise ValueError("EMAIL_FROM non configurato")
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

    mode = _resolve_smtp_mode()
    envelope_from = SMTP_ENVELOPE_FROM or EMAIL_FROM

    def _send_once(verify_tls: bool):
        if mode == "ssl":
            context = _build_tls_context(verify_tls)
            server_ctx = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=timeout, context=context)
        else:
            server_ctx = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=timeout)

        with server_ctx as server:
            server.ehlo()

            if mode == "starttls":
                tls_context = _build_tls_context(verify_tls)
                server.starttls(context=tls_context)
                server.ehlo()

            if SMTP_USER and SMTP_PASSWORD:
                server.login(SMTP_USER, SMTP_PASSWORD)

            send_resp = server.send_message(msg, from_addr=envelope_from, to_addrs=rcpt)
            return send_resp

    try:
        refused_recipients = _send_once(SMTP_VERIFY_TLS)
    except ssl.SSLError:
        if not SMTP_VERIFY_TLS or not SMTP_ALLOW_INSECURE_FALLBACK:
            logging.exception("Errore SSL/TLS durante invio SMTP")
            raise
        logging.warning("Errore verifica TLS; ritento con verifica disabilitata")
        refused_recipients = _send_once(False)
    except Exception:
        logging.exception("Errore SMTP non gestito durante invio")
        raise

    if refused_recipients:
        logging.error("Destinatari rifiutati dal relay: %s", refused_recipients)
        for recipient, smtp_error in refused_recipients.items():
            try:
                smtp_code, smtp_message = smtp_error
            except Exception:
                smtp_code, smtp_message = "?", smtp_error

            if isinstance(smtp_message, bytes):
                smtp_message = smtp_message.decode("utf-8", errors="replace")

            logging.error(
                "Dettaglio destinatario rifiutato: %s -> codice=%s messaggio=%s",
                recipient,
                smtp_code,
                smtp_message,
            )
        return False

    logging.info("E-mail accettata dal relay per destinatari: %s", ", ".join(rcpt))
    return True

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
        highest_three_client_programs = client_programs[-3:] if len(client_programs) >= 3 else client_programs
        placeholders = ", ".join(["%s"] * len(highest_three_client_programs))
        exclusions_clause = f"AND clientProgram NOT IN ({placeholders})" if placeholders else ""
        details_query = (
            "SELECT endpointHost, endpointIP, logonUser, platform, clientProgram, lastConnected "
            "FROM agents "
            f"WHERE api_url = %s AND clientProgram IS NOT NULL {exclusions_clause}"
        )

        cursor = conn.cursor()
        params = (api_key, *highest_three_client_programs)
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
        destinatari_list = _resolve_recipients_for_customer(DESTINATARI, customer_name)
        if not destinatari_list:
            logging.warning(
                "Nessun destinatario configurato per il cliente %s. Invio email saltato.",
                customer_name,
            )
            continue
        try:
            sent = send_email(
                email_subject,
                email_body,
                rcpt=destinatari_list,
                attachments=[output_file],
            )
            if not sent:
                print(
                    f"ATTENZIONE: e-mail non consegnata completamente dal relay per {customer_name}. "
                    "Controlla i log 'Destinatari rifiutati' per codice e motivo SMTP."
                )
        except Exception as e:
            logging.error("Errore invio e-mail per %s: %s", customer_name, e, exc_info=True)

    conn.close()


if __name__ == "__main__":
    main()
