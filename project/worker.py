import os
import asyncio
import logging
import logging.handlers
import json
import sys
sys.stdout.reconfigure(encoding='utf-8')
from dotenv import load_dotenv
from azure.servicebus.aio import ServiceBusClient
from datetime import datetime
import smtplib
from email.message import EmailMessage

load_dotenv()


# ========== CONFIG ==========
SERVICE_BUS_CONN = os.getenv("SERVICE_BUS_CONNECTION_STR")
TOPIC_NAME = os.getenv("TOPIC_NAME", "orders")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "billing")

# Mailtrap
MAIL_USER = os.getenv("MAILTRAP_USER")
MAIL_PASS = os.getenv("MAILTRAP_PASS")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@example.com")
MAIL_TO = os.getenv("MAIL_TO", "recipient@example.com")
MAIL_HOST = "smtp.mailtrap.io"
MAIL_PORT = 2525

LOG_FILE = os.getenv("LOG_FILE", "order_processor.log")

# ========== LOGGING ==========
logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

fh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

processed = set()

# ========== MAIL SENDER ==========
async def send_email_mailtrap(subject: str, body: str):
    if not (MAIL_USER and MAIL_PASS):
        logger.warning("Mailtrap credentials not set; skipping email")
        return
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        with smtplib.SMTP(MAIL_HOST, MAIL_PORT, timeout=10) as s:
            s.login(MAIL_USER, MAIL_PASS)
            s.send_message(msg)
        logger.info(" Email sent via Mailtrap: %s", subject)
    except Exception as e:
        logger.exception(" Failed to send email: %s", e)

# ========== PROCESS MESSAGE ==========
async def process_message(message):
    try:
        # FIX: message.body is now a generator
        body_bytes = b"".join(message.body)
        body = body_bytes.decode("utf-8")
        payload = json.loads(body)
    except Exception as e:
        logger.exception("Invalid JSON message: %s", e)
        return False

    msg_id = message.message_id or payload.get("orderId", f"unknown-{datetime.utcnow().timestamp()}")
    if msg_id in processed:
        logger.info("Skipping already processed message: %s", msg_id)
        return True

    logger.info("Processing message %s on subscription %s", msg_id, SUBSCRIPTION_NAME)
    logger.info("Payload: %s", payload)

    try:
        if SUBSCRIPTION_NAME == "inventory":
            await asyncio.sleep(0.5)
            logger.info("Reserved inventory for order %s", payload.get("orderId"))

        elif SUBSCRIPTION_NAME == "billing":
            await asyncio.sleep(1.0)
            logger.info("Charged customer for order %s", payload.get("orderId"))
            await send_email_mailtrap(
                subject=f"Order {payload.get('orderId')} - Payment Received",
                body=f"Payment for order {payload.get('orderId')} received successfully."
            )

        elif SUBSCRIPTION_NAME == "shipping":
            await asyncio.sleep(0.5)
            logger.info("Prepared shipment for order %s", payload.get("orderId"))

        processed.add(msg_id)
        return True
    except Exception as e:
        logger.exception("Processing failed for %s: %s", msg_id, e)
        return False

# ========== RUN WORKER ==========
async def run_worker():
    if not SERVICE_BUS_CONN:
        raise RuntimeError("SERVICE_BUS_CONNECTION_STR not set")

    sb_client = ServiceBusClient.from_connection_string(SERVICE_BUS_CONN, logging_enable=False)
    async with sb_client:
        receiver = sb_client.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_NAME)
        async with receiver:
            logger.info(" Started worker for subscription: %s", SUBSCRIPTION_NAME)
            async for msg in receiver:
                try:
                    ok = await process_message(msg)
                    if ok:
                        # FIX: Use receiver.complete_message()
                        await receiver.complete_message(msg)
                        logger.info(" Completed message %s", msg.message_id)
                    else:
                        # FIX: Use receiver.abandon_message()
                        await receiver.abandon_message(msg)
                        logger.info(" Abandoned message %s for retry", msg.message_id)
                except Exception as e:
                    logger.exception("Error handling message: %s", e)
                    try:
                        await receiver.abandon_message(msg)
                    except Exception:
                        pass

if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
