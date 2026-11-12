"""Faux ERP : application aval qui consomme les événements comptes du CRM.

- Consomme crm.account.v1 (groupe « erp-mock ») et tient à jour la table ERP.CUSTOMER
- Message invalide  -> envoyé sur crm.account.v1.dlq, le consommateur continue
- Message en double -> ignoré (contrôle sur eventId)
- PUT /customers/{id}/encours : met à jour l'encours et le renvoie au CRM (flux retour)
"""
import json, os, threading, time
from typing import Optional

import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common import conn

TOPIC, DLQ, GROUP = "crm.account.v1", "crm.account.v1.dlq", "erp-mock"
CRM = os.environ.get("CRM_URL", "http://crm-api:8000")
app = FastAPI(title="Faux ERP", version="1.0", description="Application aval simulée. Consomme le topic crm.account.v1.")
state = {"running": False, "last_error": None}
stop = threading.Event()


def log(cur, event_id, crm_id, outcome, detail=""):
    cur.execute("INSERT INTO ERP.EVENT_LOG (EVENT_ID, CRM_ID, OUTCOME, DETAIL) VALUES (:1, :2, :3, :4)",
                [str(event_id)[:60] if event_id else None, str(crm_id)[:18] if crm_id else None, outcome, detail[:500]])


def handle(raw, dlq):
    """Traite un message. Retourne le résultat (pour le journal)."""
    with conn() as c:
        cur = c.cursor()
        try:
            ev = json.loads(raw.value().decode("utf-8"))
            acc = ev.get("account") or {}
            missing = [f for f, v in (("eventId", ev.get("eventId")), ("eventType", ev.get("eventType")), ("account.id", acc.get("id"))) if not v]
            if missing:
                raise ValueError("Champ(s) obligatoire(s) absent(s) : " + ", ".join(missing))
        except Exception as e:
            dlq.produce(DLQ, key=raw.key(), value=raw.value(),
                        headers=[("erreur", str(e).encode()), ("partition", str(raw.partition()).encode()), ("offset", str(raw.offset()).encode())])
            dlq.flush(10)
            log(cur, None, None, "INVALIDE_VERS_DLQ", str(e)); c.commit()
            return
        eid, aid = ev["eventId"], acc["id"]
        cur.execute("SELECT 1 FROM ERP.PROCESSED_EVENT WHERE EVENT_ID = :1", [eid])
        if cur.fetchone():
            log(cur, eid, aid, "DOUBLON_IGNORE", "eventId déjà traité"); c.commit()
            return
        if ev.get("source") == "erp":
            log(cur, eid, aid, "IGNORE_ORIGINE_ERP", "modification venue de l'ERP : pas de boucle")
        else:
            cur.execute("""MERGE INTO ERP.CUSTOMER t USING (SELECT :id id FROM DUAL) s ON (t.CRM_ID = s.id)
                WHEN MATCHED THEN UPDATE SET NAME = :name, SIRET = :siret, CITY = :city, POSTAL_CODE = :cp, PHONE = :phone,
                     UPDATE_COUNT = UPDATE_COUNT + 1, LAST_EVENT_AT = SYSDATE
                WHEN NOT MATCHED THEN INSERT (CRM_ID, NAME, SIRET, CITY, POSTAL_CODE, PHONE, UPDATE_COUNT, LAST_EVENT_AT)
                     VALUES (:id, :name, :siret, :city, :cp, :phone, 1, SYSDATE)""",
                        {"id": aid, "name": acc.get("name"), "siret": acc.get("siret"), "city": acc.get("city"),
                         "cp": acc.get("postalCode"), "phone": acc.get("phone")})
            log(cur, eid, aid, "TRAITE", ev["eventType"])
        cur.execute("INSERT INTO ERP.PROCESSED_EVENT (EVENT_ID) VALUES (:1)", [eid])
        c.commit()


def consume():
    boot = os.environ["KAFKA_BOOTSTRAP"]
    consumer = Consumer({"bootstrap.servers": boot, "group.id": GROUP, "auto.offset.reset": "earliest",
                         "enable.auto.commit": False, "allow.auto.create.topics": True,
                         "session.timeout.ms": 10000})
    dlq = Producer({"bootstrap.servers": boot})
    consumer.subscribe([TOPIC])
    state["running"] = True
    while not stop.is_set():
        msg = None
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                state["last_error"] = str(msg.error())
                continue
            handle(msg, dlq)
            consumer.commit(message=msg, asynchronous=False)   # l'offset n'avance qu'après traitement
            state["last_error"] = None
        except Exception as e:                                  # erreur technique : on rejoue le même message
            state["last_error"] = str(e)
            time.sleep(2)
            if msg is not None and not msg.error():
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))


    consumer.close()                                            # quitte proprement le groupe de consommateurs
    state["running"] = False


@app.on_event("startup")
def start():
    worker.start()


@app.on_event("shutdown")
def shutdown():
    stop.set()
    worker.join(timeout=8)


worker = threading.Thread(target=consume, daemon=True)


def customer(row):
    return {"crmId": row[0], "name": row[1], "siret": row[2], "city": row[3], "postalCode": row[4], "phone": row[5],
            "encours": float(row[6]) if row[6] is not None else None, "updateCount": row[7],
            "lastEventAt": row[8].isoformat() if row[8] else None}


SEL = "SELECT CRM_ID, NAME, SIRET, CITY, POSTAL_CODE, PHONE, ENCOURS, UPDATE_COUNT, LAST_EVENT_AT FROM ERP.CUSTOMER"


@app.get("/customers", tags=["Clients"])
def list_customers(siret: Optional[str] = None):
    with conn() as c:
        cur = c.cursor()
        if siret:
            cur.execute(SEL + " WHERE SIRET = :1", [siret])
        else:
            cur.execute(SEL + " ORDER BY LAST_EVENT_AT DESC FETCH FIRST 50 ROWS ONLY")
        return [customer(r) for r in cur.fetchall()]


@app.get("/customers/{crm_id}", tags=["Clients"])
def get_customer(crm_id: str):
    with conn() as c:
        cur = c.cursor()
        cur.execute(SEL + " WHERE CRM_ID = :1", [crm_id])
        row = cur.fetchone()
    if not row:
        return JSONResponse(status_code=404, content={"code": "CLIENT_INTROUVABLE", "message": f"Client {crm_id} inconnu de l'ERP"})
    return customer(row)


class Encours(BaseModel):
    amount: float


@app.put("/customers/{crm_id}/encours", tags=["Clients"])
def set_encours(crm_id: str, body: Encours):
    """Met à jour l'encours dans l'ERP puis le pousse vers le CRM (flux retour ERP -> CRM)."""
    with conn() as c:
        cur = c.cursor()
        cur.execute("UPDATE ERP.CUSTOMER SET ENCOURS = :1 WHERE CRM_ID = :2", [body.amount, crm_id])
        if cur.rowcount == 0:
            return JSONResponse(status_code=404, content={"code": "CLIENT_INTROUVABLE", "message": f"Client {crm_id} inconnu de l'ERP"})
        c.commit()
    tok = requests.post(f"{CRM}/oauth/token", data={"grant_type": "client_credentials", "client_id": "client-rw",
                                                   "client_secret": "secret-rw"}, timeout=10).json()["access_token"]
    r = requests.patch(f"{CRM}/accounts/{crm_id}", json={"encours": body.amount},
                       headers={"Authorization": f"Bearer {tok}", "X-Source": "erp"}, timeout=10)
    return {"crmId": crm_id, "encours": body.amount, "retourCrm": r.status_code}


@app.get("/events/log", tags=["Supervision"])
def events_log(limit: int = 30):
    """Journal du consommateur : TRAITE, DOUBLON_IGNORE, INVALIDE_VERS_DLQ, IGNORE_ORIGINE_ERP."""
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT LOGGED_AT, EVENT_ID, CRM_ID, OUTCOME, DETAIL FROM ERP.EVENT_LOG ORDER BY ID DESC FETCH FIRST :1 ROWS ONLY", [limit])
        return [{"at": r[0].isoformat(), "eventId": r[1], "crmId": r[2], "outcome": r[3], "detail": r[4]} for r in cur.fetchall()]


@app.get("/health", tags=["Supervision"])
def health():
    return {"status": "UP", "consommateur": state}
