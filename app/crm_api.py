"""API du CRM cible (simule Salesforce et sa façade REST).

- Authentification OAuth2 « client credentials » : POST /oauth/token
- Comptes : POST / GET / PATCH /accounts
- Chaque création ou modification publie un événement sur le topic Kafka crm.account.v1
"""
import json, os, re, secrets, time, uuid
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from fastapi import FastAPI, Form, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common import conn, new_id

TOPIC = "crm.account.v1"
CLIENTS = {"client-rw": ("secret-rw", "write"), "client-ro": ("secret-ro", "read")}
EXPIRED_TOKEN = "jeton-expire-0000"          # jeton volontairement expiré, pour le test API-005
TOKENS = {EXPIRED_TOKEN: ("write", 0)}       # jeton -> (droit, date d'expiration)
ID_RE = re.compile(r"^001[A-Za-z0-9]{15}$")

app = FastAPI(title="CRM cible - API comptes", version="1.0",
              description="Simulation du CRM cible. Clients OAuth : client-rw / secret-rw (écriture), client-ro / secret-ro (lecture seule).")
_producer = None


def producer():
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "enable.idempotence": True})
    return _producer


class ApiError(Exception):
    def __init__(self, status, code, message, **extra):
        self.status, self.body = status, {"code": code, "message": message, **extra}


@app.exception_handler(ApiError)
def _api_error(_: Request, e: ApiError):
    return JSONResponse(status_code=e.status, content=e.body)


@app.exception_handler(RequestValidationError)
def _validation(_: Request, e: RequestValidationError):
    err = e.errors()[0]
    field = str(err["loc"][-1])
    code = "CHAMP_OBLIGATOIRE" if err["type"] == "missing" else "VALEUR_INVALIDE"
    msg = "obligatoire" if err["type"] == "missing" else "valeur invalide"
    return JSONResponse(status_code=400, content={"code": code, "message": f"Champ '{field}' : {msg}", "field": field})


class AccountIn(BaseModel):
    name: str
    siret: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    postalCode: Optional[str] = None
    phone: Optional[str] = None
    segment: Optional[str] = None
    status: Optional[str] = "Actif"


class AccountPatch(BaseModel):
    name: Optional[str] = None
    siret: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    postalCode: Optional[str] = None
    phone: Optional[str] = None
    segment: Optional[str] = None
    status: Optional[str] = None
    encours: Optional[float] = None


FIELDS = {"name": "NAME", "siret": "SIRET__C", "street": "BILLINGSTREET", "city": "BILLINGCITY", "postalCode": "BILLINGPOSTALCODE",
          "phone": "PHONE", "segment": "SEGMENT__C", "status": "STATUS__C", "encours": "ENCOURS__C"}
SELECT = "SELECT ID, SIEBEL_ID__C, " + ", ".join(FIELDS.values()) + " FROM CRM.ACCOUNT"


def to_json(row):
    d = {"id": row[0], "siebelId": row[1]}
    for i, k in enumerate(FIELDS):
        v = row[i + 2]
        d[k] = float(v) if k == "encours" and v is not None else v
    return d


def auth(authorization, need):
    if not authorization or not authorization.startswith("Bearer "):
        raise ApiError(401, "NON_AUTHENTIFIE", "Jeton d'accès absent")
    tok = TOKENS.get(authorization[7:])
    if not tok:
        raise ApiError(401, "NON_AUTHENTIFIE", "Jeton d'accès invalide")
    if tok[1] < time.time():
        raise ApiError(401, "JETON_EXPIRE", "Jeton d'accès expiré")
    if need == "write" and tok[0] != "write":
        raise ApiError(403, "ACCES_REFUSE", "Ce client n'est pas autorisé à modifier les comptes")


def publish(event_type, account, source):
    event = {"eventId": str(uuid.uuid4()), "eventType": event_type,
             "eventTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
             "source": source, "account": account}
    p = producer()
    p.produce(TOPIC, key=account["id"], value=json.dumps(event, ensure_ascii=False).encode(),
              headers=[("eventType", event_type.encode()), ("source", source.encode())])
    p.flush(10)


def fetch(cur, account_id):
    if not ID_RE.match(account_id):
        raise ApiError(400, "IDENTIFIANT_INVALIDE", "L'identifiant de compte doit faire 18 caractères et commencer par 001")
    cur.execute(SELECT + " WHERE ID = :1", [account_id])
    row = cur.fetchone()
    if not row:
        raise ApiError(404, "COMPTE_INTROUVABLE", f"Aucun compte avec l'identifiant {account_id}")
    return to_json(row)


@app.post("/oauth/token", tags=["Authentification"])
def token(grant_type: str = Form(...), client_id: str = Form(...), client_secret: str = Form(...)):
    c = CLIENTS.get(client_id)
    if grant_type != "client_credentials" or not c or c[0] != client_secret:
        raise ApiError(401, "CLIENT_INVALIDE", "Identifiants client invalides")
    t = secrets.token_urlsafe(32)
    TOKENS[t] = (c[1], time.time() + 3600)
    return {"access_token": t, "token_type": "Bearer", "expires_in": 3600, "scope": c[1]}


@app.post("/accounts", status_code=201, tags=["Comptes"])
def create_account(body: AccountIn, authorization: Optional[str] = Header(None), x_source: Optional[str] = Header(None)):
    auth(authorization, "write")
    if not body.name.strip():
        raise ApiError(400, "CHAMP_OBLIGATOIRE", "Champ 'name' : obligatoire", field="name")
    with conn() as c:
        cur = c.cursor()
        if body.siret:
            cur.execute("SELECT ID FROM CRM.ACCOUNT WHERE SIRET__C = :1", [body.siret])
            dup = cur.fetchone()
            if dup:
                raise ApiError(409, "DOUBLON_SIRET", f"Un compte existe déjà avec le SIRET {body.siret}", existingId=dup[0])
        aid = new_id("001")
        data = body.model_dump()
        cols = [FIELDS[k] for k in data]
        cur.execute(f"INSERT INTO CRM.ACCOUNT (ID, {', '.join(cols)}) VALUES (:1, {', '.join(f':{i+2}' for i in range(len(cols)))})",
                    [aid] + list(data.values()))
        c.commit()
        acc = fetch(cur, aid)
    publish("AccountCreated", acc, x_source or "crm")
    return acc


@app.get("/accounts/{account_id}", tags=["Comptes"])
def get_account(account_id: str, authorization: Optional[str] = Header(None)):
    auth(authorization, "read")
    with conn() as c:
        return fetch(c.cursor(), account_id)


@app.get("/accounts", tags=["Comptes"])
def search_accounts(siret: Optional[str] = None, siebelId: Optional[str] = None, name: Optional[str] = None,
                    authorization: Optional[str] = Header(None)):
    auth(authorization, "read")
    where, binds = [], []
    for col, val in (("SIRET__C", siret), ("SIEBEL_ID__C", siebelId)):
        if val:
            where.append(f"{col} = :{len(binds)+1}"); binds.append(val)
    if name:
        where.append(f"UPPER(NAME) LIKE :{len(binds)+1}"); binds.append(f"%{name.upper()}%")
    if not where:
        raise ApiError(400, "CRITERE_MANQUANT", "Indiquer au moins un critère : siret, siebelId ou name")
    with conn() as c:
        cur = c.cursor()
        cur.execute(SELECT + " WHERE " + " AND ".join(where) + " FETCH FIRST 50 ROWS ONLY", binds)
        return [to_json(r) for r in cur.fetchall()]


@app.patch("/accounts/{account_id}", tags=["Comptes"])
def patch_account(account_id: str, body: AccountPatch, authorization: Optional[str] = Header(None),
                  x_source: Optional[str] = Header(None)):
    auth(authorization, "write")
    with conn() as c:
        cur = c.cursor()
        before = fetch(cur, account_id)
        changes = {k: v for k, v in body.model_dump(exclude_unset=True).items() if before.get(k) != v}
        if "name" in changes and not (changes["name"] or "").strip():
            raise ApiError(400, "CHAMP_OBLIGATOIRE", "Champ 'name' : obligatoire", field="name")
        if not changes:
            return before                                   # aucune modification réelle : pas d'événement
        sets = ", ".join(f"{FIELDS[k]} = :{i+1}" for i, k in enumerate(changes))
        cur.execute(f"UPDATE CRM.ACCOUNT SET {sets}, LASTMODIFIEDDATE = SYSDATE WHERE ID = :{len(changes)+1}",
                    list(changes.values()) + [account_id])
        c.commit()
        acc = fetch(cur, account_id)
    publish("AccountUpdated", acc, x_source or "crm")
    return acc


@app.get("/health", tags=["Technique"])
def health():
    with conn() as c:
        c.cursor().execute("SELECT 1 FROM DUAL")
    return {"status": "UP"}
