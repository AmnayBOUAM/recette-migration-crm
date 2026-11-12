"""Traitement de reprise Siebel -> CRM cible.

Commandes :
  init    crée les tables et charge les données Siebel de test (ne fait rien si déjà fait)
  reset   remet tout l'environnement dans son état initial (Siebel, staging, CRM, ERP)
  full    reprise complète  : extraction Siebel -> staging, puis chargement staging -> CRM
  delta   reprise delta     : uniquement ce qui a changé dans Siebel depuis la dernière reprise
  load    chargement seul   : traite les lignes du staging à l'état NEW (lignes injectées à la main)
  report  affiche le bilan  : volumes source, cible, rejets, exclusions
"""
import sys
from common import conn, new_id

# ----------------------------------------------------------------------------- DDL
DDL = [
 # --- Siebel (source)
 """CREATE TABLE SIEBEL.S_POSTN (ROW_ID VARCHAR2(15) PRIMARY KEY, NAME VARCHAR2(50), TEAM VARCHAR2(30))""",
 """CREATE TABLE SIEBEL.S_ORG_EXT (ROW_ID VARCHAR2(15) PRIMARY KEY, NAME VARCHAR2(100), X_SIRET VARCHAR2(14),
    ADDR VARCHAR2(200), CITY VARCHAR2(50), ZIPCODE VARCHAR2(10), MAIN_PH_NUM VARCHAR2(20), CUST_STAT_CD VARCHAR2(30),
    OU_TYPE_CD VARCHAR2(30), PR_POSTN_ID VARCHAR2(15), CREATED DATE, LAST_UPD DATE)""",
 """CREATE TABLE SIEBEL.S_CONTACT (ROW_ID VARCHAR2(15) PRIMARY KEY, FST_NAME VARCHAR2(50), LAST_NAME VARCHAR2(80),
    EMAIL_ADDR VARCHAR2(100), PR_DEPT_OU_ID VARCHAR2(15), COMMENTS VARCHAR2(255), PR_POSTN_ID VARCHAR2(15),
    CREATED DATE, LAST_UPD DATE)""",
 """CREATE TABLE SIEBEL.S_OPTY (ROW_ID VARCHAR2(15) PRIMARY KEY, NAME VARCHAR2(100), PR_DEPT_OU_ID VARCHAR2(15),
    SUM_REVN_AMT NUMBER(14,2), CURCY_CD VARCHAR2(3), STG_NAME VARCHAR2(40), SUM_EFFECTIVE_DT DATE,
    CREATED DATE, LAST_UPD DATE)""",
 """CREATE TABLE SIEBEL.S_SRV_REQ (ROW_ID VARCHAR2(15) PRIMARY KEY, SR_NUM VARCHAR2(20), SR_TITLE VARCHAR2(100),
    SR_STAT_ID VARCHAR2(30), CST_OU_ID VARCHAR2(15), CREATED DATE, LAST_UPD DATE)""",
 """CREATE TABLE SIEBEL.S_EVT_ACT (ROW_ID VARCHAR2(15) PRIMARY KEY, SRA_SR_ID VARCHAR2(15), COMMENTS VARCHAR2(255), CREATED DATE)""",
 """CREATE TABLE SIEBEL.S_SR_ATT (ROW_ID VARCHAR2(15) PRIMARY KEY, PAR_ROW_ID VARCHAR2(15), FILE_NAME VARCHAR2(100), FILE_SIZE NUMBER)""",
 # --- Staging (schéma RECETTE)
 """CREATE TABLE STG_ACCOUNT (SIEBEL_ID VARCHAR2(30) PRIMARY KEY, NAME VARCHAR2(100), SIRET VARCHAR2(14), STREET VARCHAR2(200),
    CITY VARCHAR2(50), ZIPCODE VARCHAR2(10), PHONE VARCHAR2(20), STATUS_CD VARCHAR2(30), SEGMENT_CD VARCHAR2(30),
    POSTN_ID VARCHAR2(15), SRC_CREATED DATE, LOAD_STATUS VARCHAR2(10) DEFAULT 'NEW')""",
 """CREATE TABLE STG_CONTACT (SIEBEL_ID VARCHAR2(30) PRIMARY KEY, FIRST_NAME VARCHAR2(50), LAST_NAME VARCHAR2(80),
    EMAIL VARCHAR2(100), ACCOUNT_SIEBEL_ID VARCHAR2(30), COMMENTS VARCHAR2(255), POSTN_ID VARCHAR2(15),
    LOAD_STATUS VARCHAR2(10) DEFAULT 'NEW')""",
 """CREATE TABLE STG_ACCOUNT_LOADED (SIEBEL_ID VARCHAR2(30) PRIMARY KEY, CRM_ID VARCHAR2(18), LOADED_AT DATE)""",
 """CREATE TABLE STG_REJECT (SIEBEL_ID VARCHAR2(30), OBJECT_NAME VARCHAR2(20), ERROR_CODE VARCHAR2(20), ERROR_MSG VARCHAR2(300),
    REJECTED_AT DATE DEFAULT SYSDATE, PRIMARY KEY (SIEBEL_ID, OBJECT_NAME))""",
 """CREATE TABLE STG_EXCLUSION (SIEBEL_ID VARCHAR2(30), OBJECT_NAME VARCHAR2(20), MOTIF VARCHAR2(100),
    EXCLUDED_AT DATE DEFAULT SYSDATE, PRIMARY KEY (SIEBEL_ID, OBJECT_NAME))""",
 """CREATE TABLE STG_MAP (DOMAIN VARCHAR2(20), SRC_VAL VARCHAR2(40), TGT_VAL VARCHAR2(60), PRIMARY KEY (DOMAIN, SRC_VAL))""",
 """CREATE TABLE STG_RUN (RUN_ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, RUN_MODE VARCHAR2(10), STARTED_AT DATE,
    ENDED_AT DATE, SUMMARY VARCHAR2(1000))""",
 # --- CRM cible (noms de champs à la Salesforce)
 """CREATE TABLE CRM.ACCOUNT (ID VARCHAR2(18) PRIMARY KEY, NAME VARCHAR2(100) NOT NULL, SIEBEL_ID__C VARCHAR2(30) UNIQUE,
    SIRET__C VARCHAR2(14), STATUS__C VARCHAR2(30), SEGMENT__C VARCHAR2(30), BILLINGSTREET VARCHAR2(200), BILLINGCITY VARCHAR2(50),
    BILLINGPOSTALCODE VARCHAR2(10), PHONE VARCHAR2(20), ENCOURS__C NUMBER(14,2), OWNER_USERNAME VARCHAR2(60),
    SIEBEL_CREATED_DATE__C DATE, CREATEDDATE DATE DEFAULT SYSDATE, LASTMODIFIEDDATE DATE DEFAULT SYSDATE)""",
 """CREATE TABLE CRM.CONTACT (ID VARCHAR2(18) PRIMARY KEY, SIEBEL_ID__C VARCHAR2(30) UNIQUE, FIRSTNAME VARCHAR2(50),
    LASTNAME VARCHAR2(80) NOT NULL, EMAIL VARCHAR2(100), ACCOUNTID VARCHAR2(18) REFERENCES CRM.ACCOUNT(ID),
    DESCRIPTION VARCHAR2(255), OWNER_USERNAME VARCHAR2(60), CREATEDDATE DATE DEFAULT SYSDATE, LASTMODIFIEDDATE DATE DEFAULT SYSDATE)""",
 """CREATE TABLE CRM.OPPORTUNITY (ID VARCHAR2(18) PRIMARY KEY, SIEBEL_ID__C VARCHAR2(30) UNIQUE, NAME VARCHAR2(100),
    ACCOUNTID VARCHAR2(18) REFERENCES CRM.ACCOUNT(ID), AMOUNT NUMBER(14,2), CURRENCYISOCODE VARCHAR2(3), STAGENAME VARCHAR2(40),
    CLOSEDATE DATE, SIEBEL_CREATED_DATE__C DATE, CREATEDDATE DATE DEFAULT SYSDATE, LASTMODIFIEDDATE DATE DEFAULT SYSDATE)""",
 """CREATE TABLE CRM.CASES (ID VARCHAR2(18) PRIMARY KEY, SIEBEL_ID__C VARCHAR2(30) UNIQUE, CASENUMBER VARCHAR2(20),
    SUBJECT VARCHAR2(100), STATUS VARCHAR2(30), ACCOUNTID VARCHAR2(18) REFERENCES CRM.ACCOUNT(ID),
    SIEBEL_CREATED_DATE__C DATE, CREATEDDATE DATE DEFAULT SYSDATE, LASTMODIFIEDDATE DATE DEFAULT SYSDATE)""",
 """CREATE TABLE CRM.CASE_ACTIVITY (SIEBEL_ID__C VARCHAR2(30) PRIMARY KEY, CASEID VARCHAR2(18) REFERENCES CRM.CASES(ID),
    DESCRIPTION VARCHAR2(255), ACTIVITY_DATE DATE)""",
 """CREATE TABLE CRM.CASE_ATTACHMENT (SIEBEL_ID__C VARCHAR2(30) PRIMARY KEY, CASEID VARCHAR2(18) REFERENCES CRM.CASES(ID),
    FILE_NAME VARCHAR2(100), FILE_SIZE NUMBER)""",
 # --- ERP (application aval)
 """CREATE TABLE ERP.CUSTOMER (CRM_ID VARCHAR2(18) PRIMARY KEY, NAME VARCHAR2(100), SIRET VARCHAR2(14), CITY VARCHAR2(50),
    POSTAL_CODE VARCHAR2(10), PHONE VARCHAR2(20), ENCOURS NUMBER(14,2), UPDATE_COUNT NUMBER DEFAULT 0, LAST_EVENT_AT DATE)""",
 """CREATE TABLE ERP.PROCESSED_EVENT (EVENT_ID VARCHAR2(60) PRIMARY KEY, PROCESSED_AT DATE DEFAULT SYSDATE)""",
 """CREATE TABLE ERP.EVENT_LOG (ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, LOGGED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
    EVENT_ID VARCHAR2(60), CRM_ID VARCHAR2(18), OUTCOME VARCHAR2(30), DETAIL VARCHAR2(500))""",
]

MAPPINGS = [
 ("STATUS", "Active", "Actif"), ("STATUS", "Prospect", "Prospect"), ("STATUS", "Inactive", "Inactif"), ("STATUS", "Suspended", "Suspendu"),
 ("SEGMENT", "SMB", "PME"), ("SEGMENT", "Enterprise", "Grand Compte"), ("SEGMENT", "Individual", "Particulier"),
 ("STAGE", "01 - Prospecting", "Prospection"), ("STAGE", "03 - Qualification", "Qualification"), ("STAGE", "05 - Proposal", "Proposition"),
 ("STAGE", "09 - Closed/Won", "Gagnée"), ("STAGE", "09 - Closed/Lost", "Perdue"),
 ("SR_STATUS", "Open", "Nouveau"), ("SR_STATUS", "Pending", "En cours"), ("SR_STATUS", "Closed", "Fermé"),
 ("POSTN", "P-01", "alice.martin@recette.test"), ("POSTN", "P-02", "bruno.petit@recette.test"),
 ("POSTN", "P-03", "chloe.durand@recette.test"), ("POSTN", "P-04", "david.moreau@recette.test"),
]

SEED = [
 """INSERT INTO SIEBEL.S_POSTN
    SELECT 'P-0'||LEVEL, 'Commercial '||LEVEL, CASE WHEN LEVEL <= 2 THEN 'Equipe A' ELSE 'Equipe B' END FROM DUAL CONNECT BY LEVEL <= 4""",
 # 500 comptes. 1 sur 20 est inactif, dont la moitié sans activité depuis 7 ans (hors périmètre).
 """INSERT INTO SIEBEL.S_ORG_EXT
    SELECT '1-A'||LPAD(i,5,'0'),
           'Société '||DECODE(MOD(i,5),0,'Alpha',1,'Bêta',2,'Gamma',3,'Delta','Oméga')||' '||i,
           TO_CHAR(30000000000000 + i*137),
           i||' rue de la République', DECODE(MOD(i,4),0,'Paris',1,'Lyon',2,'Nantes','Lille'),
           DECODE(MOD(i,4),0,'75001',1,'69003',2,'44000','59000'), '01'||LPAD(i,8,'0'),
           CASE WHEN MOD(i,20)=0 THEN 'Inactive' WHEN MOD(i,7)=0 THEN 'Prospect' WHEN MOD(i,33)=0 THEN 'Suspended' ELSE 'Active' END,
           DECODE(MOD(i,3),0,'SMB',1,'Enterprise','Individual'), 'P-0'||(MOD(i,4)+1),
           DATE '2015-01-01' + i*5,
           CASE WHEN MOD(i,40)=0 THEN ADD_MONTHS(SYSDATE,-84) ELSE SYSDATE - 30 - MOD(i,300) END
    FROM (SELECT LEVEL i FROM DUAL CONNECT BY LEVEL <= 500)""",
 # Anomalies volontaires sur les comptes
 "UPDATE SIEBEL.S_ORG_EXT SET NAME = NULL WHERE ROW_ID IN ('1-A00011','1-A00123','1-A00301')",
 "UPDATE SIEBEL.S_ORG_EXT SET X_SIRET = (SELECT X_SIRET FROM SIEBEL.S_ORG_EXT WHERE ROW_ID='1-A00201') WHERE ROW_ID='1-A00202'",
 "UPDATE SIEBEL.S_ORG_EXT SET X_SIRET = (SELECT X_SIRET FROM SIEBEL.S_ORG_EXT WHERE ROW_ID='1-A00403') WHERE ROW_ID='1-A00404'",
 "UPDATE SIEBEL.S_ORG_EXT SET CUST_STAT_CD = 'Zombie' WHERE ROW_ID IN ('1-A00017','1-A00333')",
 "UPDATE SIEBEL.S_ORG_EXT SET NAME = 'Café Noël & Fils – L''Été d''Açaï' WHERE ROW_ID='1-A00005'",
 "UPDATE SIEBEL.S_ORG_EXT SET NAME = 'Müller-Søren GmbH <Zweigstelle>' WHERE ROW_ID='1-A00006'",
 "UPDATE SIEBEL.S_ORG_EXT SET NAME = RPAD('Compagnie au nom très long ', 100, 'x') WHERE ROW_ID='1-A00009'",
 # 1 200 contacts
 """INSERT INTO SIEBEL.S_CONTACT
    SELECT '1-C'||LPAD(j,5,'0'), DECODE(MOD(j,6),0,'Léa',1,'Hugo',2,'Inès',3,'Théo',4,'Zoé','Raphaël'),
           DECODE(MOD(j,5),0,'Lefèvre',1,'Martin',2,'Bernard',3,'Dubois','Garçon')||' '||j,
           'contact'||j||'@exemple.test', '1-A'||LPAD(MOD(j*7,500)+1,5,'0'), 'Contact n° '||j, 'P-0'||(MOD(j,4)+1),
           DATE '2016-01-01' + j, SYSDATE - 30 - MOD(j,200)
    FROM (SELECT LEVEL j FROM DUAL CONNECT BY LEVEL <= 1200)""",
 "UPDATE SIEBEL.S_CONTACT SET FST_NAME='Noël', LAST_NAME='D''Açàï' WHERE ROW_ID='1-C00001'",
 "UPDATE SIEBEL.S_CONTACT SET FST_NAME='Søren', LAST_NAME='Müller-Østergård' WHERE ROW_ID='1-C00002'",
 "UPDATE SIEBEL.S_CONTACT SET FST_NAME='Patrick', LAST_NAME='O''Brien' WHERE ROW_ID='1-C00003'",
 "UPDATE SIEBEL.S_CONTACT SET LAST_NAME=RPAD('Nom-de-quatre-vingts-caractères-',80,'z') WHERE ROW_ID='1-C00004'",
 "UPDATE SIEBEL.S_CONTACT SET COMMENTS=RPAD('Commentaire de 255 caractères : ',255,'é') WHERE ROW_ID='1-C00005'",
 "UPDATE SIEBEL.S_CONTACT SET FST_NAME='Émoji', LAST_NAME='Test 🚀 OK' WHERE ROW_ID='1-C00006'",
 "UPDATE SIEBEL.S_CONTACT SET LAST_NAME=NULL WHERE ROW_ID='1-C00010'",
 # 800 opportunités, 3 devises, 5 étapes, création proche de minuit pour 1 sur 10
 """INSERT INTO SIEBEL.S_OPTY
    SELECT '1-O'||LPAD(k,5,'0'), 'Opportunité '||k, '1-A'||LPAD(MOD(k*11,500)+1,5,'0'),
           ROUND(1000 + k*123.45, 2), DECODE(MOD(k,10),0,'USD',1,'GBP','EUR'),
           DECODE(MOD(k,5),0,'01 - Prospecting',1,'03 - Qualification',2,'05 - Proposal',3,'09 - Closed/Won','09 - Closed/Lost'),
           DATE '2026-01-31' + MOD(k,330),
           DATE '2024-01-01' + k + CASE WHEN MOD(k,10)=0 THEN (23*60+50)/1440 ELSE 10/24 END, SYSDATE - 30 - MOD(k,100)
    FROM (SELECT LEVEL k FROM DUAL CONNECT BY LEVEL <= 800)""",
 "UPDATE SIEBEL.S_OPTY SET STG_NAME = '07 - Inconnue' WHERE ROW_ID = '1-O00077'",
 # 300 demandes de service, 3 activités chacune, 1 pièce jointe pour 1 sur 2
 """INSERT INTO SIEBEL.S_SRV_REQ
    SELECT '1-S'||LPAD(s,5,'0'), 'SR-'||LPAD(s,6,'0'), 'Demande '||s||' : incident de facturation',
           DECODE(MOD(s,3),0,'Open',1,'Pending','Closed'), '1-A'||LPAD(MOD(s*13,500)+1,5,'0'),
           DATE '2025-01-01' + s, SYSDATE - 30 - MOD(s,60)
    FROM (SELECT LEVEL s FROM DUAL CONNECT BY LEVEL <= 300)""",
 """INSERT INTO SIEBEL.S_EVT_ACT
    SELECT '1-E'||LPAD((s-1)*3+n,5,'0'), '1-S'||LPAD(s,5,'0'), 'Activité '||n||' de la demande '||s, DATE '2025-01-01' + s + n
    FROM (SELECT LEVEL s FROM DUAL CONNECT BY LEVEL <= 300), (SELECT LEVEL n FROM DUAL CONNECT BY LEVEL <= 3)""",
 """INSERT INTO SIEBEL.S_SR_ATT
    SELECT '1-F'||LPAD(s,5,'0'), '1-S'||LPAD(s,5,'0'), 'piece_jointe_'||s||DECODE(MOD(s,4),0,'.pdf','.docx'), 10000 + s*17
    FROM (SELECT LEVEL s FROM DUAL CONNECT BY LEVEL <= 300) WHERE MOD(s,2)=0""",
]

TABLES = ["CRM.CASE_ATTACHMENT", "CRM.CASE_ACTIVITY", "CRM.CASES", "CRM.OPPORTUNITY", "CRM.CONTACT", "CRM.ACCOUNT",
          "ERP.EVENT_LOG", "ERP.PROCESSED_EVENT", "ERP.CUSTOMER",
          "STG_RUN", "STG_MAP", "STG_EXCLUSION", "STG_REJECT", "STG_ACCOUNT_LOADED", "STG_CONTACT", "STG_ACCOUNT",
          "SIEBEL.S_SR_ATT", "SIEBEL.S_EVT_ACT", "SIEBEL.S_SRV_REQ", "SIEBEL.S_OPTY", "SIEBEL.S_CONTACT",
          "SIEBEL.S_ORG_EXT", "SIEBEL.S_POSTN"]


def init(c, force=False):
    cur = c.cursor()
    try:   # la dernière étape du chargement initial a-t-elle abouti ?
        cur.execute("SELECT COUNT(*) FROM SIEBEL.S_SR_ATT")
        done = cur.fetchone()[0] > 0
    except Exception:
        done = False
    if done and not force:
        print("Base déjà initialisée, rien à faire (utiliser 'reset' pour repartir de zéro).")
        return
    for t in TABLES:
        try:
            cur.execute(f"DROP TABLE {t} CASCADE CONSTRAINTS PURGE")
        except Exception:
            pass
    cur.execute("ALTER SESSION SET NLS_LENGTH_SEMANTICS = CHAR")   # longueurs en caractères (accents)
    for s in DDL:
        cur.execute(s)
    cur.executemany("INSERT INTO STG_MAP VALUES (:1, :2, :3)", MAPPINGS)
    for s in SEED:
        cur.execute(s)
    c.commit()
    print("Base initialisée : tables créées et données Siebel de test chargées.")
    report(c)


# ----------------------------------------------------------------------------- reprise
def mapping(cur, domain):
    cur.execute("SELECT SRC_VAL, TGT_VAL FROM STG_MAP WHERE DOMAIN = :1", [domain])
    return dict(cur.fetchall())


def reject(cur, sid, obj, code, msg):
    cur.execute("DELETE FROM STG_REJECT WHERE SIEBEL_ID = :1 AND OBJECT_NAME = :2", [sid, obj])
    cur.execute("INSERT INTO STG_REJECT (SIEBEL_ID, OBJECT_NAME, ERROR_CODE, ERROR_MSG) VALUES (:1, :2, :3, :4)", [sid, obj, code, msg])


def unreject(cur, sid, obj):
    cur.execute("DELETE FROM STG_REJECT WHERE SIEBEL_ID = :1 AND OBJECT_NAME = :2", [sid, obj])


def upsert(cur, table, prefix, sid, values):
    """Création ou mise à jour d'un enregistrement cible, sur la clé SIEBEL_ID__C. Retourne (id, créé ?)."""
    cur.execute(f"SELECT ID FROM {table} WHERE SIEBEL_ID__C = :1", [sid])
    row = cur.fetchone()
    cols = list(values)
    if row:
        sets = ", ".join(f"{k} = :{i+1}" for i, k in enumerate(cols))
        cur.execute(f"UPDATE {table} SET {sets}, LASTMODIFIEDDATE = SYSDATE WHERE ID = :{len(cols)+1}", [values[k] for k in cols] + [row[0]])
        return row[0], False
    rid = new_id(prefix)
    names = ", ".join(["ID", "SIEBEL_ID__C"] + cols)
    binds = ", ".join(f":{i+1}" for i in range(len(cols) + 2))
    cur.execute(f"INSERT INTO {table} ({names}) VALUES ({binds})", [rid, sid] + [values[k] for k in cols])
    return rid, True


def extract(cur, since):
    """Siebel -> staging (comptes et contacts). 'since' = None pour une reprise complète."""
    f = "" if since is None else "AND s.LAST_UPD > :since"
    b = {} if since is None else {"since": since}
    # Règle d'exclusion : compte inactif sans modification depuis plus de 5 ans
    cur.execute(f"""MERGE INTO STG_EXCLUSION x USING (
          SELECT s.ROW_ID FROM SIEBEL.S_ORG_EXT s
          WHERE s.CUST_STAT_CD = 'Inactive' AND s.LAST_UPD < ADD_MONTHS(SYSDATE, -60) {f}) q
        ON (x.SIEBEL_ID = q.ROW_ID AND x.OBJECT_NAME = 'ACCOUNT')
        WHEN NOT MATCHED THEN INSERT (SIEBEL_ID, OBJECT_NAME, MOTIF) VALUES (q.ROW_ID, 'ACCOUNT', 'Inactif > 5 ans')""", b)
    cur.execute(f"""MERGE INTO STG_ACCOUNT t USING (
          SELECT s.* FROM SIEBEL.S_ORG_EXT s
          WHERE NOT (s.CUST_STAT_CD = 'Inactive' AND s.LAST_UPD < ADD_MONTHS(SYSDATE, -60)) {f}) s
        ON (t.SIEBEL_ID = s.ROW_ID)
        WHEN MATCHED THEN UPDATE SET NAME=s.NAME, SIRET=s.X_SIRET, STREET=s.ADDR, CITY=s.CITY, ZIPCODE=s.ZIPCODE, PHONE=s.MAIN_PH_NUM,
             STATUS_CD=s.CUST_STAT_CD, SEGMENT_CD=s.OU_TYPE_CD, POSTN_ID=s.PR_POSTN_ID, SRC_CREATED=s.CREATED, LOAD_STATUS='NEW'
        WHEN NOT MATCHED THEN INSERT (SIEBEL_ID, NAME, SIRET, STREET, CITY, ZIPCODE, PHONE, STATUS_CD, SEGMENT_CD, POSTN_ID, SRC_CREATED, LOAD_STATUS)
             VALUES (s.ROW_ID, s.NAME, s.X_SIRET, s.ADDR, s.CITY, s.ZIPCODE, s.MAIN_PH_NUM, s.CUST_STAT_CD, s.OU_TYPE_CD, s.PR_POSTN_ID, s.CREATED, 'NEW')""", b)
    n_acc = cur.rowcount
    cur.execute(f"""MERGE INTO STG_CONTACT t USING (SELECT s.* FROM SIEBEL.S_CONTACT s WHERE 1=1 {f}) s
        ON (t.SIEBEL_ID = s.ROW_ID)
        WHEN MATCHED THEN UPDATE SET FIRST_NAME=s.FST_NAME, LAST_NAME=s.LAST_NAME, EMAIL=s.EMAIL_ADDR, ACCOUNT_SIEBEL_ID=s.PR_DEPT_OU_ID,
             COMMENTS=s.COMMENTS, POSTN_ID=s.PR_POSTN_ID, LOAD_STATUS='NEW'
        WHEN NOT MATCHED THEN INSERT (SIEBEL_ID, FIRST_NAME, LAST_NAME, EMAIL, ACCOUNT_SIEBEL_ID, COMMENTS, POSTN_ID, LOAD_STATUS)
             VALUES (s.ROW_ID, s.FST_NAME, s.LAST_NAME, s.EMAIL_ADDR, s.PR_DEPT_OU_ID, s.COMMENTS, s.PR_POSTN_ID, 'NEW')""", b)
    return n_acc, cur.rowcount


def load_accounts(cur):
    st, seg, postn = mapping(cur, "STATUS"), mapping(cur, "SEGMENT"), mapping(cur, "POSTN")
    cur.execute("""SELECT SIEBEL_ID, NAME, SIRET, STREET, CITY, ZIPCODE, PHONE, STATUS_CD, SEGMENT_CD, POSTN_ID, SRC_CREATED
                   FROM STG_ACCOUNT WHERE LOAD_STATUS = 'NEW' ORDER BY SIEBEL_ID""")
    ok = ko = 0
    for sid, name, siret, street, city, zipc, phone, stat, segm, pos, created in cur.fetchall():
        err = None
        if not name:
            err = ("MIG-ACC-001", "Champ obligatoire manquant : NAME")
        elif stat is not None and stat not in st:
            err = ("MIG-ACC-002", f"Statut hors référentiel : {stat}")
        elif siret:
            cur.execute("SELECT ID, SIEBEL_ID__C FROM CRM.ACCOUNT WHERE SIRET__C = :1 AND (SIEBEL_ID__C IS NULL OR SIEBEL_ID__C <> :2)", [siret, sid])
            d = cur.fetchone()
            if d:
                err = ("MIG-ACC-003", f"Doublon de SIRET {siret} avec le compte {d[1] or d[0]}")
        if err:
            reject(cur, sid, "ACCOUNT", *err)
            cur.execute("UPDATE STG_ACCOUNT SET LOAD_STATUS = 'REJECTED' WHERE SIEBEL_ID = :1", [sid])
            ko += 1
            continue
        rid, _ = upsert(cur, "CRM.ACCOUNT", "001", sid, {
            "NAME": name, "SIRET__C": siret, "STATUS__C": st.get(stat, "Actif"), "SEGMENT__C": seg.get(segm),
            "BILLINGSTREET": street, "BILLINGCITY": city, "BILLINGPOSTALCODE": zipc, "PHONE": phone,
            "OWNER_USERNAME": postn.get(pos), "SIEBEL_CREATED_DATE__C": created})
        unreject(cur, sid, "ACCOUNT")
        cur.execute("""MERGE INTO STG_ACCOUNT_LOADED l USING (SELECT :1 s, :2 i FROM DUAL) q ON (l.SIEBEL_ID = q.s)
                       WHEN MATCHED THEN UPDATE SET LOADED_AT = SYSDATE
                       WHEN NOT MATCHED THEN INSERT VALUES (q.s, q.i, SYSDATE)""", [sid, rid])
        cur.execute("UPDATE STG_ACCOUNT SET LOAD_STATUS = 'LOADED' WHERE SIEBEL_ID = :1", [sid])
        ok += 1
    return ok, ko


def account_ids(cur):
    cur.execute("SELECT SIEBEL_ID__C, ID FROM CRM.ACCOUNT WHERE SIEBEL_ID__C IS NOT NULL")
    return dict(cur.fetchall())


def load_contacts(cur):
    postn, acc = mapping(cur, "POSTN"), account_ids(cur)
    cur.execute("""SELECT SIEBEL_ID, FIRST_NAME, LAST_NAME, EMAIL, ACCOUNT_SIEBEL_ID, COMMENTS, POSTN_ID
                   FROM STG_CONTACT WHERE LOAD_STATUS = 'NEW' ORDER BY SIEBEL_ID""")
    ok = ko = 0
    for sid, first, last, email, acc_sid, comments, pos in cur.fetchall():
        err = None
        if not last:
            err = ("MIG-CTC-001", "Champ obligatoire manquant : LAST_NAME")
        elif acc_sid not in acc:
            err = ("MIG-CTC-002", f"Compte parent absent ou rejeté : {acc_sid}")
        if err:
            reject(cur, sid, "CONTACT", *err)
            cur.execute("UPDATE STG_CONTACT SET LOAD_STATUS = 'REJECTED' WHERE SIEBEL_ID = :1", [sid])
            ko += 1
            continue
        upsert(cur, "CRM.CONTACT", "003", sid, {"FIRSTNAME": first, "LASTNAME": last, "EMAIL": email, "ACCOUNTID": acc[acc_sid],
                                                "DESCRIPTION": comments, "OWNER_USERNAME": postn.get(pos)})
        unreject(cur, sid, "CONTACT")
        cur.execute("UPDATE STG_CONTACT SET LOAD_STATUS = 'LOADED' WHERE SIEBEL_ID = :1", [sid])
        ok += 1
    return ok, ko


def load_opportunities(cur, since):
    stage, acc = mapping(cur, "STAGE"), account_ids(cur)
    q = "SELECT ROW_ID, NAME, PR_DEPT_OU_ID, SUM_REVN_AMT, CURCY_CD, STG_NAME, SUM_EFFECTIVE_DT, CREATED FROM SIEBEL.S_OPTY"
    cur.execute(q + (" WHERE LAST_UPD > :1" if since else "") + " ORDER BY ROW_ID", [since] if since else [])
    ok = ko = 0
    for sid, name, acc_sid, amount, cur_cd, stg, close, created in cur.fetchall():
        if acc_sid not in acc:
            reject(cur, sid, "OPPORTUNITY", "MIG-OPT-001", f"Compte parent absent ou rejeté : {acc_sid}"); ko += 1; continue
        if stg not in stage:
            reject(cur, sid, "OPPORTUNITY", "MIG-OPT-002", f"Étape hors référentiel : {stg}"); ko += 1; continue
        upsert(cur, "CRM.OPPORTUNITY", "006", sid, {"NAME": name, "ACCOUNTID": acc[acc_sid], "AMOUNT": amount, "CURRENCYISOCODE": cur_cd,
                                                    "STAGENAME": stage[stg], "CLOSEDATE": close, "SIEBEL_CREATED_DATE__C": created})
        unreject(cur, sid, "OPPORTUNITY")
        ok += 1
    return ok, ko


def load_cases(cur, since):
    stat, acc = mapping(cur, "SR_STATUS"), account_ids(cur)
    q = "SELECT ROW_ID, SR_NUM, SR_TITLE, SR_STAT_ID, CST_OU_ID, CREATED FROM SIEBEL.S_SRV_REQ"
    cur.execute(q + (" WHERE LAST_UPD > :1" if since else "") + " ORDER BY ROW_ID", [since] if since else [])
    ok = ko = 0
    for sid, num, title, st, acc_sid, created in cur.fetchall():
        if acc_sid not in acc:
            reject(cur, sid, "CASE", "MIG-SR-001", f"Compte parent absent ou rejeté : {acc_sid}"); ko += 1; continue
        cid, _ = upsert(cur, "CRM.CASES", "500", sid, {"CASENUMBER": num, "SUBJECT": title, "STATUS": stat.get(st, "Nouveau"),
                                                       "ACCOUNTID": acc[acc_sid], "SIEBEL_CREATED_DATE__C": created})
        unreject(cur, sid, "CASE")
        cur.execute("""MERGE INTO CRM.CASE_ACTIVITY t USING (SELECT ROW_ID, COMMENTS, CREATED FROM SIEBEL.S_EVT_ACT WHERE SRA_SR_ID = :1) s
                       ON (t.SIEBEL_ID__C = s.ROW_ID)
                       WHEN MATCHED THEN UPDATE SET DESCRIPTION = s.COMMENTS, ACTIVITY_DATE = s.CREATED
                       WHEN NOT MATCHED THEN INSERT VALUES (s.ROW_ID, :2, s.COMMENTS, s.CREATED)""", [sid, cid])
        cur.execute("""MERGE INTO CRM.CASE_ATTACHMENT t USING (SELECT ROW_ID, FILE_NAME, FILE_SIZE FROM SIEBEL.S_SR_ATT WHERE PAR_ROW_ID = :1) s
                       ON (t.SIEBEL_ID__C = s.ROW_ID)
                       WHEN MATCHED THEN UPDATE SET FILE_NAME = s.FILE_NAME, FILE_SIZE = s.FILE_SIZE
                       WHEN NOT MATCHED THEN INSERT VALUES (s.ROW_ID, :2, s.FILE_NAME, s.FILE_SIZE)""", [sid, cid])
        ok += 1
    return ok, ko


def run(c, mode):
    cur = c.cursor()
    cur.execute("SELECT SYSDATE, (SELECT MAX(STARTED_AT) FROM STG_RUN WHERE RUN_MODE IN ('full','delta')) FROM DUAL")
    started, last = cur.fetchone()
    since = None
    if mode == "delta":
        if last is None:
            sys.exit("Aucune reprise complète n'a encore été faite : lancer 'full' d'abord.")
        since = last
    lines = []
    if mode in ("full", "delta"):
        a, ct = extract(cur, since)
        lines.append(f"Extraction Siebel -> staging : {a} comptes, {ct} contacts")
    ok, ko = load_accounts(cur);  lines.append(f"Comptes      : {ok} chargés, {ko} rejetés")
    ok, ko = load_contacts(cur);  lines.append(f"Contacts     : {ok} chargés, {ko} rejetés")
    if mode in ("full", "delta"):
        ok, ko = load_opportunities(cur, since); lines.append(f"Opportunités : {ok} chargées, {ko} rejetées")
        ok, ko = load_cases(cur, since);         lines.append(f"Demandes     : {ok} chargées, {ko} rejetées")
    cur.execute("INSERT INTO STG_RUN (RUN_MODE, STARTED_AT, ENDED_AT, SUMMARY) VALUES (:1, :2, SYSDATE, :3)", [mode, started, " | ".join(lines)])
    c.commit()
    print(f"Lot '{mode}' terminé.")
    print("\n".join("  " + l for l in lines))


def report(c):
    cur = c.cursor()
    rows = [("Comptes", "SIEBEL.S_ORG_EXT", "CRM.ACCOUNT", "ACCOUNT"), ("Contacts", "SIEBEL.S_CONTACT", "CRM.CONTACT", "CONTACT"),
            ("Opportunités", "SIEBEL.S_OPTY", "CRM.OPPORTUNITY", "OPPORTUNITY"), ("Demandes", "SIEBEL.S_SRV_REQ", "CRM.CASES", "CASE")]
    print(f"{'Objet':<14}{'Source':>8}{'Cible':>8}{'Rejets':>8}{'Exclus':>8}{'Écart':>8}")
    for label, src, tgt, obj in rows:
        cur.execute(f"""SELECT (SELECT COUNT(*) FROM {src}),
                               (SELECT COUNT(*) FROM {tgt} WHERE SIEBEL_ID__C IS NOT NULL AND SIEBEL_ID__C IN (SELECT ROW_ID FROM {src})),
                               (SELECT COUNT(*) FROM STG_REJECT WHERE OBJECT_NAME = :o AND SIEBEL_ID IN (SELECT ROW_ID FROM {src})),
                               (SELECT COUNT(*) FROM STG_EXCLUSION WHERE OBJECT_NAME = :o) FROM DUAL""", {"o": obj})
        s, t, r, x = cur.fetchone()
        print(f"{label:<14}{s:>8}{t:>8}{r:>8}{x:>8}{s - t - r - x:>8}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd not in ("init", "reset", "full", "delta", "load", "report"):
        print(__doc__); sys.exit(0 if cmd == "help" else 2)
    with conn() as c:
        if cmd == "init":
            init(c)
        elif cmd == "reset":
            init(c, force=True)
        elif cmd == "report":
            report(c)
        else:
            run(c, cmd)
