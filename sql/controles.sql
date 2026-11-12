-- =====================================================================
-- Requêtes de contrôle de la reprise, classées par cas de test.
-- Connexion : RECETTE / recette sur localhost:1521/FREEPDB1
-- Dans cet environnement, le CRM cible est une base Oracle (schéma CRM) :
-- les requêtes SOQL des cas de tests sont remplacées par du SQL équivalent.
--   SOQL  : SELECT COUNT() FROM Account WHERE Siebel_Id__c != null
--   SQL   : SELECT COUNT(*) FROM CRM.ACCOUNT WHERE SIEBEL_ID__C IS NOT NULL
-- =====================================================================

-- ---------------------------------------------------------------- MIG-001 Volumétrie
-- Attendu après une reprise complète : 500 = 481 + 7 + 12
SELECT (SELECT COUNT(*) FROM SIEBEL.S_ORG_EXT)                                             AS N_SOURCE,
       (SELECT COUNT(*) FROM CRM.ACCOUNT WHERE SIEBEL_ID__C LIKE '1-A%')                   AS N_CIBLE,
       (SELECT COUNT(*) FROM STG_REJECT WHERE OBJECT_NAME = 'ACCOUNT' AND SIEBEL_ID LIKE '1-A%') AS N_REJETS,
       (SELECT COUNT(*) FROM STG_EXCLUSION WHERE OBJECT_NAME = 'ACCOUNT')                  AS N_EXCLUS
FROM DUAL;

-- Comptes source ni chargés, ni rejetés, ni exclus. Attendu : 0 ligne
SELECT s.ROW_ID, s.NAME
FROM   SIEBEL.S_ORG_EXT s
WHERE  NOT EXISTS (SELECT 1 FROM STG_ACCOUNT_LOADED l WHERE l.SIEBEL_ID = s.ROW_ID)
AND    NOT EXISTS (SELECT 1 FROM STG_REJECT r WHERE r.SIEBEL_ID = s.ROW_ID AND r.OBJECT_NAME = 'ACCOUNT')
AND    NOT EXISTS (SELECT 1 FROM STG_EXCLUSION x WHERE x.SIEBEL_ID = s.ROW_ID AND x.OBJECT_NAME = 'ACCOUNT');

-- ---------------------------------------------------------------- MIG-002 Intégrité champ à champ
-- Comptes dont au moins un champ diffère de la source. Attendu : 0 ligne
SELECT s.ROW_ID, s.NAME AS NOM_SOURCE, a.NAME AS NOM_CIBLE, s.X_SIRET, a.SIRET__C, s.CITY, a.BILLINGCITY
FROM   SIEBEL.S_ORG_EXT s
JOIN   CRM.ACCOUNT a ON a.SIEBEL_ID__C = s.ROW_ID
WHERE  s.NAME <> a.NAME
   OR  NVL(s.X_SIRET, '-') <> NVL(a.SIRET__C, '-')
   OR  NVL(s.ADDR, '-')    <> NVL(a.BILLINGSTREET, '-')
   OR  NVL(s.CITY, '-')    <> NVL(a.BILLINGCITY, '-')
   OR  NVL(s.ZIPCODE, '-') <> NVL(a.BILLINGPOSTALCODE, '-')
   OR  NVL(s.MAIN_PH_NUM, '-') <> NVL(a.PHONE, '-');

-- ---------------------------------------------------------------- MIG-003 Transcodification des statuts
-- Volumes par statut : source transcodée et cible côte à côte. Attendu : colonnes NB identiques
SELECT m.TGT_VAL AS STATUT_CIBLE, src.NB AS NB_SOURCE, NVL(tgt.NB, 0) AS NB_CIBLE
FROM   STG_MAP m
JOIN  (SELECT s.CUST_STAT_CD, COUNT(*) NB FROM SIEBEL.S_ORG_EXT s
       JOIN STG_ACCOUNT_LOADED l ON l.SIEBEL_ID = s.ROW_ID GROUP BY s.CUST_STAT_CD) src ON src.CUST_STAT_CD = m.SRC_VAL
LEFT JOIN (SELECT STATUS__C, COUNT(*) NB FROM CRM.ACCOUNT WHERE SIEBEL_ID__C LIKE '1-A%' GROUP BY STATUS__C) tgt ON tgt.STATUS__C = m.TGT_VAL
WHERE  m.DOMAIN = 'STATUS';

-- Comptes migrés sans statut. Attendu : 0
SELECT COUNT(*) FROM CRM.ACCOUNT WHERE SIEBEL_ID__C IS NOT NULL AND STATUS__C IS NULL;

-- ---------------------------------------------------------------- MIG-004 Exclusions
-- Comptes exclus (inactifs depuis plus de 5 ans). Attendu : 12 lignes, aucune en cible
SELECT x.SIEBEL_ID, x.MOTIF, (SELECT COUNT(*) FROM CRM.ACCOUNT a WHERE a.SIEBEL_ID__C = x.SIEBEL_ID) AS PRESENT_EN_CIBLE
FROM   STG_EXCLUSION x WHERE x.OBJECT_NAME = 'ACCOUNT' ORDER BY 1;

-- ---------------------------------------------------------------- MIG-005 / MIG-006 / MIG-008 Rejets
-- Lignes à injecter dans le staging avant de lancer :  docker compose run --rm etl load
--   INSERT INTO STG_ACCOUNT (SIEBEL_ID, NAME, SIRET) VALUES ('TEST-ERR-001', NULL, '12345678900011');
--   INSERT INTO STG_ACCOUNT (SIEBEL_ID, NAME, SIRET) VALUES ('TEST-DBL-001', 'Doublon 1', '98765432100022');
--   INSERT INTO STG_ACCOUNT (SIEBEL_ID, NAME, SIRET) VALUES ('TEST-DBL-002', 'Doublon 2', '98765432100022');
--   INSERT INTO STG_CONTACT (SIEBEL_ID, LAST_NAME, ACCOUNT_SIEBEL_ID) VALUES ('TEST-CTC-001', 'Orphelin', 'TEST-ERR-001');
--   COMMIT;
SELECT OBJECT_NAME, ERROR_CODE, COUNT(*) AS NB FROM STG_REJECT GROUP BY OBJECT_NAME, ERROR_CODE ORDER BY 1, 2;
SELECT SIEBEL_ID, OBJECT_NAME, ERROR_CODE, ERROR_MSG FROM STG_REJECT WHERE SIEBEL_ID LIKE 'TEST%';

-- ---------------------------------------------------------------- MIG-007 Rattachement contact / compte
-- Contacts dont le compte cible n'est pas celui de la source. Attendu : 0 ligne
SELECT sc.ROW_ID, sc.PR_DEPT_OU_ID AS COMPTE_SOURCE, a.SIEBEL_ID__C AS COMPTE_CIBLE
FROM   SIEBEL.S_CONTACT sc
JOIN   CRM.CONTACT c ON c.SIEBEL_ID__C = sc.ROW_ID
LEFT JOIN CRM.ACCOUNT a ON a.ID = c.ACCOUNTID
WHERE  a.SIEBEL_ID__C IS NULL OR a.SIEBEL_ID__C <> sc.PR_DEPT_OU_ID;

-- ---------------------------------------------------------------- MIG-009 Caractères spéciaux
SELECT sc.ROW_ID, sc.FST_NAME, sc.LAST_NAME, c.FIRSTNAME, c.LASTNAME, LENGTH(sc.COMMENTS) AS LG_SOURCE, LENGTH(c.DESCRIPTION) AS LG_CIBLE
FROM   SIEBEL.S_CONTACT sc JOIN CRM.CONTACT c ON c.SIEBEL_ID__C = sc.ROW_ID
WHERE  sc.ROW_ID IN ('1-C00001','1-C00002','1-C00003','1-C00004','1-C00005','1-C00006');

-- ---------------------------------------------------------------- MIG-010 Montants des opportunités
-- Écart par devise et par étape, hors opportunités rejetées. Attendu : 0 ligne
SELECT src.CURCY_CD, src.ETAPE, src.NB AS NB_SOURCE, tgt.NB AS NB_CIBLE, src.TOTAL AS TOTAL_SOURCE, tgt.TOTAL AS TOTAL_CIBLE
FROM  (SELECT o.CURCY_CD, m.TGT_VAL AS ETAPE, COUNT(*) NB, SUM(o.SUM_REVN_AMT) TOTAL
       FROM SIEBEL.S_OPTY o JOIN STG_MAP m ON m.DOMAIN = 'STAGE' AND m.SRC_VAL = o.STG_NAME
       WHERE NOT EXISTS (SELECT 1 FROM STG_REJECT r WHERE r.SIEBEL_ID = o.ROW_ID AND r.OBJECT_NAME = 'OPPORTUNITY')
       GROUP BY o.CURCY_CD, m.TGT_VAL) src
FULL JOIN (SELECT CURRENCYISOCODE, STAGENAME, COUNT(*) NB, SUM(AMOUNT) TOTAL FROM CRM.OPPORTUNITY
           GROUP BY CURRENCYISOCODE, STAGENAME) tgt ON tgt.CURRENCYISOCODE = src.CURCY_CD AND tgt.STAGENAME = src.ETAPE
WHERE  NVL(src.NB, -1) <> NVL(tgt.NB, -1) OR NVL(src.TOTAL, -1) <> NVL(tgt.TOTAL, -1);

-- ---------------------------------------------------------------- MIG-011 Dates
-- Opportunités créées peu avant minuit dont une date diffère. Attendu : 0 ligne
SELECT o.ROW_ID, TO_CHAR(o.CREATED, 'YYYY-MM-DD HH24:MI') AS CREE_SOURCE, TO_CHAR(c.SIEBEL_CREATED_DATE__C, 'YYYY-MM-DD HH24:MI') AS CREE_CIBLE,
       o.SUM_EFFECTIVE_DT AS CLOTURE_SOURCE, c.CLOSEDATE AS CLOTURE_CIBLE
FROM   SIEBEL.S_OPTY o JOIN CRM.OPPORTUNITY c ON c.SIEBEL_ID__C = o.ROW_ID
WHERE  o.CREATED <> c.SIEBEL_CREATED_DATE__C OR o.SUM_EFFECTIVE_DT <> c.CLOSEDATE;

-- ---------------------------------------------------------------- MIG-012 Historique et pièces jointes
-- Demandes dont le nombre d'activités ou de pièces jointes diffère. Attendu : 0 ligne
SELECT sr.ROW_ID,
       (SELECT COUNT(*) FROM SIEBEL.S_EVT_ACT e WHERE e.SRA_SR_ID = sr.ROW_ID)  AS ACT_SOURCE,
       (SELECT COUNT(*) FROM CRM.CASE_ACTIVITY a WHERE a.CASEID = c.ID)          AS ACT_CIBLE,
       (SELECT COUNT(*) FROM SIEBEL.S_SR_ATT f WHERE f.PAR_ROW_ID = sr.ROW_ID)   AS PJ_SOURCE,
       (SELECT COUNT(*) FROM CRM.CASE_ATTACHMENT t WHERE t.CASEID = c.ID)        AS PJ_CIBLE
FROM   SIEBEL.S_SRV_REQ sr JOIN CRM.CASES c ON c.SIEBEL_ID__C = sr.ROW_ID
WHERE  (SELECT COUNT(*) FROM SIEBEL.S_EVT_ACT e WHERE e.SRA_SR_ID = sr.ROW_ID) <> (SELECT COUNT(*) FROM CRM.CASE_ACTIVITY a WHERE a.CASEID = c.ID)
   OR  (SELECT COUNT(*) FROM SIEBEL.S_SR_ATT f WHERE f.PAR_ROW_ID = sr.ROW_ID) <> (SELECT COUNT(*) FROM CRM.CASE_ATTACHMENT t WHERE t.CASEID = c.ID);

-- ---------------------------------------------------------------- MIG-013 Idempotence
-- Doublons d'identifiant Siebel après rejeu. Attendu : 0 ligne
SELECT SIEBEL_ID__C, COUNT(*) FROM CRM.ACCOUNT WHERE SIEBEL_ID__C IS NOT NULL GROUP BY SIEBEL_ID__C HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------- MIG-014 Reprise delta
-- 1) Modifier 10 comptes dans Siebel :
--   UPDATE SIEBEL.S_ORG_EXT SET MAIN_PH_NUM = '0999999999', LAST_UPD = SYSDATE WHERE ROW_ID BETWEEN '1-A00050' AND '1-A00059';
--   COMMIT;
-- 2) Lancer :  docker compose run --rm etl delta
-- 3) Comptes modifiés en cible par le dernier lot. Attendu : 10
SELECT COUNT(*) FROM CRM.ACCOUNT WHERE LASTMODIFIEDDATE >= (SELECT MAX(STARTED_AT) FROM STG_RUN);

-- ---------------------------------------------------------------- MIG-015 Propriétaires
-- Comptes dont le propriétaire ne correspond pas à la position Siebel. Attendu : 0 ligne
SELECT s.ROW_ID, s.PR_POSTN_ID, m.TGT_VAL AS ATTENDU, a.OWNER_USERNAME AS OBTENU
FROM   SIEBEL.S_ORG_EXT s JOIN CRM.ACCOUNT a ON a.SIEBEL_ID__C = s.ROW_ID
JOIN   STG_MAP m ON m.DOMAIN = 'POSTN' AND m.SRC_VAL = s.PR_POSTN_ID
WHERE  NVL(a.OWNER_USERNAME, '-') <> m.TGT_VAL;

-- ---------------------------------------------------------------- Historique des lots
SELECT RUN_ID, RUN_MODE, TO_CHAR(STARTED_AT, 'YYYY-MM-DD HH24:MI:SS') AS DEBUT, SUMMARY FROM STG_RUN ORDER BY RUN_ID;
