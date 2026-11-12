# Environnement de recette simulé : migration Siebel → CRM cible

> **Projet personnel de démonstration.** Tout est simulé (aucun vrai Siebel ni Salesforce) et toutes les données sont fictives.
> Le code et les cas de tests ont été générés avec l'aide de Claude Code, l'assistant IA d'Anthropic, puis exécutés et vérifiés par mes soins.

Cet environnement tourne sur votre poste avec Docker. Il simule la chaîne complète d'une migration
de CRM pour pouvoir **exécuter réellement** les cas de tests (reprise SQL, API Postman, événements Kafka,
flux vers l'ERP).

> Tout est simulé : il n'y a ni vrai Siebel ni vrai Salesforce. Le « Siebel » est un schéma Oracle
> avec des tables aux noms Siebel, le « CRM cible » est une API REST avec sa base, nommée à la façon de Salesforce.

## 1. Ce que contient l'environnement

| Composant | Rôle | Accès |
|---|---|---|
| Oracle | Schémas `SIEBEL` (source), `RECETTE` (staging), `CRM` (cible), `ERP` (aval) | `localhost:1521/FREEPDB1`, utilisateur `RECETTE`, mot de passe `recette` |
| API CRM | Création, consultation, modification de comptes, avec OAuth | http://localhost:8181/docs |
| Kafka | Topics `crm.account.v1` et `crm.account.v1.dlq` | `localhost:29092` |
| Kafka UI | Consultation et publication de messages | http://localhost:8183 |
| Faux ERP | Consomme les événements, renvoie l'encours au CRM | http://localhost:8182/docs |
| Traitement de reprise | Lot Siebel → staging → CRM, lancé à la demande | `docker compose run --rm etl ...` |

Les ports 8181 à 8183 ont été choisis pour ne pas gêner Squash sur le 8090.

## 2. Démarrage

Prérequis : Docker Desktop (ou Docker Engine avec Compose), environ 4 Go de mémoire libres.

```bash
docker compose up -d --build
```

Le premier démarrage télécharge les images (environ 2 Go) et crée la base avec ses données de test.
Vérifier ensuite que tout est prêt :

```bash
docker compose ps
```

`oracle` et `kafka` doivent être « healthy », `crm-api`, `erp-mock` et `kafka-ui` « Up ».

Arrêter l'environnement en gardant les données :

```bash
docker compose stop
```

Tout supprimer, données comprises :

```bash
docker compose down -v
```

## 3. Commandes de reprise

| Commande | Effet |
|---|---|
| `docker compose run --rm etl full` | Reprise complète : Siebel → staging → CRM |
| `docker compose run --rm etl delta` | Reprise des seules lignes modifiées dans Siebel depuis le dernier lot |
| `docker compose run --rm etl load` | Charge uniquement les lignes du staging à l'état `NEW` (lignes injectées à la main) |
| `docker compose run --rm etl report` | Bilan : source, cible, rejets, exclusions, écart |
| `docker compose run --rm etl reset` | Remet **tout** dans l'état initial (Siebel, staging, CRM, ERP) |

Résultat attendu de la première reprise complète :

| Objet | Source | Cible | Rejets | Exclus | Écart |
|---|---|---|---|---|---|
| Comptes | 500 | 481 | 7 | 12 | 0 |
| Contacts | 1 200 | 1 155 | 45 | 0 | 0 |
| Opportunités | 800 | 768 | 32 | 0 | 0 |
| Demandes de service | 300 | 288 | 12 | 0 | 0 |

## 4. Anomalies placées volontairement dans les données Siebel

| Donnée | Anomalie | Comportement attendu |
|---|---|---|
| Comptes `1-A00011`, `1-A00123`, `1-A00301` | Nom vide | Rejet `MIG-ACC-001` |
| Comptes `1-A00017`, `1-A00333` | Statut `Zombie`, hors référentiel | Rejet `MIG-ACC-002` |
| Comptes `1-A00202`, `1-A00404` | Même SIRET que `1-A00201` et `1-A00403` | Rejet `MIG-ACC-003` |
| 12 comptes (`1-A00040`, `1-A00080`… `1-A00480`) | Inactifs depuis 7 ans | Exclus, motif « Inactif > 5 ans » |
| 13 comptes inactifs récents | Inactifs depuis moins de 5 ans | Migrés avec le statut « Inactif » |
| Comptes `1-A00005`, `1-A00006`, `1-A00009` | Accents, symboles, nom de 100 caractères | Migrés à l'identique |
| Contacts `1-C00001` à `1-C00006` | Accents, apostrophe, 80 et 255 caractères, emoji | Migrés à l'identique |
| Contact `1-C00010` | Nom vide | Rejet `MIG-CTC-001` |
| 44 contacts | Rattachés à un compte rejeté ou exclu | Rejet en cascade `MIG-CTC-002` |
| Opportunité `1-O00077` | Étape hors référentiel | Rejet `MIG-OPT-002` |
| 1 opportunité sur 10 | Créée à 23 h 50 | Date conservée, sans décalage d'un jour |

## 5. Exécuter les cas de tests

### Reprise de données (MIG-001 à MIG-015)

Se connecter à Oracle avec un client SQL (DBeaver, SQL Developer) ou en ligne de commande :

```bash
docker compose exec oracle sqlplus RECETTE/recette@FREEPDB1
```

Toutes les requêtes de contrôle sont dans `sql/controles.sql`, classées par identifiant de cas de test,
avec le résultat attendu en commentaire.

Les cas de tests parlent de requêtes SOQL sur Salesforce. Ici la cible est en Oracle, donc on écrit du SQL :

| Dans le cas de test (SOQL) | Dans cet environnement (SQL) |
|---|---|
| `FROM Account` | `FROM CRM.ACCOUNT` |
| `Siebel_Id__c != null` | `SIEBEL_ID__C IS NOT NULL` |
| `FROM Contact`, `Opportunity`, `Case` | `FROM CRM.CONTACT`, `CRM.OPPORTUNITY`, `CRM.CASES` |

Trois écarts avec la rédaction des cas de tests :

- **MIG-001** : l'équation de volumétrie compte aussi les exclusions : source = cible + rejets + exclus.
- **MIG-012** : seules les métadonnées des pièces jointes sont reprises (nom, taille), il n'y a pas de fichier à ouvrir.
- **MIG-015** : le propriétaire est vérifiable en SQL, mais il n'y a pas d'écran pour tester la visibilité par utilisateur.

### API (API-001 à API-010)

1. Dans Postman, importer les deux fichiers du dossier `postman/`.
2. Choisir l'environnement « RECETTE locale ».
3. Lancer la collection dans le Runner, ou les dossiers un par un en commençant par « 0 - Authentification ».

| Client OAuth | Secret | Droit |
|---|---|---|
| `client-rw` | `secret-rw` | Lecture et écriture |
| `client-ro` | `secret-ro` | Lecture seule (API-006) |

Le jeton expiré pour API-005 est `jeton-expire-0000` (variable `expiredToken`).
API-009 n'est pas couvert : il n'y a pas de service SOAP historique dans la simulation.

### Kafka (KAF-001 à KAF-007)

Le plus simple est Kafka UI : http://localhost:8183, menu Topics, `crm.account.v1`, onglet Messages.
Le bouton « Produce Message » sert à publier un message à la main (KAF-004 et KAF-005).

| Cas | Comment le dérouler |
|---|---|
| KAF-001, KAF-002 | Créer puis modifier un compte par l'API (Postman ou Swagger), lire le message dans Kafka UI |
| KAF-003 | Envoyer 5 `PATCH` sur le même compte, vérifier la partition et les offsets |
| KAF-004 | Publier `{"eventType":"AccountUpdated","account":{"name":"sans id"}}`, puis lire `crm.account.v1.dlq` |
| KAF-005 | Publier deux fois le même message avec le même `eventId`, puis lire le journal de l'ERP |
| KAF-006 | `docker compose stop erp-mock`, faire 20 modifications, `docker compose start erp-mock` |
| KAF-007 | Noter les offsets, lancer `etl full`, vérifier qu'aucun message n'a été publié |

Journal du consommateur ERP (résultats `TRAITE`, `DOUBLON_IGNORE`, `INVALIDE_VERS_DLQ`, `IGNORE_ORIGINE_ERP`) :
http://localhost:8182/events/log

Retard du groupe de consommateurs (KAF-006) :

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group erp-mock
```

Exemple de message valide pour KAF-005 (remplacer l'identifiant par celui d'un compte existant, et le mettre aussi en clé) :

```json
{"eventId":"evt-test-005","eventType":"AccountUpdated","source":"crm","account":{"id":"001XXXXXXXXXXXXXXX","name":"TEST","city":"Bordeaux"}}
```

### Intégration (INT-001 à INT-003)

| Cas | Comment le dérouler |
|---|---|
| INT-001 | Créer un compte par l'API, puis `GET http://localhost:8182/customers?siret=...` |
| INT-002 | `PUT http://localhost:8182/customers/{id}/encours` avec `{"amount": 15000}`, puis relire le compte dans le CRM |
| INT-003 | `docker compose stop erp-mock`, créer un compte, `docker compose start erp-mock`, vérifier le rattrapage |

Pour INT-002, l'absence de boucle se lit dans le journal de l'ERP : l'événement de retour est marqué `IGNORE_ORIGINE_ERP`.

## 6. Ce que l'environnement ne couvre pas

| Cas | Raison |
|---|---|
| API-009 | Pas de façade SOAP ni de consommateur historique |
| INT-003 (partie alerte et nouvelles tentatives) | Pas de middleware ni de supervision : Kafka sert de tampon |
| INT-004 | Pas de synchronisation bidirectionnelle Siebel ↔ CRM |
| INT-005 | Pas de prospect, de devis ni de commande dans la simulation |
| DEC-001 à DEC-005 | Pas d'interface Siebel, pas d'archivage ni de procédure de retour arrière |

## 7. Dépannage

| Symptôme | Action |
|---|---|
| Un port est déjà utilisé | Changer le port de gauche dans `docker-compose.yml` (par exemple `"1522:1521"`) |
| `db-init` en erreur | `docker compose logs db-init`, puis `docker compose up -d` pour relancer |
| L'ERP ne reçoit rien | Consulter http://localhost:8182/health et `docker compose logs erp-mock` |
| Données de test polluées | `docker compose run --rm etl reset` |
