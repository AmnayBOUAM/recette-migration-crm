"""Fonctions partagées : connexion Oracle (avec attente au démarrage) et identifiants."""
import os, secrets, string, time
import oracledb

_pool = None

def pool():
    global _pool
    if _pool is None:
        last = None
        for _ in range(60):
            try:
                _pool = oracledb.create_pool(user=os.environ["ORACLE_USER"], password=os.environ["ORACLE_PASSWORD"],
                                             dsn=os.environ["ORACLE_DSN"], min=1, max=8, increment=1)
                with _pool.acquire() as c:
                    c.cursor().execute("SELECT 1 FROM DUAL")
                break
            except Exception as e:  # base pas encore prête
                last, _pool = e, None
                time.sleep(5)
        else:
            raise RuntimeError(f"Oracle injoignable : {last}")
    return _pool

def conn():
    return pool().acquire()

_ALNUM = string.ascii_letters + string.digits

def new_id(prefix):
    """Identifiant de 18 caractères au format Salesforce (001 = compte, 003 = contact...)."""
    return prefix + "".join(secrets.choice(_ALNUM) for _ in range(15))
