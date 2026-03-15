import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "contabilidade.settings")

import django

django.setup()

from django.conf import settings
from django.db import connections
from django.db.utils import InterfaceError, OperationalError

try:
    from psycopg2 import InterfaceError as PsycopgInterfaceError
    from psycopg2 import OperationalError as PsycopgOperationalError
except Exception:  # pragma: no cover - psycopg2 is present in production
    PsycopgInterfaceError = ()
    PsycopgOperationalError = ()


def main() -> int:
    database = settings.DATABASES["default"]
    engine = database.get("ENGINE", "")
    if engine.endswith("sqlite3"):
        print("[wait_for_db] SQLite em uso; seguindo sem espera.")
        return 0

    timeout = int(os.getenv("DB_WAIT_TIMEOUT", "180"))
    interval = float(os.getenv("DB_WAIT_INTERVAL", "3"))
    deadline = time.monotonic() + timeout
    attempt = 0
    errors = (OperationalError, InterfaceError, PsycopgOperationalError, PsycopgInterfaceError)

    while True:
        attempt += 1
        try:
            connection = connections["default"]
            connection.close_if_unusable_or_obsolete()
            connection.ensure_connection()
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            print(f"[wait_for_db] Banco pronto na tentativa {attempt}.")
            return 0
        except errors as exc:
            remaining = deadline - time.monotonic()
            print(f"[wait_for_db] Tentativa {attempt} falhou: {exc}")
            if remaining <= 0:
                print(f"[wait_for_db] Timeout apos {timeout}s aguardando banco.")
                return 1
            time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
