import os
from pathlib import Path

import dj_database_url
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent

# Carrega variáveis do .env, se existir
load_dotenv(BASE_DIR / ".env")


def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes", "on")


SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "change-me")
DEBUG = env_bool("DJANGO_DEBUG", True)
allowed_hosts_raw = os.getenv("DJANGO_ALLOWED_HOSTS") or os.getenv("ALLOWED_HOSTS") or "*"
ALLOWED_HOSTS = [h.strip() for h in allowed_hosts_raw.split(",") if h.strip()]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "contabilidade",
    "contabilidade.accounts",
    "contabilidade.clients",
    "contabilidade.billing",
    "contabilidade.messaging",
    "contabilidade.whatsapp",
    "contabilidade.sales",
    "contabilidade.macros",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "contabilidade.middleware.SellerRedirectMiddleware",
]

ROOT_URLCONF = "contabilidade.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "contabilidade.wsgi.application"
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}
database_url = os.getenv("DATABASE_URL")
if database_url:
    DATABASES["default"] = dj_database_url.parse(database_url, conn_max_age=600)

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "pt-br"
TIME_ZONE = "America/Sao_Paulo"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATICFILES_DIRS = [BASE_DIR / "static"] if (BASE_DIR / "static").exists() else []
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "dashboard"
LOGOUT_REDIRECT_URL = "login"

ASAAS_API_KEY = os.getenv("ASAAS_API_KEY")
ASAAS_CUSTOMER_DEFAULT_ID = os.getenv("ASAAS_CUSTOMER_DEFAULT_ID", "146211763")
ASAAS_API_BASE_URL = os.getenv("ASAAS_API_BASE_URL", "https://api.asaas.com/v3/")

NGROK_ENABLED = env_bool("NGROK_ENABLED", False)
NGROK_AUTHTOKEN = os.getenv("NGROK_AUTHTOKEN")
NGROK_DOMAIN = os.getenv("NGROK_DOMAIN")
NGROK_PUBLIC_URL = None
csrf_trusted_raw = os.getenv("DJANGO_CSRF_TRUSTED_ORIGINS") or os.getenv("CSRF_TRUSTED_ORIGINS") or ""
CSRF_TRUSTED_ORIGINS = [o.strip() for o in csrf_trusted_raw.split(",") if o.strip()]
if NGROK_DOMAIN:
    CSRF_TRUSTED_ORIGINS.append(f"https://{NGROK_DOMAIN}")
    ALLOWED_HOSTS.append(NGROK_DOMAIN)

MACRO_API_TOKEN = os.getenv("MACRO_API_TOKEN", "").strip()
MACRO_TARGET_URL = os.getenv(
    "MACRO_TARGET_URL",
    "https://gattaran.didi-food.com/v2/gtr_crm/leads/list/all",
).strip()
MACRO_IMPORT_API_URL = os.getenv("MACRO_IMPORT_API_URL", "").strip()
MACRO_LOCAL_AGENT_URL = os.getenv("MACRO_LOCAL_AGENT_URL", "http://127.0.0.1:8765/").strip()
MACRO_AGENT_VERSION = os.getenv("MACRO_AGENT_VERSION", "v4.4.0").strip()
MACRO_LOCAL_AGENT_EXE_PATH = os.getenv(
    "MACRO_LOCAL_AGENT_EXE_PATH",
    str(BASE_DIR / "downloads" / "ColetorMacro.exe"),
).strip()
macro_ips_raw = os.getenv("MACRO_API_ALLOWED_IPS", "").strip()
MACRO_API_ALLOWED_IPS = [ip.strip() for ip in macro_ips_raw.split(",") if ip.strip()]
try:
    MACRO_API_RATE_LIMIT_PER_MINUTE = int(os.getenv("MACRO_API_RATE_LIMIT_PER_MINUTE", "60"))
except ValueError:
    MACRO_API_RATE_LIMIT_PER_MINUTE = 60
