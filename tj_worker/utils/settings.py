import os

import dotenv

dotenv.load_dotenv()

REQUIRED_ENV_VARS = ("DB_SERVER", "DB_USERNAME", "DB_PASSWORD", "DB_NAME", "AZURE_STORAGE_CONN_STR", "MODULE_TO_RUN")

missing = []
for v in REQUIRED_ENV_VARS:
    if v not in os.environ:
        missing.append(v)

if missing:
    print("Required Environment Variables Unset::")
    print("\t" + "\n\t".join(missing))
    print("Exiting.")
    exit()


DB_SERVER = os.getenv("DB_SERVER")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
AZURE_STORAGE_CONN_STR = os.getenv("AZURE_STORAGE_CONN_STR")
MODULE_TO_RUN = os.getenv("MODULE_TO_RUN")
SLEEP_MODE = os.getenv("SLEEP_MODE")
