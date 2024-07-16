from settings.keys import get_system_key

DB_USER = get_system_key("DB_USER")
DB_PASSWORD = get_system_key("DB_PASSWORD")
DB_HOST = get_system_key("DB_HOST")
DB_PORT = get_system_key("DB_PORT")
DB_NAME = get_system_key("DB_NAME")

USERS_TABLE_NAME = get_system_key("USERS_TABLE_NAME")
