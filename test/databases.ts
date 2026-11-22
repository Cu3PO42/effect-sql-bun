export const networkDatabases = [
  ["postgres", Bun.env.TEST_POSTGRES_URL, "SELECT pg_sleep(0.5)"],
  ["mysql", Bun.env.TEST_MYSQL_URL, "SELECT SLEEP(0.5)"],
  ["mariadb", Bun.env.TEST_MARIADB_URL, "SELECT SLEEP(0.5)"],
] as const
