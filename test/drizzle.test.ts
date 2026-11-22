import { expect, test } from "bun:test"
import { sql } from "drizzle-orm"
import { int, mysqlTable, varchar } from "drizzle-orm/mysql-core"
import { integer, pgTable, text as pgText } from "drizzle-orm/pg-core"
import { integer as sqliteInteger, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Effect } from "effect"
import { SqlClient } from "effect/sql"
import * as Mysql from "../src/drizzle-mysql.js"
import * as Postgres from "../src/drizzle-postgres.js"
import * as Sqlite from "../src/drizzle-sqlite.js"
import { layer } from "../src/index.js"
import { networkDatabases } from "./databases.js"

const sqliteRecords = sqliteTable("drizzle_records", {
  id: sqliteInteger().primaryKey(),
  name: text().notNull(),
})

test("Drizzle SQLite uses Bun codecs, values and savepoints", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* Sqlite.makeWithDefaults()
    yield* db.run(sql`CREATE TABLE drizzle_records (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
    const inserted = yield* db.transaction((tx) => Effect.gen(function* () {
      const raw = yield* tx.run(sql`INSERT INTO drizzle_records VALUES (${1}, ${"committed"})`)
      yield* Effect.ignore(tx.transaction((nested) => Effect.gen(function* () {
        yield* nested.insert(sqliteRecords).values({ id: 2, name: "rolled back" })
        return yield* nested.rollback()
      })))
      return raw
    }))
    expect(inserted).toMatchObject({ affectedRows: 1, lastInsertRowid: 1n })
    expect(yield* db.select().from(sqliteRecords)).toEqual([{ id: 1, name: "committed" }])
    expect(yield* db.get(sql`SELECT 9007199254740993 AS id`)).toEqual({ id: 9007199254740993n })
    expect(yield* db.values(sql`SELECT 1 AS duplicate, 2 AS duplicate`)).toEqual([[1n, 2n]])
  }).pipe(Effect.provide(layer(":memory:"))))
})

const [, postgresUrl] = networkDatabases[0]
const pgRecords = pgTable("drizzle_records", {
  id: integer().primaryKey(),
  name: pgText().notNull(),
})

test.skipIf(!postgresUrl)("Drizzle PostgreSQL uses Bun raw metadata and transaction settings", async () => {
  if (!postgresUrl) return
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* Postgres.makeWithDefaults()
    yield* db.execute(sql`CREATE TEMPORARY TABLE drizzle_records (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
    const raw = yield* db.execute(sql`INSERT INTO drizzle_records VALUES (${1}, ${"committed"})`)
    expect(raw).toMatchObject({ command: "INSERT", affectedRows: 1 })
    expect(yield* db.select().from(pgRecords)).toEqual([{ id: 1, name: "committed" }])
    expect(yield* db.select({
      first: sql<number>`1`.as("duplicate"),
      second: sql<number>`2`.as("duplicate"),
    }).from(pgRecords)).toEqual([{ first: 1, second: 2 }])
    const rows = yield* db.transaction(
      (tx) => tx.execute<{ transaction_read_only: string }>(sql`SHOW TRANSACTION_READ_ONLY`),
      { accessMode: "read only" },
    )
    expect(Array.from(rows)).toEqual([{ transaction_read_only: "on" }])

    const sqlClient = yield* SqlClient.SqlClient
    const outcome = yield* Effect.result(sqlClient.withTransaction(Effect.ignore(
      sqlClient`INSERT INTO drizzle_records VALUES (1, 'duplicate')`,
    )))
    expect(outcome).toMatchObject({ _tag: "Failure", failure: { reason: { operation: "commit" } } })
    expect(yield* db.select().from(pgRecords)).toEqual([{ id: 1, name: "committed" }])
  }).pipe(Effect.provide(layer({ url: postgresUrl, max: 1 }))))
})

const mysqlRecords = mysqlTable("drizzle_records", {
  id: int().autoincrement().primaryKey(),
  name: varchar({ length: 50 }).notNull(),
})

for (const [adapter, url] of networkDatabases.slice(1)) {
  test.skipIf(!url)(`Drizzle ${adapter} maps rows and generated IDs in Effect transactions`, async () => {
    if (!url) return
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* Mysql.makeWithDefaults()
      yield* db.execute(sql`CREATE TEMPORARY TABLE drizzle_records (
        id INTEGER AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL
      ) ENGINE=InnoDB`)
      const ids = yield* db.transaction((tx) => Effect.gen(function* () {
        const inserted = yield* tx.insert(mysqlRecords).values({ name: "committed" }).$returningId()
        yield* Effect.ignore(tx.transaction((nested) => Effect.gen(function* () {
          yield* nested.insert(mysqlRecords).values({ name: "rolled back" })
          return yield* nested.rollback()
        })))
        return inserted
      }))
      expect(ids).toEqual([{ id: 1 }])
      expect(yield* db.select().from(mysqlRecords)).toEqual([{ id: 1, name: "committed" }])
      expect(yield* db.select({
        first: sql<number>`1`.as("duplicate"),
        second: sql<number>`2`.as("duplicate"),
      }).from(mysqlRecords)).toEqual([{ first: 1, second: 2 }])
    }).pipe(Effect.provide(layer({ url, max: 1 }))))
  })
}

test("Drizzle reports mismatched Bun adapters as configuration errors", async () => {
  const result = await Effect.runPromise(Postgres.makeWithDefaults().pipe(
    Effect.provide(layer(":memory:")),
    Effect.result,
  ))
  expect(result).toMatchObject({ _tag: "Failure", failure: { reason: { operation: "configure Drizzle" } } })
})
