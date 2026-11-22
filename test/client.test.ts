import { SQL } from "bun"
import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber, Stream } from "effect"
import { SqlClient } from "effect/sql"
import { BunSql, layer, layerFromSql, type Adapter, type Database } from "../src/index.js"

import { networkDatabases } from "./databases.js"

const databases: ReadonlyArray<readonly [Adapter, Database | undefined]> = [
  ["sqlite", ":memory:"],
  ...networkDatabases.map(([adapter, url]) => [adapter, url && { url, max: 1 }] as const),
]

for (const [adapter, database] of databases) {
  test.skipIf(database === undefined)(`${adapter}: dialect, binding, result modes and transactions`, async () => {
    if (!database) return

    await Effect.runPromise(Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const service = yield* BunSql
      expect(service.client).toBe(sql)
      expect(service.adapter).toBe(adapter)

      yield* sql`CREATE TEMPORARY TABLE effect_bun_test (id INTEGER NOT NULL UNIQUE)`.unprepared
      const inserted = yield* sql`INSERT INTO effect_bun_test ${sql.insert({ id: 1 })}`.raw
      expect(inserted).toMatchObject({ affectedRows: 1 })

      const query = sql`SELECT ${sql("id")} AS ${sql("AliasName")} FROM effect_bun_test WHERE id = ${1}`
      const [text, args] = query.compile()
      expect(text).toContain(adapter === "postgres" ? "$1" : "?")
      expect(args).toEqual([1])
      expect(yield* query).toEqual([{ ALIASNAME: 1 }])
      expect(yield* query.withoutTransform).toEqual([{ AliasName: 1 }])
      expect(yield* query.unprepared).toEqual([{ ALIASNAME: 1 }])
      expect(Array.from(yield* Stream.runCollect(query.stream))).toEqual([{ ALIASNAME: 1 }])

      const duplicateAliases = sql`SELECT 1 AS a, 2 AS a`
      expect(yield* duplicateAliases.values).toEqual([[1, 2]])
      expect(yield* duplicateAliases.valuesUnprepared).toEqual([[1, 2]])

      yield* sql.withTransaction(Effect.gen(function* () {
        yield* sql`INSERT INTO effect_bun_test VALUES (${2})`
        const nested = yield* Effect.result(sql.withTransaction(Effect.gen(function* () {
          yield* sql`INSERT INTO effect_bun_test VALUES (${3})`
          return yield* Effect.fail("rollback savepoint")
        })))
        expect(nested).toMatchObject({ _tag: "Failure", failure: "rollback savepoint" })
        yield* sql.withTransaction(sql`INSERT INTO effect_bun_test VALUES (${4})`)
      }))
      expect(yield* sql`SELECT id FROM effect_bun_test ORDER BY id`.values).toEqual([[1], [2], [4]])

      const duplicate = yield* Effect.result(sql`INSERT INTO effect_bun_test VALUES (${1})`)
      expect(duplicate).toMatchObject({ _tag: "Failure", failure: { reason: { _tag: "UniqueViolation" } } })
    }).pipe(Effect.provide(layer(database, { transformResultNames: (name) => name.toUpperCase() }))))
  })
}

for (const [adapter, url, slowQuery] of networkDatabases) {
  test.skipIf(!url)(`${adapter}: interrupted queries and pending reservations leave the pool usable`, async () => {
    if (!url) return
    await Effect.runPromise(Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TEMPORARY TABLE effect_bun_interrupt (id INTEGER)`.unprepared
      const started = yield* Deferred.make<void>()
      const tx = yield* sql.withTransaction(Effect.gen(function* () {
        yield* sql`INSERT INTO effect_bun_interrupt VALUES (1)`
        yield* Deferred.succeed(started, undefined)
        yield* sql.unsafe(slowQuery)
      })).pipe(Effect.forkChild)

      yield* Deferred.await(started)
      const waiting = yield* sql.withTransaction(Effect.void).pipe(Effect.forkChild)
      yield* Effect.sleep("50 millis")
      yield* Fiber.interrupt(waiting)
      yield* Fiber.interrupt(tx)
      expect(yield* sql.withTransaction(sql`SELECT id FROM effect_bun_interrupt`.values)).toEqual([])
    }).pipe(Effect.provide(layer({ url, max: 1 })), Effect.timeout("5 seconds")))
  })
}

test("SQLite serializes transactions across wrappers and leaves a borrowed Bun client open", async () => {
  const native = new SQL(":memory:")
  try {
    await Effect.runPromise(Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const otherLayer = layerFromSql(native)
      const otherQuery = Effect.flatMap(SqlClient.SqlClient, (client) => client`SELECT id FROM records`.values)
        .pipe(Effect.provide(otherLayer))
      yield* sql`CREATE TABLE records (id INTEGER)`
      const started = yield* Deferred.make<void>()
      const reader = yield* Effect.andThen(Deferred.await(started), otherQuery).pipe(Effect.forkChild)
      yield* Effect.ignore(sql.withTransaction(Effect.gen(function* () {
        yield* sql`INSERT INTO records VALUES (1)`
        yield* Deferred.succeed(started, undefined)
        yield* Effect.yieldNow
        return yield* Effect.fail("rollback")
      })))
      expect(yield* Fiber.join(reader)).toEqual([])
    }).pipe(Effect.provide(layerFromSql(native)), Effect.timeout("5 seconds")))
    expect((await native`SELECT 1 AS ok`)[0]).toEqual({ ok: 1 })
  } finally {
    await native.close()
  }
})

test("SQLite rolls back after a failed commit and reuses the connection", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* sql`PRAGMA foreign_keys = ON`
    yield* sql`CREATE TABLE parent (id INTEGER PRIMARY KEY)`
    yield* sql`CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`
    const invalid = yield* Effect.result(sql.withTransaction(sql`INSERT INTO child VALUES (1)`))
    expect(invalid).toMatchObject({ _tag: "Failure", failure: { reason: { _tag: "ConstraintError" } } })
    yield* sql.withTransaction(sql`INSERT INTO parent VALUES (1)`)
    expect(yield* sql`SELECT parent_id FROM child`.values).toEqual([])
  }).pipe(Effect.provide(layer(":memory:"))))
})

test("SQLite supports per-query integer precision through Effect", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const query = sql`SELECT 9007199254740993 AS id, 1.5 AS fraction`
    expect(yield* query).toEqual([{ id: 9007199254740992, fraction: 1.5 }])
    const lossless = { id: 9007199254740993n, fraction: 1.5 }
    expect(yield* query.pipe(Effect.provideService(SqlClient.SafeIntegers, true))).toEqual([lossless])
    expect(yield* query.valuesUnprepared.pipe(Effect.provideService(SqlClient.SafeIntegers, true)))
      .toEqual([[lossless.id, lossless.fraction]])
    expect(Array.from(yield* Stream.runCollect(query.stream).pipe(
      Effect.provideService(SqlClient.SafeIntegers, true),
    ))).toEqual([lossless])
    yield* sql`CREATE TABLE ids (id INTEGER PRIMARY KEY)`
    const raw = yield* sql`INSERT INTO ids VALUES (1)`.raw
    expect(raw).toMatchObject({ lastInsertRowid: 1, affectedRows: 1 })
  }).pipe(Effect.provide(layer({ adapter: "sqlite", filename: ":memory:" }))))
})

test("SQLite borrowed connections retain their configured precision default", async () => {
  const unsafe = new SQL(":memory:")
  const lossless = new SQL(":memory:", { bigint: true })
  const query = Effect.flatMap(SqlClient.SqlClient, (sql) => sql`SELECT 1 AS id`)
  try {
    expect(await Effect.runPromise(query.pipe(Effect.provide(layerFromSql(lossless)))))
      .toEqual([{ id: 1n }])
    expect(await Effect.runPromise(query.pipe(
      Effect.provideService(SqlClient.SafeIntegers, false),
      Effect.provide(layerFromSql(lossless)),
    ))).toEqual([{ id: 1 }])
    expect(await Effect.runPromise(query.pipe(
      Effect.provideService(SqlClient.SafeIntegers, true),
      Effect.provide(layerFromSql(unsafe)),
      Effect.result,
    ))).toMatchObject({ _tag: "Failure", failure: { reason: { operation: "execute" } } })
  } finally {
    await Promise.all([unsafe.close(), lossless.close()])
  }
})
