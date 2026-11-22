# effect-sql-bun

An Effect 4 `SqlClient` backed by Bun's built-in `SQL` client. One layer works with PostgreSQL, MySQL, MariaDB, and SQLite. The adapter selects the Effect statement compiler from the Bun client's resolved dialect.

Requires Bun 1.4.5 or newer and Effect 4.1.5 or newer within Effect 4. The optional Drizzle bindings target `drizzle-orm@1.0.0-rc.6`.

## Effect SQL

```ts
import { Effect } from "effect"
import { SqlClient } from "effect/sql"
import { layer } from "effect-sql-bun"

const database = layer({ adapter: "sqlite", filename: ":memory:" })

const program = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`
  yield* sql.withTransaction(
    sql`INSERT INTO users ${sql.insert({ id: 1, name: "Ada" })}`,
  )
  return yield* sql<{ id: number; name: string }>`SELECT id, name FROM users`
})

console.log(await Effect.runPromise(program.pipe(Effect.provide(database))))
```

Pass a connection string, a URL, or a Bun `SQL.Options` object to `layer`. It creates and closes the Bun client with the Effect scope. Use `layerFromSql(sql)` to reuse a caller-owned Bun client and close it yourself after the layer has finished. Both layers also provide `BunSql`, which exposes the resolved `adapter`, the native Bun `sql`, and the Effect `client`. The lower-level constructors `make` and `makeFromSql` return that service and require Effect's `Reactivity` service.

Network transactions reserve a connection for their full duration. SQLite queries use a semaphore per Bun client. Reuse the same Effect client within a transaction so queries inherit its connection. Run transactional statements through the Effect client, including when a Drizzle transaction is active. Direct calls to the native client bypass Effect's transaction tracking and SQLite semaphore.

`.raw` returns Bun's result with metadata such as `affectedRows`. `.values` preserves column order and duplicate aliases. `.stream` buffers the complete result before emitting rows. For unprepared queries without bindings, the adapter uses Bun's simple query protocol. Queries with bindings use Bun's parameterized execution.

### SQLite precision and streaming

For owned SQLite connections, `layer` opens Bun SQL in lossless mode by default. Effect queries return numbers by default, matching `@effect/sql-sqlite-bun`. Provide `SqlClient.SafeIntegers` with `true` on a query to return SQLite integers as bigints. An explicit Bun precision option sets the default for that connection.

Bun SQL fixes its native integer mode when the connection opens. For a caller-owned connection passed to `layerFromSql`, enable `safeIntegers: true` or `bigint: true` at construction to use lossless queries. Both options are supported by `make` and `layer`.

The upstream adapter leaves streaming unimplemented. Bun SQL materializes SQLite query results, so this adapter's stream emits rows from the completed result.

## Drizzle Effect

| Bun adapter | Import |
| --- | --- |
| PostgreSQL | `effect-sql-bun/drizzle-postgres` |
| MySQL or MariaDB | `effect-sql-bun/drizzle-mysql` |
| SQLite | `effect-sql-bun/drizzle-sqlite` |

Each binding uses Drizzle's Effect query builders and its Bun SQL codecs. Use `make({ relations })` with a Drizzle `EffectLogger` service, or `makeWithDefaults({ relations })` for the default logger. Drizzle requests lossless SQLite results automatically. With `layerFromSql`, use a Bun client opened in lossless mode.

```ts
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { layer } from "effect-sql-bun"
import { makeWithDefaults } from "effect-sql-bun/drizzle-postgres"

const databaseUrl = Bun.env.DATABASE_URL
if (!databaseUrl) throw new Error("DATABASE_URL is required")

const program = Effect.gen(function* () {
  const db = yield* makeWithDefaults()
  return yield* db.transaction((tx) =>
    tx.execute(sql`SELECT ${42}::int AS answer`),
  )
})

console.log(await Effect.runPromise(program.pipe(Effect.provide(layer(databaseUrl)))))
```

## Development

```sh
bun install --frozen-lockfile
bun run typecheck
bun test
bun run build
```

SQLite tests run in memory. Set `TEST_POSTGRES_URL`, `TEST_MYSQL_URL`, and `TEST_MARIADB_URL` to run the live suites against their respective databases. The test users need permission to create temporary tables. For a local MySQL instance using caching SHA-2 authentication without TLS, Bun accepts `?allowPublicKeyRetrieval=true` in the test URL.
