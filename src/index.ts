import { SQL, type ReservedSQL } from "bun"
import { Context, Effect, Layer, Semaphore, Stream } from "effect"
import { Reactivity } from "effect/reactivity"
import { SqlClient, Statement, type SqlConnection } from "effect/sql"
import type { SqlError } from "effect/sql/SqlError"
import { sqlError } from "./errors.js"
import { openSql, sqliteQuery } from "./sqlite.js"

export type Adapter = "postgres" | "mysql" | "mariadb" | "sqlite"
type Row = Record<string, unknown>
export type Database = string | URL | SQL.Options

export interface Options {
  readonly transformQueryNames?: (name: string) => string
  readonly transformResultNames?: (name: string) => string
}

export interface BunSqlService {
  readonly adapter: Adapter
  readonly sql: SQL
  readonly client: SqlClient.SqlClient
}

export const BunSql = Context.Service<BunSqlService>("effect-bun-sql/BunSql")

function compiler(adapter: Adapter, transform?: (name: string) => string) {
  if (adapter === "sqlite") return Statement.makeCompilerSqlite(transform)

  const postgres = adapter === "postgres"
  const escape = Statement.defaultEscape(postgres ? '"' : "`")

  return Statement.makeCompiler({
    dialect: postgres ? "pg" : "mysql",
    placeholder: (index) => postgres ? `$${index}` : "?",
    onIdentifier: (name, withoutTransform) =>
      escape(transform && !withoutTransform ? transform(name) : name),
    onRecordUpdate: (placeholders, alias, columns, rows, returning) => {
      if (!postgres) return ["", []]

      return [
        `(values ${placeholders}) AS ${alias}${columns}${
          returning ? ` RETURNING ${returning[0]}` : ""
        }`,
        [...rows.flat(), ...(returning?.[1] ?? [])],
      ]
    },
    onCustom: () => ["", []],
  })
}

function runQuery<A>(adapter: Adapter, create: () => SQL.Query<A>) {
  return Effect.callback<A & SQL.ResultMetadata, SqlError>((resume) => {
    try {
      const query = create()
      const settled = query.then(
        (result) => resume(Effect.succeed(result)),
        (cause) => resume(Effect.fail(sqlError("execute", cause, adapter))),
      )

      // Bun may let an in-flight MySQL query finish after cancel().
      // Wait for settlement before Effect rolls back or releases the connection.
      return Effect.promise(async () => {
        try {
          query.cancel()
        } catch {
          // A cancellation failure must still allow transaction cleanup.
        } finally {
          await settled
        }
      })
    } catch (cause) {
      resume(Effect.fail(sqlError("execute", cause, adapter)))
    }
  })
}

function connection(sql: SQL, adapter: Adapter): SqlConnection.Connection {
  const query = <A>(text: string, params: ReadonlyArray<unknown>, unprepared = false) => {
    const statement = sql.unsafe<A>(text, [...params])
    return unprepared && params.length === 0 ? statement.simple() : statement
  }

  const run = <A extends Array<object>>(create: () => SQL.Query<A>) => {
    const execution = runQuery(adapter, create)
    return adapter === "sqlite" ? sqliteQuery(sql, execution) : execution
  }

  const executeRaw = (text: string, params: ReadonlyArray<unknown>) =>
    run(() => query<Row[]>(text, params))

  const executeValues = (text: string, params: ReadonlyArray<unknown>) =>
    run(() => query<unknown[][]>(text, params).values())

  const execute: SqlConnection.Connection["execute"] = (text, params, transform) =>
    Effect.map(executeRaw(text, params), (rows) => transform ? transform(rows) : rows)

  return {
    execute,
    executeRaw,
    executeValues,
    executeUnprepared: (text, params, transform) =>
      Effect.map(run(() => query<Row[]>(text, params, true)), (rows) =>
        transform ? transform(rows) : rows),
    executeValuesUnprepared: (text, params) =>
      run(() => query<unknown[][]>(text, params, true).values()),
    executeStream: (text, params, transform) =>
      Stream.unwrap(
        Effect.map(execute(text, params, transform), (rows) => Stream.fromIterable(rows)),
      ),
  }
}

function reserve(sql: SQL, adapter: Adapter) {
  return Effect.acquireRelease(
    Effect.callback<ReservedSQL, SqlError>((resume, signal) => {
      try {
        const pending = sql.reserve({ signal })
        void pending.then(
          (reserved) => resume(Effect.succeed(reserved)),
          (cause) => resume(Effect.fail(sqlError("reserve", cause, adapter))),
        )

        // Handles interruption racing with delivery of a successful reserve().
        return Effect.promise(() => pending.then(
          (reserved) => reserved.release().catch(() => undefined),
          () => undefined,
        ))
      } catch (cause) {
        resume(Effect.fail(sqlError("reserve", cause, adapter)))
      }
    }),
    (reserved) => Effect.orDie(Effect.tryPromise({
      try: () => reserved.release(),
      catch: (cause) => sqlError("release", cause, adapter),
    })),
    { interruptible: true },
  )
}

const sqliteAcquirers = new WeakMap<SQL, ReturnType<typeof SqlClient.makeSqliteAcquirers>>()

function getSqliteAcquirers(sql: SQL, pooled: SqlConnection.Connection) {
  const existing = sqliteAcquirers.get(sql)
  if (existing) return existing

  const acquirers = SqlClient.makeSqliteAcquirers({
    connection: Effect.succeed(pooled),
    semaphore: Semaphore.makeUnsafe(1),
  })
  sqliteAcquirers.set(sql, acquirers)
  return acquirers
}

export const makeFromSql = (sql: SQL, options: Options = {}) =>
  Effect.gen(function* () {
    const adapter = sql.options.adapter
    if (adapter !== "postgres" && adapter !== "mysql" && adapter !== "mariadb" && adapter !== "sqlite") {
      return yield* Effect.fail(sqlError("configure", new Error("Unsupported Bun SQL adapter")))
    }

    const pooled = connection(sql, adapter)
    const borrower: SqlConnection.Borrower = (use) => use(pooled)
    const acquirers = adapter === "sqlite"
      ? getSqliteAcquirers(sql, pooled)
      : {
          acquirer: Effect.succeed(pooled),
          borrower,
          onCommitFailure: (conn: SqlConnection.Connection) =>
            Effect.asVoid(conn.executeUnprepared("ROLLBACK", [], undefined)),
          transactionAcquirer: Effect.map(reserve(sql, adapter), (lease) =>
            connection(lease, adapter)),
        }

    const commit = (conn: SqlConnection.Connection) =>
      Effect.flatMap(conn.executeRaw("COMMIT", []), (result) => {
        if (
          typeof result === "object" && result !== null &&
          "command" in result && result.command === "ROLLBACK"
        ) {
          return Effect.fail(sqlError(
            "commit",
            new Error("PostgreSQL rolled back the transaction during COMMIT"),
            adapter,
          ))
        }
        return Effect.void
      })

    const client = yield* SqlClient.make({
      ...acquirers,
      compiler: compiler(adapter, options.transformQueryNames),
      driver: "bun-sql",
      spanAttributes: [
        ["db.system.name", adapter === "postgres" ? "postgresql" : adapter],
      ],
      beginTransaction: adapter === "sqlite"
        ? ("readonly" in sql.options && sql.options.readonly ? "BEGIN" : "BEGIN IMMEDIATE")
        : adapter === "postgres" ? "BEGIN" : "START TRANSACTION",
      commit: adapter === "postgres" ? commit : "COMMIT",
      releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`,
      transformRows: options.transformResultNames
        ? Statement.defaultTransforms(options.transformResultNames).array
        : undefined,
    })

    return { adapter, sql, client } satisfies BunSqlService
  })

export const make = (database: Database, options?: Options) =>
  Effect.flatMap(
    Effect.acquireRelease(
      Effect.try({
        try: () => openSql(database),
        catch: (cause) => sqlError("open", cause),
      }),
      (sql) => Effect.orDie(Effect.tryPromise({
        try: () => sql.close(),
        catch: (cause) => sqlError("close", cause),
      })),
    ),
    (sql) => makeFromSql(sql, options),
  )

function services(service: BunSqlService) {
  return Context.make(BunSql, service).pipe(
    Context.add(SqlClient.SqlClient, service.client),
  )
}

export const layer = (database: Database, options?: Options) =>
  Layer.effectContext(Effect.map(make(database, options), services)).pipe(
    Layer.provide(Reactivity.layer),
  )

export const layerFromSql = (sql: SQL, options?: Options) =>
  Layer.effectContext(Effect.map(makeFromSql(sql, options), services)).pipe(
    Layer.provide(Reactivity.layer),
  )
