import { Effect } from "effect"
import { SafeIntegers } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import type { AnyRelations, EmptyRelations, Query } from "drizzle-orm"
import { bunSqlSQLiteCodecs, type BunSQLiteRunResult } from "drizzle-orm/bun-sql/sqlite"
import { EffectLogger, type EffectLoggerShape } from "drizzle-orm/effect-core"
import { SQLiteDialect, type PreparedQueryConfig, type SQLiteExecuteMethod } from "drizzle-orm/sqlite-core"
import {
  SQLiteEffectDatabase,
  SQLiteEffectPreparedQuery,
  SQLiteEffectSession,
  SQLiteEffectTransaction,
  type EffectDrizzleSQLiteConfig,
} from "drizzle-orm/sqlite-core/effect"
import type { BunSqlService } from "./index.js"
import { requireAdapter, type QueryEffect } from "./drizzle-common.js"
import { sqlError } from "./errors.js"
import { hasSafeIntegers } from "./sqlite.js"

type Transaction<R extends AnyRelations> =
  SQLiteEffectTransaction<QueryEffect, BunSQLiteRunResult, R>

class Session<R extends AnyRelations> extends SQLiteEffectSession<BunSQLiteRunResult, QueryEffect, R> {
  constructor(
    private readonly client: BunSqlService["client"],
    dialect: SQLiteDialect,
    private readonly relations: R,
    private readonly logger: EffectLoggerShape,
    private readonly paramsInErrors: boolean | undefined,
  ) {
    super(dialect)
  }

  override prepareQuery<T extends PreparedQueryConfig>(
    query: Query,
    mode: "arrays" | "objects" | "raw",
    _prepare: boolean,
    executeMethod?: SQLiteExecuteMethod,
    mapper?: (rows: any[]) => any,
  ) {
    const all = (params: unknown[]) => {
      const statement = this.client.unsafe(query.sql, params)
      return Effect.provideService(
        mode === "arrays" ? statement.values : statement.withoutTransform,
        SafeIntegers,
        true,
      )
    }
    return new SQLiteEffectPreparedQuery<T, QueryEffect>(executeMethod, {
      all,
      get: (params) => Effect.map(all(params), (rows) => rows[0]),
      run: (params) => Effect.provideService(this.client.unsafe(query.sql, params).raw, SafeIntegers, true),
    }, query, mapper, mode, this.logger, this.paramsInErrors)
  }

  override transaction<A, E, Env>(
    use: (tx: Transaction<R>) => Effect.Effect<A, E, Env>,
  ): Effect.Effect<A, E | SqlError, Env> {
    return this.client.withTransaction(
      Effect.suspend(() => use(new BunTransaction(this.dialect, this, this.relations))),
    )
  }
}

class BunTransaction<R extends AnyRelations> extends SQLiteEffectTransaction<QueryEffect, BunSQLiteRunResult, R> {}

export const make = <R extends AnyRelations = EmptyRelations>(config: EffectDrizzleSQLiteConfig<R> = {}) =>
  Effect.gen(function* () {
    const { sql, client } = yield* requireAdapter("sqlite")
    if (!hasSafeIntegers(sql)) {
      return yield* Effect.fail(sqlError("configure Drizzle", new Error(
        "Bun SQL SQLite requires safeIntegers: true (or bigint: true) to preserve 64-bit integers",
      )))
    }
    const logger = yield* EffectLogger
    const dialect = new SQLiteDialect({ codecs: config.codecs ?? bunSqlSQLiteCodecs, useJitMappers: config.jit })
    const relations = config.relations ?? ({} as R)
    const session = new Session(client, dialect, relations, logger, config.paramsInErrors)
    const db = new SQLiteEffectDatabase<QueryEffect, BunSQLiteRunResult, R>(dialect, session, relations)
    return Object.assign(db, { $client: sql })
  })

export const makeWithDefaults = <R extends AnyRelations = EmptyRelations>(config: EffectDrizzleSQLiteConfig<R> = {}) =>
  make(config).pipe(Effect.provide(EffectLogger.Default))
