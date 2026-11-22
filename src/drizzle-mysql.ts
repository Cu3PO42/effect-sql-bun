import { Effect } from "effect"
import type { SqlError } from "effect/sql/SqlError"
import type { SQL } from "bun"
import type { AnyRelations, EmptyRelations, Query } from "drizzle-orm"
import { bunSqlMySqlCodecs, type BunMySqlQueryResultHKT } from "drizzle-orm/bun-sql/mysql"
import { EffectLogger, type EffectLoggerShape } from "drizzle-orm/effect-core"
import { MySqlDialect, type MySqlPreparedQueryConfig } from "drizzle-orm/mysql-core"
import {
  MySqlEffectDatabase,
  MySqlEffectPreparedQuery,
  MySqlEffectSession,
  MySqlEffectTransaction,
  type EffectDrizzleMySqlConfig,
} from "drizzle-orm/mysql-core/effect"
import type { BunSqlService } from "./index.js"
import { requireAdapter, type QueryEffect } from "./drizzle-common.js"
import { sqlError } from "./errors.js"

type Transaction<R extends AnyRelations> =
  MySqlEffectTransaction<QueryEffect, BunMySqlQueryResultHKT, R>

class Session<R extends AnyRelations> extends MySqlEffectSession<QueryEffect, BunMySqlQueryResultHKT, R> {
  constructor(
    private readonly client: BunSqlService["client"],
    dialect: MySqlDialect,
    private readonly relations: R,
    private readonly logger: EffectLoggerShape,
    private readonly paramsInErrors: boolean | undefined,
  ) {
    super(dialect)
  }

  override prepareQuery<T extends MySqlPreparedQueryConfig>(
    query: Query,
    mode: "arrays" | "objects" | "raw",
    mapper?: (rows: any[]) => any,
  ) {
    return new MySqlEffectPreparedQuery<T, QueryEffect>((params) => {
      const statement = this.client.unsafe(query.sql, params)
      if (mode === "arrays") return statement.values
      if (mode === "objects") return statement.withoutTransform
      if (!mapper) return statement.raw
      return Effect.flatMap(statement.raw, (result) => {
        const { lastInsertRowid, affectedRows } = result as SQL.ResultMetadata
        // Drizzle's generated-ID mapper increments a JavaScript number.
        const insertId = Number(lastInsertRowid)
        if (!Number.isSafeInteger(insertId) || !Number.isSafeInteger(insertId + affectedRows)) {
          return Effect.fail(sqlError("map generated IDs", new RangeError("Generated MySQL IDs exceed the safe integer range")))
        }
        return Effect.succeed({ insertId, affectedRows })
      })
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

class BunTransaction<R extends AnyRelations> extends MySqlEffectTransaction<QueryEffect, BunMySqlQueryResultHKT, R> {
  constructor(dialect: MySqlDialect, private readonly bunSession: Session<R>, relations: R) {
    super(dialect, bunSession, relations)
  }

  override transaction<A, E, Env>(use: (tx: Transaction<R>) => Effect.Effect<A, E, Env>) {
    return this.bunSession.transaction(use)
  }
}

export const make = <R extends AnyRelations = EmptyRelations>(config: EffectDrizzleMySqlConfig<R> = {}) =>
  Effect.gen(function* () {
    const { sql, client } = yield* requireAdapter("mysql", "mariadb")
    const logger = yield* EffectLogger
    const dialect = new MySqlDialect({ codecs: config.codecs ?? bunSqlMySqlCodecs, useJitMappers: config.jit })
    const relations = config.relations ?? ({} as R)
    const session = new Session(client, dialect, relations, logger, config.paramsInErrors)
    const db = new MySqlEffectDatabase<QueryEffect, BunMySqlQueryResultHKT, R>(dialect, session, relations)
    return Object.assign(db, { $client: sql })
  })

export const makeWithDefaults = <R extends AnyRelations = EmptyRelations>(config: EffectDrizzleMySqlConfig<R> = {}) =>
  make(config).pipe(Effect.provide(EffectLogger.Default))
