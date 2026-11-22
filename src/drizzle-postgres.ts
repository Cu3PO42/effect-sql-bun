import { Effect } from "effect"
import type { SqlError } from "effect/sql/SqlError"
import type { AnyRelations, EmptyRelations, Query } from "drizzle-orm"
import { bunSqlPgCodecs, type BunSQLQueryResultHKT } from "drizzle-orm/bun-sql/postgres"
import {
  EffectLogger,
  type EffectLoggerShape,
} from "drizzle-orm/effect-core"
import {
  PgDialect,
  type PgTransactionConfig,
  type PreparedQueryConfig,
} from "drizzle-orm/pg-core"
import {
  PgEffectDatabase,
  PgEffectPreparedQuery,
  PgEffectSession,
  PgEffectTransaction,
  type EffectDrizzlePgConfig,
} from "drizzle-orm/pg-core/effect"
import type { BunSqlService } from "./index.js"
import { requireAdapter, type QueryEffect } from "./drizzle-common.js"
import { sqlError } from "./errors.js"

type Transaction<R extends AnyRelations> =
  PgEffectTransaction<QueryEffect, BunSQLQueryResultHKT, R>

class Session<R extends AnyRelations>
  extends PgEffectSession<QueryEffect, BunSQLQueryResultHKT, R>
{
  constructor(
    private readonly client: BunSqlService["client"],
    dialect: PgDialect,
    private readonly relations: R,
    private readonly logger: EffectLoggerShape,
    private readonly paramsInErrors: boolean | undefined,
  ) {
    super(dialect)
  }

  override prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
    query: Query,
    mode: "arrays" | "objects" | "raw" | "call",
    _name: string | boolean,
    mapper?: (rows: any[]) => any,
  ) {
    return new PgEffectPreparedQuery<T, QueryEffect>(
      (params) => {
        const statement = this.client.unsafe(query.sql, params)
        if (mode === "arrays") return statement.values
        if (mode === "raw") return statement.raw
        return statement.withoutTransform
      },
      query,
      mapper,
      mode,
      this.logger,
      this.paramsInErrors,
    )
  }

  override transaction<A, E, Env>(
    use: (tx: Transaction<R>) => Effect.Effect<A, E, Env>,
    config?: PgTransactionConfig,
  ): Effect.Effect<A, E | SqlError, Env> {
    const tx = new BunTransaction(this.dialect, this, this.relations)
    return this.client.withTransaction(Effect.gen(function* () {
      if (config) {
        yield* tx.setTransaction(config).pipe(Effect.mapError((cause) => sqlError("configure transaction", cause)))
      }
      return yield* use(tx)
    }))
  }
}

class BunTransaction<R extends AnyRelations>
  extends PgEffectTransaction<QueryEffect, BunSQLQueryResultHKT, R> {
  constructor(dialect: PgDialect, private readonly bunSession: Session<R>, relations: R) {
    super(dialect, bunSession, relations)
  }

  override transaction<A, E, Env>(use: (tx: Transaction<R>) => Effect.Effect<A, E, Env>) {
    return this.bunSession.transaction(use)
  }
}

export const make = <R extends AnyRelations = EmptyRelations>(
  config: EffectDrizzlePgConfig<R> = {},
) =>
  Effect.gen(function* () {
    const { sql, client } = yield* requireAdapter("postgres")

    const logger = yield* EffectLogger
    const dialect = new PgDialect({ codecs: config.codecs ?? bunSqlPgCodecs(sql), useJitMappers: config.jit })
    const relations = config.relations ?? ({} as R)
    const session = new Session(client, dialect, relations, logger, config.paramsInErrors)
    const db = new PgEffectDatabase<QueryEffect, BunSQLQueryResultHKT, R>(
      dialect,
      session,
      relations,
    )

    return Object.assign(db, { $client: sql })
  })

export const makeWithDefaults = <R extends AnyRelations = EmptyRelations>(
  config: EffectDrizzlePgConfig<R> = {},
) => make(config).pipe(Effect.provide(EffectLogger.Default))
