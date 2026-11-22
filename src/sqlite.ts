import { SQL } from "bun"
import { Context, Effect } from "effect"
import { SafeIntegers } from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { sqlError } from "./errors.js"
import type { Database } from "./index.js"

const defaultPrecision = new WeakSet<SQL>()

export function openSql(database: Database) {
  if (typeof database === "string" || database instanceof URL) {
    const sql = new SQL(database, { safeIntegers: true })
    if (sql.options.adapter === "sqlite") defaultPrecision.add(sql)
    return sql
  }

  // Preserve explicit precision settings, including the bigint alias.
  if (("safeIntegers" in database && database.safeIntegers !== undefined) || database.bigint !== undefined) {
    return new SQL(database)
  }

  const sql = new SQL({ ...database, safeIntegers: true })
  if (sql.options.adapter === "sqlite") defaultPrecision.add(sql)
  return sql
}

export function hasSafeIntegers(sql: SQL): boolean {
  return "safeIntegers" in sql.options && sql.options.safeIntegers === true
}

export function sqliteQuery<A extends Array<object>>(
  sql: SQL,
  query: Effect.Effect<A & SQL.ResultMetadata, SqlError>,
): Effect.Effect<A & SQL.ResultMetadata, SqlError> {
  return Effect.withFiber((fiber) => {
    const lossless = hasSafeIntegers(sql)
    const safeIntegers = Context.getOrUndefined(fiber.context, SafeIntegers) ??
      (lossless && !defaultPrecision.has(sql))

    if (safeIntegers) {
      return lossless ? query : Effect.fail(sqlError("execute", new Error(
        "Lossless SQLite integers require the Bun SQL client to open with safeIntegers: true or bigint: true",
      )))
    }

    if (!lossless) return query

    return Effect.map(query, (result) => {
      // Bun materializes these rows for this execution. Keep the array and metadata intact.
      for (const row of result) {
        for (const [key, value] of Object.entries(row)) {
          if (typeof value === "bigint") Reflect.set(row, key, Number(value))
        }
      }
      if (typeof result.lastInsertRowid === "bigint") {
        result.lastInsertRowid = Number(result.lastInsertRowid)
      }
      return result
    })
  })
}
