import { SqlError } from "effect/sql"
import type { Adapter } from "./index.js"

export function sqlError(operation: string, cause: unknown, adapter?: Adapter) {
  const details = { operation, message: `Bun SQL ${operation} failed`, cause }
  return new SqlError.SqlError({
    reason: adapter === "sqlite" ? SqlError.classifySqliteError(cause, details) : networkReason(details),
  })
}

function networkReason(details: { operation: string; message: string; cause: unknown }) {
  const cause = details.cause
  if (typeof cause !== "object" || cause === null) return new SqlError.UnknownError(details)

  const errno = "errno" in cause ? cause.errno : undefined
  const state = "sqlState" in cause ? cause.sqlState : errno
  const code = "code" in cause ? cause.code : undefined

  if (state === "23505" || errno === 1062) {
    const constraint = "constraint" in cause && typeof cause.constraint === "string" ? cause.constraint : "unknown"
    return new SqlError.UniqueViolation({ ...details, constraint })
  }
  if (state === "40P01" || errno === 1213) return new SqlError.DeadlockError(details)
  if (state === "40001") return new SqlError.SerializationError(details)
  if (state === "55P03" || errno === 1205) return new SqlError.LockTimeoutError(details)
  if (state === "42501" || errno === 1142) return new SqlError.AuthorizationError(details)
  if (state === "42601" || errno === 1064) return new SqlError.SqlSyntaxError(details)
  if (typeof state === "string") {
    if (state.startsWith("23")) return new SqlError.ConstraintError(details)
    if (state.startsWith("28")) return new SqlError.AuthenticationError(details)
    if (state.startsWith("08")) return new SqlError.ConnectionError(details)
  }
  if (code === "ECONNREFUSED" || code === "ECONNRESET" || code === "ETIMEDOUT") {
    return new SqlError.ConnectionError(details)
  }
  return new SqlError.UnknownError(details)
}
