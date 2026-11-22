import { Effect } from "effect"
import type { EffectDrizzleQueryError, QueryEffectHKTBase } from "drizzle-orm/effect-core"
import { BunSql, type Adapter } from "./index.js"
import { sqlError } from "./errors.js"

export interface QueryEffect extends QueryEffectHKTBase {
  readonly error: EffectDrizzleQueryError
  readonly context: never
}

export const requireAdapter = (...supported: Adapter[]) => Effect.gen(function* () {
  const service = yield* BunSql
  if (!supported.includes(service.adapter)) {
    return yield* Effect.fail(sqlError("configure Drizzle", new Error(
      `Expected Bun SQL adapter ${supported.join(" or ")}, received ${service.adapter}`,
    )))
  }
  return service
})
