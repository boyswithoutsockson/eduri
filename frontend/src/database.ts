import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";

import type { DB } from "~src/database.gen.d";

export const db = new Kysely<DB>({
    dialect: new PostgresDialect({
        pool: new Pool({
            connectionString: import.meta.env.DATABASE_URL,
        }),
    }),
});
