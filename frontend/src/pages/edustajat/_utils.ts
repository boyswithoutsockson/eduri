import { sql, type InferResult } from "kysely";
import { db } from "~src/database";

/** Partial query for MP data in mp listings */
export function mpData() {
    return db.selectFrom("persons").select([
        "persons.id",
        "persons.first_name",
        "persons.last_name",
        "persons.photo",
        "persons.email",
        "persons.occupation",
        "persons.place_of_residence",
        parliamentary_groups_sql.as("parliamentary_groups"),
        sql<
            { name: string; start_date: Date; end_date: Date | null }[]
        >`COALESCE(
        (
          SELECT json_agg(
            json_build_object(
              'name', m.minister_position,
              'start_date', m.start_date,
              'end_date', m.end_date
            )
            ORDER BY m.start_date
          )
          FROM ministers m
          WHERE m.person_id = persons.id
        ), '[]'
      )`.as("minister_positions"),
    ]);
}

/* 
The parliamentary group statement can't be used 
later on in a .where() clause as of it self.
With this abstraction, we can do 
.where(`${parliamentary_groups_sql} [...])
clauses.
*/
export const parliamentary_groups_sql = sql<
    {
        pg_id: string;
        name: string;
        start_date: Date;
        end_date: Date | null;
    }[]
>`COALESCE(
    (
    SELECT json_agg(
        json_build_object(
        'pg_id', pg.id,
        'name', pg.name,
        'start_date', m.start_date,
        'end_date', m.end_date
        )
        ORDER BY m.start_date
    )
    FROM mp_parliamentary_group_memberships m
    JOIN parliamentary_groups pg ON pg.id = m.pg_id
    WHERE m.person_id = persons.id
    ), '[]'
)`;

export type MP = InferResult<ReturnType<typeof mpData>>[0];

export type MPshort = Pick<MP, "id" | "first_name" | "last_name" | "photo"> & {
    current_party_id: string;
};
