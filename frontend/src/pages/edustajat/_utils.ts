import type { InferResult } from "kysely";
import { db } from "~src/database";

/** Partial query for MP data in mp listings */
export function mpData() {
    return db
        .selectFrom("persons")
        .leftJoinLateral(
            (eb) =>
                eb
                    .selectFrom("mp_parliamentary_group_memberships as mppm")
                    .select(["mppm.pg_id", "mppm.end_date"])
                    .whereRef("mppm.person_id", "=", "persons.id")
                    .orderBy("mppm.end_date", (ob) => ob.desc().nullsLast())
                    .as("parliamentary_group"),
            (join) => join.onTrue(),
        )
        .leftJoin("ministers", (join) =>
            join
                .onRef("ministers.person_id", "=", "persons.id")
                .on("ministers.end_date", "is", null),
        )
        .select([
            "persons.id",
            "persons.first_name",
            "persons.last_name",
            "persons.photo",
            "persons.email",
            "persons.occupation",
            "persons.place_of_residence",
            "parliamentary_group.pg_id as party_id",
            "ministers.minister_position",
        ]);
}

export type MP = InferResult<ReturnType<typeof mpData>>[0];
