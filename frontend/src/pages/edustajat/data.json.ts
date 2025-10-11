import type { InferResult } from "kysely";
import { db } from "~src/database";
import { mpsWithPhotoUrl } from "~src/utils";

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
                    .limit(1)
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

/**
 * This function constructs a static `data.json` file that contains the
 * results of all MPs, so that we can initially render the mp list page
 * with only the current 200 mps, and asynchronously fetch the remaining
 * data whenever the user uses the search bar.
 */
export async function GET() {
    const data = await mpData().execute();

    const mpsWithPhotoUrls = await mpsWithPhotoUrl(data);

    return new Response(JSON.stringify(mpsWithPhotoUrls));
}
