import { db } from "~src/database";
import { mpsWithPhotoUrl } from "~src/utils";

/** Partial query for MP data in mp listings */
export function mpData() {
    return db
        .selectFrom("persons")
        .leftJoin(
            "mp_parliamentary_group_memberships",
            "persons.id",
            "mp_parliamentary_group_memberships.person_id",
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
            "mp_parliamentary_group_memberships.pg_id as party_id",
            "ministers.ministry",
        ]);
}

/**
 * This function constructs a static `data.json` file that contains the
 * results of all MPs, so that we can initially render the mp list page
 * with only the current 200 mps, and asynchronously fetch the remaining
 * data whenever the user uses the search bar.
 */
export async function GET() {
    const data = await mpData().distinctOn("persons.id").execute();

    const mpsWithPhotoUrls = await mpsWithPhotoUrl(data);

    return new Response(JSON.stringify(mpsWithPhotoUrls));
}
