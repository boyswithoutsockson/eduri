import { db } from "~src/database";
import { mpWithPhotoUrl } from "~src/utils";

/** Partial query for MP data in mp listings */
export function mpData() {
    return db
        .selectFrom("members_of_parliament")
        .leftJoin(
            "mp_party_memberships",
            "members_of_parliament.id",
            "mp_party_memberships.mp_id",
        )
        .leftJoin("ministers", (join) =>
            join
                .onRef("ministers.mp_id", "=", "members_of_parliament.id")
                .on("ministers.end_date", "is", null),
        )
        .select([
            "members_of_parliament.id",
            "members_of_parliament.first_name",
            "members_of_parliament.last_name",
            "members_of_parliament.photo",
            "mp_party_memberships.party_id",
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
    const data = await mpData()
        .distinctOn("members_of_parliament.id")
        .execute();

    const mpsWithPhotoUrls = await mpWithPhotoUrl(data);

    return new Response(JSON.stringify(mpsWithPhotoUrls));
}
