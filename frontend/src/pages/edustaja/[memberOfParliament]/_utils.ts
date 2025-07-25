import { db } from "~src/database";

/** Helper function for `getStaticPaths` for `/edustaja` subpages */
export async function getAllMps() {
    return db
        .selectFrom("members_of_parliament")
        .leftJoin(
            "mp_party_memberships",
            "members_of_parliament.id",
            "mp_party_memberships.mp_id",
        )
        .select([
            "full_name",
            "email",
            "occupation",
            "place_of_residence",
            "mp_party_memberships.party_id",
        ])
        .selectAll()
        .execute();
}

/** Common type for MPs in all `/edustaja` subpages */
export type MP = Awaited<ReturnType<typeof getAllMps>>[0];
