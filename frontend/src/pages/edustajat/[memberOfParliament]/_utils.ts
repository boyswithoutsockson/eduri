import { db } from "~src/database";

/** Helper function for `getStaticPaths` for `/edustaja` subpages */
export async function getAllMps() {
    return db
        .selectFrom("persons")
        .leftJoinLateral(
            (eb) =>
                eb
                    .selectFrom("mp_parliamentary_group_memberships as mppm")
                    .select("mppm.pg_id")
                    .whereRef("mppm.person_id", "=", "persons.id")
                    .orderBy("mppm.end_date", (ob) => ob.desc().nullsLast())
                    .limit(1)
                    .as("party"),
            (join) => join.onTrue(),
        )
        .select([
            "full_name",
            "email",
            "occupation",
            "place_of_residence",
            "party.pg_id as party_id",
        ])
        .selectAll()
        .execute();
}

/** Common type for MPs in all `/edustaja` subpages */
export type MP = Awaited<ReturnType<typeof getAllMps>>[0];

/** Common static path generation function for all [membersOfParliament] subpages */
export async function getMpStaticPaths() {
    const data = await getAllMps();
    return data.map((mp) => ({
        params: { memberOfParliament: `${mp.first_name}+${mp.last_name}` },
        props: { mp },
    }));
}
