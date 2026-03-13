import { db } from "~src/database";

export async function committeeData() {
    const data = await db.selectFrom("assemblies").selectAll().execute();
    return data.map((committee) => ({
        params: { committee: `${committee.name}` },
        props: { committee },
    }));
}

export async function getMPsMasterObjectList() {
    // Get basic MP data, party affiliation and committee memberships. Extend as needed..
    const data = await db
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
        .leftJoin("mp_committee_memberships", (join) =>
            join.onRef("mp_committee_memberships.person_id", "=", "persons.id"),
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
            "mp_committee_memberships.committee_name",
            "mp_committee_memberships.start_date",
            "mp_committee_memberships.end_date",
            "mp_committee_memberships.role",
        ])
        .execute();

    // Group rows by person and build a `committees` object:
    const grouped = new Map<number, any>();
    for (const row of data) {
        const id = row.id;
        if (!grouped.has(id)) {
            // copy person fields (exclude committee-specific fields)
            const { committee_name, start_date, end_date, role, ...person } =
                row;
            grouped.set(id, {
                ...person,
                committees: {}, // will become { [committeeName]: Array<{ start_date, end_date, role }> }
            });
        }

        const personEntry = grouped.get(id);
        const cname = row.committee_name;
        if (cname) {
            if (!personEntry.committees[cname])
                personEntry.committees[cname] = [];
            personEntry.committees[cname].push({
                start_date: row.start_date,
                end_date: row.end_date,
                role: row.role,
            });
        }
    }

    return Array.from(grouped.values());
}
