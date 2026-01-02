import { db } from "~src/database";

export async function committeeData() {
    const data = await db.selectFrom("assemblies").selectAll().execute();
    return data.map((committee) => ({
        params: { committee: `${committee.name}` },
        props: { committee },
    }));
}
