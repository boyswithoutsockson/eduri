import { getImage } from "astro:assets";
import { db } from "~src/database";

export async function GET() {
    const images = import.meta.glob<{ default: ImageMetadata }>(
        "/src/assets/*.{jpeg,jpg,png,gif}",
    );

    const data = await db
        .selectFrom("members_of_parliament")
        .leftJoin(
            "mp_party_memberships",
            "members_of_parliament.id",
            "mp_party_memberships.mp_id",
        )
        .select([
            "members_of_parliament.id",
            "members_of_parliament.full_name",
            "members_of_parliament.photo",
            "mp_party_memberships.party_id",
        ])
        .distinctOn("members_of_parliament.id")
        .execute();

    const photoUrlData = await Promise.all(
        data.map(async (mp) =>
            mp.photo
                ? {
                      ...mp,
                      photo: (
                          await getImage({
                              src: images[`/src/assets/${mp.photo}`]?.()!,
                              height: 150,
                              width: 100,
                          })
                      ).src,
                  }
                : mp,
        ),
    );
    return new Response(JSON.stringify(photoUrlData));
}
