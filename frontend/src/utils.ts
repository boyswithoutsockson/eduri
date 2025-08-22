import { getImage } from "astro:assets";

export const PARLIAMENT_BASE_URL = "https://eduskunta.fi";

export const VOTE_MAP = {
    yes: "Jaa",
    no: "Ei",
    abstain: "Tyhjä",
    absent: "Poissa",
};

export function groupBy<T, K extends keyof any>(
    list: T[],
    getKey: (item: T) => K,
) {
    return list.reduce(
        (previous, currentItem) => {
            const group = getKey(currentItem);
            if (!previous[group]) previous[group] = [];
            previous[group].push(currentItem);
            return previous;
        },
        {} as Record<K, T[]>,
    );
}

/** We store our mp photos adjacent to our source files, but sometimes we
 * need the urls of the photos after they've been built into the output
 * `dist` folder, for example when retrieving individual mp data with search.
 * This function updates a list of mp objects by changing the `photo` field
 * to contain the full URL to their `photo` fields, instead of the file name
 * that the field originally contains in the database.
 */
export async function mpWithPhotoUrl<T extends { photo: string | null }>(
    mpData: T[],
): Promise<T[]> {
    const images = import.meta.glob<{ default: ImageMetadata }>(
        "/src/assets/*.{jpeg,jpg,png,gif}",
    );

    return Promise.all(
        mpData.map(async (mp) =>
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
}
