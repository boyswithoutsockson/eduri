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

/** We need to get a well-contrasting color for each party color, so this
 * function calculates whether white or black works better as text color
 * for a given party color hex */
export function getContrastingColor(hex: string, bw: boolean) {
    function padZero(str: string, len = 2) {
        var zeros = new Array(len).join("0");
        return (zeros + str).slice(-len);
    }

    if (hex.indexOf("#") === 0) {
        hex = hex.slice(1);
    }
    if (hex.length !== 6) {
        throw new Error("Invalid HEX color.");
    }
    var r: string | number = parseInt(hex.slice(0, 2), 16),
        g: string | number = parseInt(hex.slice(2, 4), 16),
        b: string | number = parseInt(hex.slice(4, 6), 16);
    if (bw) {
        // https://stackoverflow.com/a/3943023/112731
        return r * 0.299 + g * 0.587 + b * 0.114 > 186 ? "#000000" : "#FFFFFF";
    }
    // invert color components
    r = (255 - r).toString(16);
    g = (255 - g).toString(16);
    b = (255 - b).toString(16);
    // pad each with zeros and return
    return "#" + padZero(r) + padZero(g) + padZero(b);
}
