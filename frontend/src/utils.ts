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
