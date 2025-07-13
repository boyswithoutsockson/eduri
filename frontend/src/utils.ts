export function encode(str: string): string {
    return encodeURIComponent(str).replace(/%20/g, "+");
}

export function decode(str: string): string {
    return decodeURIComponent(str.replace(/\+/g, "%20"));
}

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
