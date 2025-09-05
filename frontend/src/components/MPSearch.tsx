import { useMiniSearch } from "react-minisearch";
import MiniSearch from "minisearch";
import { useEffect, useState } from "preact/hooks";
import { PARTY_COLORS } from "~src/constants";

function padZero(str: string, len = 2) {
    var zeros = new Array(len).join("0");
    return (zeros + str).slice(-len);
}

function invertColor(hex: string, bw: boolean) {
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

const suffixes = (term: string, minLength: number) => {
    if (term == null) return;
    if (term.length <= minLength) return [term];

    const tokens = [];
    for (let i = 0; i <= term.length - minLength; i++) {
        tokens.push(term.slice(i));
    }
    return tokens;
};

// See MiniSearch for documentation on options
const miniSearchOptions = {
    idField: "id",
    fields: ["full_name", "party_id"],
    storeFields: ["full_name", "party_id", "photo"],
    processTerm: (term: string) => suffixes(term.toLowerCase(), 3),
    searchOptions: {
        processTerm: MiniSearch.getDefault("processTerm"),
        fuzzy: 0.1,
        prefix: true,
        combineWith: "AND" as const,
    },
};

type MP = {
    full_name: string | null;
    photo: string | null;
    party_id: string | null;
};

interface Props {
    initial: MP[];
}

export const Search = ({ initial }: Props) => {
    const [query, setQuery] = useState<string>("");
    const { search, searchResults, addAll } = useMiniSearch(
        [],
        miniSearchOptions,
    );

    useEffect(
        () =>
            void fetch("/data.json")
                .then((resp) => resp.json())
                .then(addAll),
        [],
    );

    const handleSearchChange = (event: any) => {
        const current = event.target.value.toLowerCase();
        search(current);
        setQuery(current);
    };

    return (
        <div>
            <input
                type="search"
                onChange={handleSearchChange}
                placeholder="Hae kansanedustajia..."
                aria-label="Hae kansanedustajia..."
                aria-controls="results"
            />

            <ol id="results" aria-live="polite">
                {query !== ""
                    ? searchResults?.slice(0, 50).map((result, i) => {
                          return (
                              <li key={i}>
                                  <MemberListItem mp={result} />
                              </li>
                          );
                      })
                    : initial.map((mp) => (
                          <li key={mp.full_name}>
                              <MemberListItem mp={mp} />
                          </li>
                      ))}
            </ol>
        </div>
    );
};

interface MLIProps {
    mp: MP;
}

function MemberListItem({ mp }: MLIProps) {
    const backgroundColor =
        PARTY_COLORS[(mp.party_id ?? "") as keyof typeof PARTY_COLORS] ??
        "#bbbbbb";
    const textColor = invertColor(backgroundColor, true);

    return (
        <>
            <article>
                {mp.photo ? (
                    <img
                        src={mp.photo}
                        alt={mp.full_name!}
                        height="150"
                        width="100"
                    />
                ) : (
                    <div className="missing">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            width="100%"
                            height="100%"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            stroke-width="2"
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            class="feather feather-user"
                        >
                            <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
                            <circle cx="12" cy="7" r="4"></circle>
                        </svg>
                    </div>
                )}
                <p>
                    <strong>{mp.full_name}</strong>
                    <span
                        class="party"
                        style={{
                            "--background": backgroundColor,
                            "--text": textColor,
                        }}
                    >
                        {mp.party_id}
                    </span>
                    <br />
                    <a href={`/edustaja/${mp.full_name || ""}`}>open mp page</a>
                </p>
            </article>
            <hr />
        </>
    );
}
