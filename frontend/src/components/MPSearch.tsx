import { useMiniSearch } from "react-minisearch";
import MiniSearch from "minisearch";
import { useEffect, useState } from "preact/hooks";

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
    return (
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
                {mp.party_id}
                <br />
                <a href={`/edustaja/${mp.full_name || ""}`}>open mp page</a>
            </p>
        </article>
    );
}
