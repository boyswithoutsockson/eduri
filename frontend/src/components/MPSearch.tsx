import { useMiniSearch } from "react-minisearch";
import { useEffect, useState } from "preact/hooks";
import { PARTY_ABBRS, PARTY_COLORS } from "~src/constants";
import { getContrastingColor } from "~src/utils";

// See MiniSearch for documentation on options
// ref: https://lucaong.github.io/minisearch/types/MiniSearch.SearchOptions.html
const miniSearchOptions = {
    idField: "id",
    // indexed for searching
    fields: ["first_name", "last_name", "party_id"],
    // available as search result data
    storeFields: ["id", "first_name", "last_name", "party_id", "photo"],
    searchOptions: {
        fuzzy: 0.2,
        prefix: true,
        combineWith: "AND" as const,
    },
};

type MP = {
    id: number;
    first_name: string | null;
    last_name: string | null;
    photo: string | null;
    party_id: string | null;
};

interface Props {
    initial: MP[];
}

/** This component implements the front page search bar and the rendering of
 * search results. This kind of functionality is quite annoying to do without
 * some frontend framework, so we use Preact as a minimal choice. */
export function Search({ initial }: Props) {
    // The current search input text and a function for updating it
    const [query, setQuery] = useState<string>("");

    // A function for searching the full mp data set and the results of
    // the search query
    const { search, searchResults, addAll } = useMiniSearch(
        [],
        miniSearchOptions,
    );

    // When this component renders itself for the first time, fetch the
    // MP data form the data.json file to be used for searching
    useEffect(() => {
        const input = document.querySelector(
            "input[type='search']",
        ) as HTMLInputElement;
        fetch("/data.json")
            .then((resp) => resp.json())
            .then(addAll)
            .then(() => handleSearchChange(input.value));
    }, []);

    /** Whenever the user writes stuff into the search bar, handle the query
     * variable change and also perform the search with the most recent input
     * value */
    const handleSearchChange = (input: string) => {
        const inputValue = input.toLowerCase();
        search(inputValue);
        setQuery(inputValue);
    };

    return (
        <div>
            <input
                type="search"
                onChange={(event: any) =>
                    handleSearchChange(event.target.value)
                }
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
                          <li key={mp.id}>
                              <MemberListItem mp={mp} />
                          </li>
                      ))}
            </ol>
        </div>
    );
}

interface MLIProps {
    mp: MP;
}

/** A simple component for rendering a single MP from the search results. */
function MemberListItem({ mp }: MLIProps) {
    const backgroundColor =
        PARTY_COLORS[(mp.party_id ?? "") as keyof typeof PARTY_COLORS] ??
        "#bbbbbb";
    const textColor = getContrastingColor(backgroundColor, true);

    return (
        <>
            <article>
                {mp.photo ? (
                    <img
                        src={mp.photo}
                        alt={mp.first_name + " " + mp.last_name}
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
                    <span>
                        <strong>
                            {mp.first_name} {mp.last_name}
                        </strong>
                        <span
                            class="party"
                            style={{
                                "--background": backgroundColor,
                                "--text": textColor,
                            }}
                        >
                            {PARTY_ABBRS[
                                mp.party_id as keyof typeof PARTY_ABBRS
                            ] || mp.party_id}
                        </span>
                    </span>

                    <br />
                    <a
                        href={`/edustaja/${mp.first_name + "+" + mp.last_name || ""}`}
                    >
                        open mp page
                    </a>
                </p>
            </article>
            <hr />
        </>
    );
}
