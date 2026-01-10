import { sql } from "kysely";
import { db } from "~src/database";
import type { ProposalStatus } from "~src/database.gen";
import { mpData } from "~src/pages/edustajat/_utils";

/** Common static path generation function for all [membersOfParliament] subpages */
export async function getMpStaticPaths() {
    const data = await mpData().execute();
    return data.map((mp) => ({
        params: { memberOfParliament: `${mp.first_name}+${mp.last_name}` },
        props: { mp },
    }));
}

// --- Return types ----------------------------------------------------------
export interface SeasonRange {
    start_date: Date;
    end_date: Date | null;
}

export interface AbsenceStats {
    total: number;
    workRelated: number;
    nonWorkRelated: number;
    distinctPersonsAbsent: number;
}

export interface ProposalStats {
    nonGovernmentTotal: number; // total non-government proposals in season that this MP signed
    authoredCount: number; // number of those that have a first signer = this MP
    passedCount: number; // number with status in passed variants that this MP signed
}

export interface SpeechStats {
    totalSpeeches: number;
    distinctSpeakers: number;
    avgSpeechesPerSpeaker: number;
    byType?: { speech_type: string; count: number }[]; // optional breakdown
}

export interface LobbyStats {
    totalActions: number;
    distinctLobbies: number;
    distinctPersonsContacted: number;
    topLobbies?: { lobby_id: string; actions: number }[]; // top 10 for this MP
}

// --- Helper: load season range ---------------------------------------------
/**
 * Finds election season by its start_date (primary key).
 * If the season has NULL end_date it uses the current date as end.
 * Throws when no season found.
 */
export async function getSeasonRange(seasonStart: Date): Promise<SeasonRange> {
    const row = await db
        .selectFrom("election_seasons")
        .select(["start_date", "end_date"])
        .where("start_date", "=", seasonStart)
        .executeTakeFirst();

    if (!row) {
        throw new Error(
            `Election season starting at '${seasonStart}' not found`,
        );
    }

    return {
        start_date: row.start_date,
        end_date: row.end_date ?? new Date(), // end date or today if ongoing season
    };
}

// --- Absence stats for a single MP ---------------------------------------------------------
export async function getAbsenceStats(
    seasonStart: Date,
    mpId: number,
): Promise<AbsenceStats> {
    const season = await getSeasonRange(seasonStart);

    // Join absences -> records to get record.meeting_date and filter by meeting_date in season
    const row = await db
        .selectFrom("absences")
        .innerJoin("records", (jb) =>
            jb
                .onRef(
                    "records.assembly_code",
                    "=",
                    "absences.record_assembly_code",
                )
                .onRef("records.number", "=", "absences.record_number")
                .onRef("records.year", "=", "absences.record_year"),
        )
        .select([
            sql`COUNT(*)::bigint`.as("total"),
            sql`SUM(CASE WHEN absences.work_related THEN 1 ELSE 0 END)::bigint`.as(
                "workRelated",
            ),
            sql`SUM(CASE WHEN absences.work_related THEN 0 ELSE 1 END)::bigint`.as(
                "nonWorkRelated",
            ),
            sql`COUNT(DISTINCT absences.person_id)::bigint`.as(
                "distinctPersons",
            ),
        ])
        .where("absences.person_id", "=", mpId)
        .where("records.meeting_date", ">=", season.start_date)
        .where("records.meeting_date", "<=", season.end_date)
        .executeTakeFirst();

    return {
        total: Number(row?.total ?? 0),
        workRelated: Number(row?.workRelated ?? 0),
        nonWorkRelated: Number(row?.nonWorkRelated ?? 0),
        distinctPersonsAbsent: Number(row?.distinctPersons ?? 0),
    };
}

// --- Proposal stats (exclude government proposals) for a single MP ------------------------
export async function getProposalStats(
    seasonStart: Date,
    mpId: number,
): Promise<ProposalStats> {
    const season = await getSeasonRange(seasonStart);

    // Non-government total proposals in season that this MP signed (any signature)
    const totalRow = await db
        .selectFrom("proposals")
        .innerJoin(
            "proposal_signatures",
            "proposals.id",
            "proposal_signatures.proposal_id",
        )
        .select(sql`COUNT(DISTINCT proposals.id)::bigint`.as("cnt"))
        .where("proposals.date", ">=", season.start_date)
        .where("proposals.date", "<=", season.end_date)
        .where("proposals.ptype", "<>", "government")
        .where("proposal_signatures.person_id", "=", mpId)
        .executeTakeFirst();

    const nonGovernmentTotal = Number(totalRow?.cnt ?? 0);

    // Authored: proposals where this MP is the first signer
    const authoredRow = await db
        .selectFrom("proposal_signatures")
        .innerJoin(
            "proposals",
            "proposals.id",
            "proposal_signatures.proposal_id",
        )
        .select(
            sql`COUNT(DISTINCT proposal_signatures.proposal_id)::bigint`.as(
                "cnt",
            ),
        )
        .where("proposal_signatures.first", "=", true)
        .where("proposal_signatures.person_id", "=", mpId)
        .where("proposals.date", ">=", season.start_date)
        .where("proposals.date", "<=", season.end_date)
        .where("proposals.ptype", "<>", "government")
        .executeTakeFirst();

    const authoredCount = Number(authoredRow?.cnt ?? 0);

    // Passed: non-government proposals signed by this MP whose status is one of the 'passed' statuses
    const passedStatuses: ProposalStatus[] = [
        "passed",
        "passed_changed",
        "passed_urgent",
    ];
    const passedRow = await db
        .selectFrom("proposals")
        .innerJoin(
            "proposal_signatures",
            "proposals.id",
            "proposal_signatures.proposal_id",
        )
        .select(sql`COUNT(DISTINCT proposals.id)::bigint`.as("cnt"))
        .where("proposals.date", ">=", season.start_date)
        .where("proposals.date", "<=", season.end_date)
        .where("proposals.ptype", "<>", "government")
        .where("proposal_signatures.person_id", "=", mpId)
        .where("proposals.status", "in", passedStatuses)
        .executeTakeFirst();

    const passedCount = Number(passedRow?.cnt ?? 0);

    return {
        nonGovernmentTotal,
        authoredCount,
        passedCount,
    };
}

// --- Speech stats for a single MP ----------------------------------------------------------
export async function getSpeechStats(
    seasonStart: Date,
    mpId: number,
): Promise<SpeechStats> {
    const season = await getSeasonRange(seasonStart);

    // Total and distinct speakers (filtering by speech.start_time within season) but restricted to this MP
    const baseRow = await db
        .selectFrom("speeches")
        .select([
            sql`COUNT(*)::bigint`.as("total"),
            sql`COUNT(DISTINCT person_id)::bigint`.as("distinct"),
        ])
        .where("person_id", "=", mpId)
        .where("start_time", ">=", season.start_date)
        .where("start_time", "<=", season.end_date)
        .executeTakeFirst();

    const totalSpeeches = Number(baseRow?.total ?? 0);
    const distinctSpeakers = Number(baseRow?.distinct ?? 0);
    const avgSpeechesPerSpeaker =
        distinctSpeakers === 0 ? 0 : totalSpeeches / distinctSpeakers;

    // Optional: breakdown by speech_type for this MP
    const byTypeRows = await db
        .selectFrom("speeches")
        .select(["speech_type", sql`COUNT(*)::bigint`.as("cnt")])
        .where("person_id", "=", mpId)
        .where("start_time", ">=", season.start_date)
        .where("start_time", "<=", season.end_date)
        .groupBy("speech_type")
        .orderBy("cnt", "desc")
        .execute();

    const byType = byTypeRows.map((r) => ({
        speech_type: r.speech_type,
        count: Number((r as any).cnt ?? 0),
    }));

    return {
        totalSpeeches,
        distinctSpeakers,
        avgSpeechesPerSpeaker,
        byType,
    };
}

// --- Lobby stats for a single MP -----------------------------------------------------------
export async function getLobbyStats(
    seasonStart: Date,
    mpId: number,
): Promise<LobbyStats> {
    const season = await getSeasonRange(seasonStart);

    // total actions, distinct lobbies, distinct persons contacted for this MP
    const row = await db
        .selectFrom("lobby_actions")
        .innerJoin("lobby_terms", "lobby_terms.id", "lobby_actions.term_id")
        .select([
            sql`COUNT(*)::bigint`.as("totalActions"),
            sql`COUNT(DISTINCT lobby_actions.lobby_id)::bigint`.as(
                "distinctLobbies",
            ),
            sql`COUNT(DISTINCT lobby_actions.person_id)::bigint`.as(
                "distinctPersons",
            ),
        ])
        .where("lobby_actions.person_id", "=", mpId)
        .where((eb) =>
            eb("lobby_terms.start_date", "<=", season.end_date).and(
                eb.or([
                    eb("lobby_terms.end_date", ">=", season.start_date),
                    eb("lobby_terms.end_date", "is", null),
                ]),
            ),
        )
        .executeTakeFirst();

    const totalActions = Number(row?.totalActions ?? 0);
    const distinctLobbies = Number(row?.distinctLobbies ?? 0);
    const distinctPersonsContacted = Number(row?.distinctPersons ?? 0);

    // Top 10 lobbies by number of actions in the season for this MP
    const top = await db
        .selectFrom("lobby_actions")
        .innerJoin("lobby_terms", "lobby_terms.id", "lobby_actions.term_id")
        .select(["lobby_actions.lobby_id", sql`COUNT(*)::bigint`.as("cnt")])
        .where("lobby_actions.person_id", "=", mpId)
        .where((eb) =>
            eb("lobby_terms.start_date", "<=", season.end_date).and(
                eb.or([
                    eb("lobby_terms.end_date", ">=", season.start_date),
                    eb("lobby_terms.end_date", "is", null),
                ]),
            ),
        )
        .groupBy("lobby_actions.lobby_id")
        .orderBy("cnt", "desc")
        .limit(10)
        .execute();

    const topLobbies = top.map((r) => ({
        lobby_id: r.lobby_id,
        actions: Number((r as any).cnt ?? 0),
    }));

    return {
        totalActions,
        distinctLobbies,
        distinctPersonsContacted,
        topLobbies,
    };
}
