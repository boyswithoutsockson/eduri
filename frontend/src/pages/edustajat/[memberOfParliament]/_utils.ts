import { sql } from "kysely";
import { db } from "~src/database";
import { mpData } from "~src/pages/edustajat/_utils";

/** Common static path generation function for all [membersOfParliament] subpages */
export async function getMpStaticPaths() {
    const data = await mpData().execute();
    return data.map((mp) => ({
        params: { memberOfParliament: `${mp.first_name}+${mp.last_name}` },
        props: { mp },
    }));
}

/* 1) Attendance / participation rate (counts + rate) */
export async function getAttendance(personId: number) {
    const totalRow = await db
        .selectFrom("votes")
        .select(sql`COUNT(*)`.as("total"))
        .where("person_id", "=", personId)
        .executeTakeFirst();

    const presentRow = await db
        .selectFrom("votes")
        .select(sql`COUNT(*)`.as("present"))
        .where("person_id", "=", personId)
        .where("vote", "!=", "absent")
        .executeTakeFirst();

    const total = Number(totalRow?.total ?? 0);
    const present = Number(presentRow?.present ?? 0);
    const attendanceRate = total === 0 ? null : present / total;

    return { total, present, attendanceRate };
}

/* 2) Contra-vote score (materialized view) */
export async function getContraVoteScore(personId: number) {
    const row = await db
        .selectFrom("contra_vote_scores_view")
        .selectAll()
        .where("person_id", "=", personId)
        .executeTakeFirst();

    // If the view has no row for this MP, contra_vote_score might be null
    return { contra_vote_score: row?.contra_vote_score ?? null };
}

/* 3) Vote breakdown by type (yes / no / abstain / absent) */
export async function getVoteBreakdown(personId: number) {
    const row = await db
        .selectFrom("votes")
        .select([
            sql`SUM(CASE WHEN "vote" = 'yes' THEN 1 ELSE 0 END)`.as(
                "yes_count",
            ),
            sql`SUM(CASE WHEN "vote" = 'no' THEN 1 ELSE 0 END)`.as("no_count"),
            sql`SUM(CASE WHEN "vote" = 'abstain' THEN 1 ELSE 0 END)`.as(
                "abstain_count",
            ),
            sql`SUM(CASE WHEN "vote" = 'absent' THEN 1 ELSE 0 END)`.as(
                "absent_count",
            ),
            sql`COUNT(*)`.as("total"),
        ])
        .where("person_id", "=", personId)
        .executeTakeFirst();

    return {
        yes: Number(row?.yes_count ?? 0),
        no: Number(row?.no_count ?? 0),
        abstain: Number(row?.abstain_count ?? 0),
        absent: Number(row?.absent_count ?? 0),
        total: Number(row?.total ?? 0),
    };
}

/* 4) Monthly attendance trend (month -> present_rate)
   Returns rows: { month: string (YYYY-MM-01), present_count, total_count, present_rate }
*/
export async function getMonthlyAttendanceTrend(personId: number) {
    // Uses a raw date_trunc on ballots.start_time and aggregates
    const rows = await db
        .selectFrom("votes")
        .innerJoin("ballots", "votes.ballot_id", "ballots.id")
        .select([
            // cast to date so client gets YYYY-MM-DD (first day of month)
            sql`date_trunc('month', "ballots"."start_time")::date`.as("month"),
            sql`SUM(CASE WHEN "votes"."vote" != 'absent' THEN 1 ELSE 0 END)`.as(
                "present_count",
            ),
            sql`COUNT(*)`.as("total_count"),
        ])
        .where("votes.person_id", "=", personId)
        .groupBy(sql`date_trunc('month', "ballots"."start_time")::date`)
        .orderBy(sql`date_trunc('month', "ballots"."start_time")::date`)
        .execute();

    return rows.map((r) => {
        const present = Number((r as any).present_count ?? 0);
        const total = Number((r as any).total_count ?? 0);
        return {
            month: (r as any).month as string,
            present_count: present,
            total_count: total,
            present_rate: total === 0 ? null : present / total,
        };
    });
}

/* 5) Ministerial career: total days + timeline rows */
export async function getMinisterialCareer(personId: number) {
    // Total days as minister (sum of end_date - start_date; open terms use CURRENT_DATE)
    const sumRow = await db
        .selectFrom("ministers")
        .select([
            sql`SUM( (COALESCE("end_date", CURRENT_DATE) - "start_date") )`.as(
                "total_days",
            ),
        ])
        .where("person_id", "=", personId)
        .executeTakeFirst();

    const timeline = await db
        .selectFrom("ministers")
        .select(["minister_position", "cabinet_id", "start_date", "end_date"])
        .where("person_id", "=", personId)
        .orderBy("start_date", "asc")
        .execute();

    return {
        total_days:
            sumRow?.total_days === null ? 0 : Number(sumRow?.total_days ?? 0),
        timeline: timeline.map((r) => ({
            position: r.minister_position,
            cabinet: r.cabinet_id,
            start_date: r.start_date,
            end_date: r.end_date,
        })),
    };
}

/* 6) Proposals authored & success rate (first signer = creator)
   Returns: { authored_count, passed_count, pass_rate }
*/
export async function getProposalsAuthoredAndPassRate(personId: number) {
    const row = await db
        .selectFrom("proposal_signatures as ps")
        .leftJoin("proposals as p", "ps.proposal_id", "p.id")
        .select([
            sql`COUNT(*) FILTER (WHERE ps.first)`.as("authored_count"),
            sql`COUNT(*) FILTER (WHERE ps.first AND p.status IN ('passed','passed_changed','passed_urgent'))`.as(
                "passed_count",
            ),
        ])
        .where("ps.person_id", "=", personId)
        .executeTakeFirst();

    const authored = Number((row as any)?.authored_count ?? 0);
    const passed = Number((row as any)?.passed_count ?? 0);
    const pass_rate = authored === 0 ? null : passed / authored;

    return { authored, passed, pass_rate };
}
