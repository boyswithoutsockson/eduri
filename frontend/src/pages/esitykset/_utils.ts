import { db } from "~src/database";
import { sql, type InferResult } from "kysely";
import type { Persons } from "~src/database.gen";

export function urlencodeProposalId(proposalId: string) {
    return proposalId.replaceAll(" ", "+").replaceAll("/", "-");
}

/** Partial query for proposal data */
export function proposalData() {
    return db
        .selectFrom("proposals as p")
        .select([
            "p.id as id",
            "p.title as title",
            "p.ptype as proposer",
            "p.status as status",
            "p.summary as summary",
            "p.law_changes as law_changes",
            "p.date as date",
            sql<Signature[]>`(
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'id', prs.id,
                    'first_name', prs.first_name,
                    'last_name', prs.last_name,
                    'full_name', prs.full_name,
                    'photo', prs.photo,
                    'first', ps.first,
                    'party_id', mppm.pg_id
                ) ORDER BY (ps.first IS NOT TRUE), prs.last_name, prs.first_name
            ),
            '[]'::jsonb
        )
        FROM proposal_signatures ps
        JOIN persons prs ON prs.id = ps.person_id
        JOIN LATERAL (
            SELECT mppm.pg_id
            FROM mp_parliamentary_group_memberships AS mppm
            WHERE mppm.person_id = prs.id
            ORDER BY mppm.start_date DESC NULLS LAST, mppm.pg_id DESC
            LIMIT 1
        ) mppm ON TRUE
        WHERE ps.proposal_id = p.id
    )`.as("signatures"),
        ])
        .select((eb) =>
            eb
                .selectFrom("committee_reports as cr")
                .select((ceb) => ceb.fn.count("cr.id").as("count"))
                .whereRef("cr.proposal_id", "=", eb.ref("p.id"))
                .as("committeeReportCount"),
        )
        .orderBy("date", "desc");
}

export type Proposal = InferResult<ReturnType<typeof proposalData>>[0];

type Signature = Pick<Persons, "id" | "first_name" | "last_name" | "photo"> & {
    first: boolean;
    party_id: string;
};
