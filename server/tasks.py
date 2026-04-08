from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, List, Sequence, Tuple

from model import SqlDebugReward

Rows = List[Tuple]


@dataclass(frozen=True)
class TaskSpec:
    name: str
    description: str
    schema_sql: str
    seed_sql: str
    broken_artifact: str
    hints: List[str]
    max_steps: int
    expected_query: str
    grader: Callable[[str, str, int, int], SqlDebugReward]

    @property
    def full_schema(self) -> str:
        return f"{self.schema_sql}\n\n{self.seed_sql}".strip()


def _normalize_rows(rows: Sequence[Tuple]) -> Rows:
    return [tuple("" if c is None else str(c) for c in row) for row in rows]


def _safe_execute_query(conn: sqlite3.Connection, query: str) -> Rows:
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return _normalize_rows(rows)


def _jaccard_like_score(predicted: Rows, expected: Rows) -> float:
    if not expected and not predicted:
        return 1.0
    p = set(predicted)
    e = set(expected)
    intersection = len(p.intersection(e))
    union = max(1, len(p.union(e)))
    return intersection / union


def _efficiency_score(step_number: int, max_steps: int) -> float:
    extra_steps = max(0, step_number - 3)
    span = max(1, max_steps - 3)
    return max(0.0, 1.0 - (extra_steps / span))



def _heuristic_intent_score(task_name: str, submission_query: str) -> float:
    q = submission_query.lower()

    checks: dict[str, list[bool]] = {
        "fix_broken_join": [
            "t_evt_master" in q,
            "t_evt_type" in q,
            "t_wkf_step" in q,
            "et.evttypeid = e.evttypeid" in q or "e.evttypeid = et.evttypeid" in q,
            "2024-01-01" in q and "2024-02-01" in q,
            "order by" in q and "step_name" in q,
        ],
        "debug_etl_pipeline": [
            "t_ref_master" in q,
            "t_alt_master" in q,
            "t_wkf_step" in q,
            "count(distinct" in q,
            "group by" in q and "subject" in q,
            "lower(ws.name)" in q or "lower (ws.name)" in q,
            "open" in q and "close" in q,
            "landing" in q or "pending" in q or "escalation" in q,
        ],
        "optimize_slow_query": [
            "with " in q,
            "row_number()" in q,
            "partition by" in q,
            "rn = 1" in q or "rn=1" in q,
            "group by" in q and "subject" in q,
            "t_evt_master" in q and "t_evt_type" in q and "t_wkf_step" in q,
            "open" in q or "landing" in q or "pending" in q or "escalation" in q,
        ],
    }

    task_checks = checks.get(task_name, [])
    if not task_checks:
        return 0.0
    return sum(1.0 for ok in task_checks if ok) / len(task_checks)

def _generic_grader(task_name: str, submission_query: str, step_number: int, max_steps: int) -> SqlDebugReward:
    task = TASKS[task_name]
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(task.schema_sql)
        conn.executescript(task.seed_sql)

        expected_rows = _safe_execute_query(conn, task.expected_query)
        predicted_rows = _safe_execute_query(conn, submission_query)

        correctness = _jaccard_like_score(predicted_rows, expected_rows)
        progress = correctness
        efficiency = _efficiency_score(step_number, max_steps)

        return SqlDebugReward(
            total=round((0.6 * correctness) + (0.2 * efficiency) + (0.2 * progress), 4),
            correctness=round(0.6 * correctness, 4),
            efficiency=round(0.2 * efficiency, 4),
            progress=round(0.2 * progress, 4),
            step_bonus=0.0,
        )
    except Exception:
        efficiency = _efficiency_score(step_number, max_steps)
        intent_score = _heuristic_intent_score(task_name, submission_query)
        correctness = min(0.45, 0.45 * intent_score)
        progress = min(0.60, 0.60 * intent_score)
        return SqlDebugReward(
            total=round((0.6 * correctness) + (0.2 * efficiency) + (0.2 * progress), 4),
            correctness=round(0.6 * correctness, 4),
            efficiency=round(0.2 * efficiency, 4),
            progress=round(0.2 * progress, 4),
            step_bonus=0.0,
        )
    finally:
        conn.close()


def _fix_broken_join_grader(submission_query: str, task_name: str, step_number: int, max_steps: int) -> SqlDebugReward:
    return _generic_grader(task_name, submission_query, step_number, max_steps)


def _debug_etl_grader(submission_query: str, task_name: str, step_number: int, max_steps: int) -> SqlDebugReward:
    return _generic_grader(task_name, submission_query, step_number, max_steps)


def _optimize_query_grader(submission_query: str, task_name: str, step_number: int, max_steps: int) -> SqlDebugReward:
    return _generic_grader(task_name, submission_query, step_number, max_steps)


SCHEMA_SQL = """
CREATE TABLE T_XML_MASTER (
    xmlid INTEGER PRIMARY KEY,
    type TEXT,
    evtTypeId INTEGER,
    datecreated TEXT,
    dateedited TEXT,
    subjectId TEXT
);

CREATE TABLE T_ALT_TYPE (
    altTypeId INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE T_ALT_MASTER (
    invid INTEGER PRIMARY KEY,
    subject TEXT,
    datecreated TEXT,
    dateedited TEXT,
    stepsid INTEGER,
    xmlid INTEGER,
    altTypeId INTEGER
);

CREATE TABLE T_EVT_MASTER (
    invid INTEGER PRIMARY KEY,
    subject TEXT,
    datecreated TEXT,
    stepsid INTEGER,
    evtTypeId INTEGER,
    xmlid INTEGER
);

CREATE TABLE T_REF_MASTER (
    invid INTEGER PRIMARY KEY,
    subject TEXT,
    datecreated TEXT,
    stepsid INTEGER,
    refTypeId INTEGER,
    xmlid INTEGER
);

CREATE TABLE T_WKF_MASTER (
    workflowId INTEGER PRIMARY KEY,
    name TEXT,
    domainId INTEGER,
    stages TEXT
);

CREATE TABLE T_WKF_STEP (
    stepId INTEGER PRIMARY KEY,
    workflowId INTEGER,
    name TEXT,
    datemodified TEXT
);

CREATE TABLE T_EVT_TYPE (
    evtTypeId INTEGER PRIMARY KEY,
    name TEXT,
    domid INTEGER
);
""".strip()


SEED_SQL = """
INSERT INTO T_WKF_MASTER (workflowId, name, domainId, stages) VALUES
    (10, 'Event Workflow', 1, 'Landing,Escalation,Open,Close'),
    (20, 'Referral Workflow', 2, 'Open,Pending,Close'),
    (30, 'Alert Workflow', 3, 'Open,Close');

INSERT INTO T_WKF_STEP (stepId, workflowId, name, datemodified) VALUES
    (1, 10, 'Landing', '2024-01-01'),
    (2, 10, 'Escalation', '2024-01-03'),
    (3, 10, 'Open', '2024-01-05'),
    (4, 10, 'Close', '2024-01-10'),
    (5, 20, 'Open', '2024-01-02'),
    (6, 20, 'Pending', '2024-01-04'),
    (7, 20, 'Close', '2024-01-09'),
    (8, 30, 'Open', '2024-01-02'),
    (9, 30, 'Close', '2024-01-07');

INSERT INTO T_EVT_TYPE (evtTypeId, name, domid) VALUES
    (101, 'EFTR', 1),
    (102, 'LCTR', 1),
    (103, 'STR', 1);

INSERT INTO T_ALT_TYPE (altTypeId, name) VALUES
    (201, 'HighRisk'),
    (202, 'MediumRisk');

INSERT INTO T_XML_MASTER (xmlid, type, evtTypeId, datecreated, dateedited, subjectId) VALUES
    (1001, 'Event', 101, '2024-01-02', '2024-01-02', 'C001'),
    (1002, 'Event', 102, '2024-01-03', '2024-01-03', 'C002'),
    (1003, 'Referral', 101, '2024-01-05', '2024-01-06', 'C001'),
    (1004, 'Alert', 101, '2024-01-06', '2024-01-08', 'C001'),
    (1005, 'Event', 103, '2024-01-08', '2024-01-09', 'C003'),
    (1006, 'Referral', 102, '2024-01-09', '2024-01-10', 'C002'),
    (1007, 'Alert', 102, '2024-01-10', '2024-01-11', 'C002'),
    (1008, 'Event', 101, '2024-02-01', '2024-02-01', 'C004');

INSERT INTO T_EVT_MASTER (invid, subject, datecreated, stepsid, evtTypeId, xmlid) VALUES
    (1, 'Alice', '2024-01-02', 1, 101, 1001),
    (2, 'Bob', '2024-01-03', 2, 102, 1002),
    (3, 'Cara', '2024-01-08', 4, 103, 1005),
    (4, 'Dan', '2024-02-01', 3, 101, 1008),
    (5, 'Evan', '2024-01-11', 3, 101, 1001);

INSERT INTO T_REF_MASTER (invid, subject, datecreated, stepsid, refTypeId, xmlid) VALUES
    (11, 'Alice', '2024-01-05', 5, 301, 1003),
    (12, 'Bob', '2024-01-09', 6, 302, 1006),
    (13, 'Cara', '2024-01-10', 7, 301, 1005),
    (14, 'Alice', '2024-01-12', 6, 303, 1003);

INSERT INTO T_ALT_MASTER (invid, subject, datecreated, dateedited, stepsid, xmlid, altTypeId) VALUES
    (21, 'Alice', '2024-01-06', '2024-01-08', 8, 1004, 201),
    (22, 'Bob', '2024-01-10', '2024-01-11', 9, 1007, 202),
    (23, 'Alice', '2024-01-13', '2024-01-14', 8, 1004, 201);
""".strip()


TASKS: dict[str, TaskSpec] = {
    "fix_broken_join": TaskSpec(
        name="fix_broken_join",
        description=(
            "Return January 2024 events with client name, event type code, and current event workflow step. "
            "The broken query has three issues: wrong join key to event type, invalid date filter range, and alias typo in ORDER BY."
        ),
        schema_sql=SCHEMA_SQL,
        seed_sql=SEED_SQL,
        broken_artifact="""
SELECT e.subject, et.name AS event_type, ws.name AS step_name, e.datecreated
FROM T_EVT_MASTER e
JOIN T_EVT_TYPE et ON et.evtTypeId = e.xmlid
JOIN T_WKF_STEP ws ON ws.stepId = e.stepsid
WHERE e.datecreated >= '2024-01-01' AND e.datecreated < '2024-01-31'
ORDER BY step_nam, e.datecreated;
""".strip(),
        hints=[
            "`evtTypeId` should join to `T_EVT_TYPE.evtTypeId`, not `xmlid`.",
            "Use an inclusive January window ending before 2024-02-01.",
            "Check the ORDER BY alias spelling.",
        ],
        max_steps=8,
        expected_query="""
SELECT e.subject, et.name AS event_type, ws.name AS step_name, e.datecreated
FROM T_EVT_MASTER e
JOIN T_EVT_TYPE et ON et.evtTypeId = e.evtTypeId
JOIN T_WKF_STEP ws ON ws.stepId = e.stepsid
WHERE e.datecreated >= '2024-01-01' AND e.datecreated < '2024-02-01'
ORDER BY step_name, e.datecreated;
""".strip(),
        grader=_fix_broken_join_grader,
    ),
    "debug_etl_pipeline": TaskSpec(
        name="debug_etl_pipeline",
        description=(
            "Return the queue status of Event and Referral records, ranked by latest invid within each type/domain. "
            "The desired result is the top 5 queue items for domain EFTR using open-like workflow steps."
        ),
        schema_sql=SCHEMA_SQL,
        seed_sql=SEED_SQL,
        broken_artifact="""
WITH INV_DATA AS (
    SELECT 'REFERRAL' AS TYPE, r.invid, ws.name AS step_name, et.name AS domain
    FROM T_REF_MASTER r
    LEFT JOIN T_XML_MASTER x ON x.xmlid = r.xmlid
    LEFT JOIN T_EVT_TYPE et ON et.evtTypeId = r.refTypeId
    LEFT JOIN T_WKF_STEP ws ON ws.stepId = r.stepsid
    WHERE ws.name = 'Open'
    UNION
    SELECT 'EVENT' AS TYPE, e.invid, ws.name AS step_name, et.name AS domain
    FROM T_EVT_MASTER e
    LEFT JOIN T_EVT_TYPE et ON et.evtTypeId = e.evtTypeId
    LEFT JOIN T_WKF_STEP ws ON ws.stepId = e.stepsid
    WHERE ws.name = 'Open'
),
TOP_FIVE_DATA AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY TYPE ORDER BY invid DESC, domain) AS row_num, *
    FROM INV_DATA
)
SELECT *
FROM TOP_FIVE_DATA
WHERE DOMAIN = 'EFTR' AND ROW_NUM <= 5;
""".strip(),
        hints=[
            "Referral domain should come via `T_XML_MASTER.evtTypeId`, not `refTypeId`.",
            "Queue status should include open-like steps: open, landing, pending, escalation.",
            "Rank within both `TYPE` and `DOMAIN`, then keep top 5 rows for `EFTR`.",
        ],
        max_steps=10,
        expected_query="""
WITH inv_data AS (
    SELECT
        'REFERRAL' AS type,
        r.invid,
        ws.name AS step_name,
        et.name AS domain
    FROM T_REF_MASTER r
    LEFT JOIN T_XML_MASTER x ON x.xmlid = r.xmlid
    LEFT JOIN T_EVT_TYPE et ON et.evtTypeId = x.evtTypeId
    LEFT JOIN T_WKF_STEP ws ON ws.stepId = r.stepsid
    WHERE LOWER(ws.name) IN ('open', 'landing', 'pending', 'escalation')

    UNION ALL

    SELECT
        'EVENT' AS type,
        e.invid,
        ws.name AS step_name,
        et.name AS domain
    FROM T_EVT_MASTER e
    LEFT JOIN T_EVT_TYPE et ON et.evtTypeId = e.evtTypeId
    LEFT JOIN T_WKF_STEP ws ON ws.stepId = e.stepsid
    WHERE LOWER(ws.name) IN ('open', 'landing', 'pending', 'escalation')
),
top_five_data AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY type, domain ORDER BY invid DESC) AS row_num,
        type,
        invid,
        step_name,
        domain
    FROM inv_data
)
SELECT type, invid, step_name, domain, row_num
FROM top_five_data
WHERE domain = 'EFTR' AND row_num <= 5
ORDER BY type, row_num;
""".strip(),
        grader=_debug_etl_grader,
    ),
    "optimize_slow_query": TaskSpec(
        name="optimize_slow_query",
        description=(
            "Find the top client per event type by event volume for January open-like stages "
            "(open/landing/pending/escalation). The broken query is both slow and logically wrong."
        ),
        schema_sql=SCHEMA_SQL,
        seed_sql=SEED_SQL,
        broken_artifact="""
SELECT et.name AS event_type, e.subject, COUNT(*) AS event_count
FROM T_EVT_MASTER e
JOIN T_EVT_TYPE et ON et.evtTypeId = e.evtTypeId
JOIN T_WKF_STEP ws ON ws.stepId = e.stepsid
WHERE e.datecreated >= '2024-01-01' AND e.datecreated < '2024-02-01'
  AND ws.name = 'Open'
  AND COUNT(*) = (
      SELECT MAX(COUNT(*))
      FROM T_EVT_MASTER e2
      JOIN T_EVT_TYPE et2 ON et2.evtTypeId = e2.evtTypeId
      WHERE et2.name = et.name
      GROUP BY e2.subject
  )
GROUP BY et.name, e.subject
ORDER BY event_count DESC;
""".strip(),
        hints=[
            "You cannot nest COUNT aggregate that way inside WHERE.",
            "Pre-aggregate by event type + subject, then rank.",
            "Include open-like stages using LOWER(ws.name) IN (...).",
        ],
        max_steps=10,
        expected_query="""
WITH subject_totals AS (SELECT
        et.name AS event_type,
        e.subject,
        COUNT(*) AS event_count
    FROM T_EVT_MASTER e
    JOIN T_EVT_TYPE et ON et.evtTypeId = e.evtTypeId
    JOIN T_WKF_STEP ws ON ws.stepId = e.stepsid
    WHERE e.datecreated >= '2024-01-01'
      AND e.datecreated < '2024-02-01'
      AND LOWER(ws.name) IN ('open', 'landing', 'pending', 'escalation')
    GROUP BY et.name, e.subject
),
ranked AS (
    SELECT
        event_type,
        subject,
        event_count,
        ROW_NUMBER() OVER (
            PARTITION BY event_type
            ORDER BY event_count DESC, subject ASC
        ) AS rn
    FROM subject_totals
)
SELECT event_type, subject, event_count
FROM ranked
WHERE rn = 1
ORDER BY event_type;
""".strip(),
        grader=_optimize_query_grader,
    ),
}


