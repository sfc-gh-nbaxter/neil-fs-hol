import json

cells = []

def _split(source):
    lines = source.split("\n")
    return [line + "\n" if i < len(lines) - 1 else line for i, line in enumerate(lines)]

def md(source):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": _split(source)})

def sql(source):
    cells.append({"cell_type": "code", "execution_count": None, "metadata": {"language": "sql"}, "outputs": [], "source": _split(source)})

# =============================================================================
# TITLE
# =============================================================================
md("""# Financial Services Risk Management — Hands-On Lab

| Detail | Value |
|---|---|
| **Duration** | ~90 minutes |
| **Prerequisites** | Snowflake account (Enterprise or 30-day trial) |
| **Warehouse** | SMALL, auto-suspend 60 s |

**Snowflake features covered:** Cross-Region Inference, Cortex Code, Databases & Schemas, Virtual Warehouses, RBAC, VARIANT & semi-structured data, LATERAL FLATTEN, Dynamic Tables, Dynamic Data Masking, Row Access Policies, Zero-Copy Cloning, Time Travel, UNDROP, Internal Stages, Directory Tables, Marketplace, Streamlit in Snowflake.

---

### Data Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RISK_HOL  (Database)                        │
├──────────────┬──────────────┬───────────────┬───────────────────────┤
│  RAW_DATA    │  ANALYTICS   │  GOVERNANCE   │  UNSTRUCTURED         │
│  (Schema)    │  (Schema)    │  (Schema)     │  (Schema)             │
│              │              │               │                       │
│ counterpar-  │ risk_events  │ email_mask    │ risk_documents_stage  │
│   ties       │  (Dynamic    │ phone_mask    │   (Internal Stage +   │
│              │   Table)     │  (Masking     │    Directory Table)   │
│ risk_events  │              │   Policies)   │                       │
│   _raw       │ risk_summary │               │ document_catalogue    │
│  (VARIANT    │  (Dynamic    │ risk_severity │                       │
│   JSON)      │   Table)     │   _policy     │                       │
│              │              │  (Row Access  │                       │
│              │              │   Policy)     │                       │
└──────┬───────┴──────▲───────┴───────────────┴───────────────────────┘
       │              │
       │   ┌──────────┴──────────┐
       └──►│   Dynamic Tables    │
           │   (Automated ELT)   │
           │                     │
           │  risk_events_raw    │
           │    ──[LAG 1 min]──► │  risk_events
           │    ──[LAG 2 min]──► │  risk_summary
           └─────────────────────┘

┌──────────────────────┐    ┌──────────────────────┐
│  Snowflake           │    │  Streamlit in         │
│  Marketplace         │    │  Snowflake            │
│  (Cybersyn)          │◄──►│  (Risk Dashboard)     │
└──────────────────────┘    └──────────────────────┘

Roles:  RISK_ADMIN  ──►  RISK_ANALYST  ──►  RISK_AUDITOR
        (full access)    (masked PII)      (HIGH/CRITICAL only)

Warehouse:  RISK_WH  (SMALL, auto-suspend 60s)
```""")

# =============================================================================
# STEP 1 — CORTEX CODE PREREQUISITES
# =============================================================================
md("""---
## 1 · Enable Cortex Code

**Cortex Code** is Snowflake's AI coding agent — it can write SQL, build Streamlit apps, create notebooks, and answer questions about your account. It is available in Snowsight and as a CLI.

**Prerequisite:** Cross-region inference must be enabled so Cortex Code can access the required LLMs.""")

md("""### 1.1 — Enable Cross-Region Inference
This allows Snowflake to route AI model requests to regions where the models are available. Required for Cortex Code (and all Cortex AI features).""")

sql("""USE ROLE accountadmin;

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';""")

md("""### 1.2 — Verify Cross-Region Inference""")

sql("""SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;""")

md("""### 1.3 — Open Cortex Code
You can now access Cortex Code in Snowsight:

1. Click the **Cortex Code** icon (sparkle ✦) in the left navigation bar — or press **Cmd + J** (Mac) / **Ctrl + J** (Windows)
2. Cortex Code opens as a side panel and is context-aware of the notebook you have open
3. You can ask it natural-language questions, generate SQL, or build entire applications

> **Tip:** We will use Cortex Code later in this lab to build a Streamlit risk dashboard from a single prompt.""")

# =============================================================================
# STEP 2 — SETUP
# =============================================================================
md("""---
## 2 · Setup Infrastructure""")

md("""### 2.1 — Create Database & Schemas
Snowflake **databases** are the top-level container for all objects. **Schemas** organise tables, views, and policies into logical groups.""")

sql("""USE ROLE sysadmin;

CREATE OR REPLACE DATABASE risk_hol
    COMMENT = 'Financial Services Risk Management Hands-On Lab';

CREATE OR REPLACE SCHEMA risk_hol.raw_data
    COMMENT = 'Raw ingested data (landing zone)';

CREATE OR REPLACE SCHEMA risk_hol.analytics
    COMMENT = 'Analytical views and transformed data';

CREATE OR REPLACE SCHEMA risk_hol.governance
    COMMENT = 'Masking policies, row-access policies';

CREATE OR REPLACE SCHEMA risk_hol.unstructured
    COMMENT = 'Unstructured document storage and processing';""")

md("""### 2.2 — Create a Virtual Warehouse
A **Virtual Warehouse** provides compute for queries. Key settings:
- `WAREHOUSE_SIZE` — controls compute power (XSMALL → 6XLARGE)
- `AUTO_SUSPEND` — shuts down after N seconds of idle (saves credits)
- `AUTO_RESUME` — wakes automatically on the next query""")

sql("""CREATE OR REPLACE WAREHOUSE risk_wh
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND  = 60
    AUTO_RESUME   = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute warehouse for Risk HOL';""")

sql("""USE WAREHOUSE risk_wh;
USE DATABASE  risk_hol;
USE SCHEMA    raw_data;""")

md("""### 2.3 — Role-Based Access Control (RBAC)
Snowflake enforces access through **roles**. We create a three-tier hierarchy:

| Role | Purpose |
|---|---|
| `risk_admin` | Full access — owns all objects |
| `risk_analyst` | Read analytics, PII is masked |
| `risk_auditor` | Read-only, sees only HIGH / CRITICAL events |""")

sql("""USE ROLE securityadmin;

-- Create the role hierarchy
CREATE ROLE IF NOT EXISTS risk_admin
    COMMENT = 'Admin role for the Risk HOL';

CREATE ROLE IF NOT EXISTS risk_analyst
    COMMENT = 'Analyst role — analytics only, no PII';

CREATE ROLE IF NOT EXISTS risk_auditor
    COMMENT = 'Auditor role — read-only, limited rows';

GRANT ROLE risk_admin   TO ROLE sysadmin;
GRANT ROLE risk_analyst TO ROLE risk_admin;
GRANT ROLE risk_auditor TO ROLE risk_admin;""")

md("""### 2.3b — Grant Privileges
Grant each role the minimum privileges it needs on the database, schemas, and warehouse.""")

sql("""-- Database & schema access
GRANT USAGE ON DATABASE risk_hol TO ROLE risk_admin;
GRANT USAGE ON DATABASE risk_hol TO ROLE risk_analyst;
GRANT USAGE ON DATABASE risk_hol TO ROLE risk_auditor;

GRANT USAGE ON ALL SCHEMAS IN DATABASE risk_hol TO ROLE risk_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE risk_hol TO ROLE risk_analyst;
GRANT USAGE ON ALL SCHEMAS IN DATABASE risk_hol TO ROLE risk_auditor;

GRANT ALL ON SCHEMA risk_hol.raw_data       TO ROLE risk_admin;
GRANT ALL ON SCHEMA risk_hol.analytics      TO ROLE risk_admin;
GRANT ALL ON SCHEMA risk_hol.governance     TO ROLE risk_admin;
GRANT ALL ON SCHEMA risk_hol.unstructured   TO ROLE risk_admin;

-- Future grants so new objects are automatically accessible
GRANT SELECT ON FUTURE TABLES IN SCHEMA risk_hol.analytics TO ROLE risk_analyst;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA risk_hol.analytics TO ROLE risk_analyst;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA risk_hol.analytics TO ROLE risk_auditor;""")

sql("""-- Warehouse & account-level privileges
USE ROLE accountadmin;

GRANT USAGE ON WAREHOUSE risk_wh TO ROLE risk_admin;
GRANT USAGE ON WAREHOUSE risk_wh TO ROLE risk_analyst;
GRANT USAGE ON WAREHOUSE risk_wh TO ROLE risk_auditor;

GRANT APPLY MASKING POLICY    ON ACCOUNT TO ROLE risk_admin;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE risk_admin;""")

sql("""-- Set working context
USE ROLE      risk_admin;
USE WAREHOUSE risk_wh;
USE DATABASE  risk_hol;

SELECT 'Setup complete' AS status;""")

md("""### 2.4 — Verify Setup
Use `SHOW` commands to confirm the objects exist.""")

sql("""SHOW SCHEMAS IN DATABASE risk_hol;""")

sql("""SHOW WAREHOUSES LIKE 'RISK%';""")

sql("""SHOW ROLES LIKE 'RISK%';""")

# =============================================================================
# STEP 3 — SEMI-STRUCTURED DATA
# =============================================================================
md("""---
## 3 · Ingest & Transform Semi-Structured Data""")

md("""### 3.1 — Create the Counterparties Table
Use `GENERATOR` to produce 500 synthetic rows and `ARRAY_CONSTRUCT` to pick random values — no CSV files needed.""")

sql("""USE ROLE      risk_admin;
USE DATABASE  risk_hol;
USE SCHEMA    raw_data;
USE WAREHOUSE risk_wh;

CREATE OR REPLACE TABLE counterparties (
    counterparty_id   VARCHAR(10)  PRIMARY KEY,
    legal_name        VARCHAR(200),
    country           VARCHAR(50),
    sector            VARCHAR(50),
    credit_rating     VARCHAR(5),
    lei               VARCHAR(20),
    pii_contact_email VARCHAR(100),
    pii_phone         VARCHAR(30),
    onboarding_date   DATE,
    is_active         BOOLEAN DEFAULT TRUE
);""")

sql("""INSERT INTO counterparties
SELECT
    'CP' || LPAD(SEQ4()::VARCHAR, 6, '0'),

    ARRAY_CONSTRUCT(
        'Meridian Capital','Atlas Holdings','Vanguard Finance',
        'Pinnacle Investments','Sterling Bank','Oceanic Securities',
        'Northern Trust Corp','Pacific Ventures','Eagle Trading',
        'Falcon Asset Mgmt'
    )[UNIFORM(0, 9, RANDOM())]::VARCHAR
        || ' ' || UNIFORM(1, 999, RANDOM())::VARCHAR,

    ARRAY_CONSTRUCT('US','UK','DE','JP','SG','CH','AU','CA','FR','BR')
        [UNIFORM(0, 9, RANDOM())]::VARCHAR,

    ARRAY_CONSTRUCT('Banking','Insurance','Asset Management','Hedge Fund',
        'Private Equity','Pension Fund','Sovereign Wealth')
        [UNIFORM(0, 6, RANDOM())]::VARCHAR,

    ARRAY_CONSTRUCT('AAA','AA+','AA','AA-','A+','A','A-','BBB+','BBB','BBB-','BB+','BB')
        [UNIFORM(0, 11, RANDOM())]::VARCHAR,

    UPPER(SUBSTRING(MD5(RANDOM()::VARCHAR), 1, 20)),

    'contact' || SEQ4() || '@'
        || ARRAY_CONSTRUCT('meridian.com','atlas.io','vanguard.net','sterling.co.uk')
           [UNIFORM(0, 3, RANDOM())]::VARCHAR,

    '+' || UNIFORM(1, 9, RANDOM())::VARCHAR
        || LPAD(UNIFORM(100000000, 999999999, RANDOM())::VARCHAR, 9, '0'),

    DATEADD('day', -UNIFORM(30, 3650, RANDOM()), CURRENT_DATE()),
    TRUE
FROM TABLE(GENERATOR(ROWCOUNT => 500));""")

md("""### 3.2 — Load Semi-Structured JSON into VARIANT
Snowflake's **VARIANT** type stores JSON, Avro, Parquet, or XML natively. `OBJECT_CONSTRUCT` builds JSON objects; `ARRAY_CONSTRUCT` builds arrays — all inside a single INSERT.""")

sql("""CREATE OR REPLACE TABLE risk_events_raw (
    event_id     VARCHAR(20) PRIMARY KEY,
    ingestion_ts TIMESTAMP   DEFAULT CURRENT_TIMESTAMP(),
    payload      VARIANT
);""")

sql("""INSERT INTO risk_events_raw (event_id, payload)
SELECT
    'EVT' || LPAD(SEQ4()::VARCHAR, 10, '0'),
    OBJECT_CONSTRUCT(
        'event_type',
            ARRAY_CONSTRUCT('CREDIT','MARKET','OPERATIONAL','LIQUIDITY','COUNTERPARTY')
            [UNIFORM(0, 4, RANDOM())]::VARCHAR,
        'event_date',
            DATEADD('day', -UNIFORM(1, 730, RANDOM()), CURRENT_DATE())::VARCHAR,
        'counterparty_id',
            'CP' || LPAD(UNIFORM(0, 499, RANDOM())::VARCHAR, 6, '0'),
        'exposure_usd',
            ROUND(UNIFORM(10000, 50000000, RANDOM())::FLOAT, 2),
        'currency',
            ARRAY_CONSTRUCT('USD','EUR','GBP','JPY','CHF')
            [UNIFORM(0, 4, RANDOM())]::VARCHAR,
        'risk_score',
            ROUND(UNIFORM(1, 100, RANDOM())::FLOAT, 1),
        'region',
            ARRAY_CONSTRUCT('AMERICAS','EMEA','APAC')
            [UNIFORM(0, 2, RANDOM())]::VARCHAR,
        'description',
            ARRAY_CONSTRUCT(
                'Limit breach on trading book',
                'Margin call trigger event',
                'System outage in settlement',
                'Failed trade reconciliation',
                'VaR exceedance detected',
                'Collateral shortfall',
                'Regulatory capital threshold',
                'Counterparty downgrade alert'
            )[UNIFORM(0, 7, RANDOM())]::VARCHAR,
        'severity',
            ARRAY_CONSTRUCT('LOW','MEDIUM','HIGH','CRITICAL')
            [UNIFORM(0, 3, RANDOM())]::VARCHAR,
        'status',
            ARRAY_CONSTRUCT('OPEN','INVESTIGATING','MITIGATED','CLOSED')
            [UNIFORM(0, 3, RANDOM())]::VARCHAR,
        'mitigation_actions',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'action',   'Increase collateral',
                    'owner',    'Risk Ops',
                    'deadline', DATEADD('day', UNIFORM(1, 30, RANDOM()), CURRENT_DATE())::VARCHAR
                ),
                OBJECT_CONSTRUCT(
                    'action',   'Reduce exposure',
                    'owner',    'Trading Desk',
                    'deadline', DATEADD('day', UNIFORM(1, 14, RANDOM()), CURRENT_DATE())::VARCHAR
                )
            )
    )
FROM TABLE(GENERATOR(ROWCOUNT => 10000));""")

md("""### 3.3 — Query VARIANT with Colon Notation
Access JSON fields directly using **colon notation** (`payload:field`) and cast to SQL types with `::STRING`, `::DATE`, etc.""")

sql("""SELECT
    event_id,
    payload:event_type::STRING       AS event_type,
    payload:event_date::DATE         AS event_date,
    payload:counterparty_id::STRING  AS counterparty_id,
    payload:exposure_usd::NUMBER(15,2) AS exposure_usd,
    payload:risk_score::FLOAT        AS risk_score,
    payload:severity::STRING         AS severity,
    payload:status::STRING           AS status
FROM risk_events_raw
LIMIT 20;""")

md("""### 3.3b — LATERAL FLATTEN for Nested Arrays
`LATERAL FLATTEN` explodes a nested JSON array into rows — one row per array element. This is how you parse repeated structures like `mitigation_actions`.""")

sql("""SELECT
    r.event_id,
    r.payload:event_type::STRING  AS event_type,
    f.value:action::STRING        AS action,
    f.value:owner::STRING         AS owner,
    f.value:deadline::DATE        AS deadline
FROM risk_events_raw r,
    LATERAL FLATTEN(INPUT => r.payload:mitigation_actions) f
LIMIT 20;""")

md("""### 3.4 — Dynamic Tables (Automated ELT)
**Dynamic Tables** continuously transform data as the source changes — no orchestrator or scheduled tasks required. The `LAG` parameter sets the maximum acceptable staleness.""")

sql("""CREATE OR REPLACE DYNAMIC TABLE analytics.risk_events
    LAG       = '1 minute'
    WAREHOUSE = risk_wh
AS
SELECT
    r.event_id,
    r.payload:event_type::STRING       AS event_type,
    r.payload:event_date::DATE         AS event_date,
    r.payload:counterparty_id::STRING  AS counterparty_id,
    r.payload:exposure_usd::NUMBER(15,2) AS exposure_usd,
    r.payload:currency::STRING         AS currency,
    r.payload:risk_score::FLOAT        AS risk_score,
    r.payload:region::STRING           AS region,
    r.payload:description::STRING      AS description,
    r.payload:severity::STRING         AS severity,
    r.payload:status::STRING           AS status
FROM raw_data.risk_events_raw r;""")

md("""### 3.4b — Summary Dynamic Table (Chained Pipeline)
Dynamic Tables can reference other Dynamic Tables. Snowflake automatically builds a refresh DAG. View it in **Data > Databases > Dynamic Tables > Graph**.""")

sql("""CREATE OR REPLACE DYNAMIC TABLE analytics.risk_summary
    LAG       = '2 minutes'
    WAREHOUSE = risk_wh
AS
SELECT
    re.event_type,
    re.severity,
    re.region,
    DATE_TRUNC('MONTH', re.event_date) AS month,
    COUNT(*)                           AS event_count,
    SUM(re.exposure_usd)               AS total_exposure,
    ROUND(AVG(re.risk_score), 1)       AS avg_risk_score,
    COUNT(CASE WHEN re.status = 'OPEN' THEN 1 END) AS open_events
FROM analytics.risk_events re
GROUP BY re.event_type, re.severity, re.region, DATE_TRUNC('MONTH', re.event_date);""")

md("""### 3.5 — Verify Dynamic Tables""")

sql("""SELECT * FROM analytics.risk_events LIMIT 10;""")

sql("""SELECT * FROM analytics.risk_summary ORDER BY month DESC LIMIT 20;""")

# =============================================================================
# STEP 3.5 — BUILD A STREAMLIT APP WITH CORTEX CODE
# =============================================================================
md("""---
## 🤖 Cortex Code Challenge — Build a Risk Dashboard with AI

Now that the data pipeline is running, let's use **Cortex Code** to build a **Streamlit in Snowflake** app — entirely from a natural-language prompt.

### Instructions
1. Open **Cortex Code** — click the ✦ icon in the left sidebar (or **Cmd/Ctrl + J**)
2. Copy and paste the prompt below into the Cortex Code chat
3. Review the generated code, then click **Accept** to create the app
4. Click **Run** to launch the app in Snowsight

### Prompt

```
Create a Streamlit in Snowflake app called RISK_DASHBOARD in the RISK_HOL.ANALYTICS schema
that connects to the RISK_HOL.ANALYTICS.RISK_SUMMARY dynamic table using warehouse RISK_WH
and role RISK_ADMIN.

The app should have:

1. A title "Risk Exposure Dashboard" with a subtitle showing today's date
2. A sidebar with three dropdown filters:
   - Region (AMERICAS, EMEA, APAC, or All)
   - Severity (LOW, MEDIUM, HIGH, CRITICAL, or All)
   - Event Type (CREDIT, MARKET, OPERATIONAL, LIQUIDITY, COUNTERPARTY, or All)
3. A row of four KPI metric cards at the top showing:
   - Total Events
   - Total Exposure (formatted as $X.XM or $X.XB)
   - Average Risk Score
   - Open Events
4. A bar chart showing Total Exposure by Event Type using Altair
5. A line chart showing Event Count by Month using Altair
6. A heatmap or grouped bar chart showing Event Count by Severity and Region
7. A data table at the bottom with the filtered data

Use st.columns for layout, st.metric for KPIs, and Altair for all charts.
Apply the filters from the sidebar to all visuals and metrics.
Use @st.cache_data with a TTL of 60 seconds for the data query.
```

> **What just happened?** Cortex Code read your prompt, understood the database schema, and generated a complete Streamlit app with interactive filters, KPI cards, and Altair charts — deployed directly in Snowflake with no local environment needed.""")

# =============================================================================
# STEP 4 — SECURITY
# =============================================================================
md("""---
## 4 · Security & Governance""")

md("""### 4.1 — Dynamic Data Masking
**Masking policies** replace sensitive column values at query time based on the caller's role. The policy is attached to a column — no application code changes needed.""")

sql("""USE ROLE     risk_admin;
USE DATABASE risk_hol;
USE SCHEMA   governance;""")

sql("""CREATE OR REPLACE MASKING POLICY email_mask
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('RISK_ADMIN', 'ACCOUNTADMIN', 'SYSADMIN')
            THEN val
        ELSE REGEXP_REPLACE(val, '.+@', '****@')
    END;""")

sql("""CREATE OR REPLACE MASKING POLICY phone_mask
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('RISK_ADMIN', 'ACCOUNTADMIN', 'SYSADMIN')
            THEN val
        ELSE CONCAT('***-***-', RIGHT(val, 4))
    END;""")

md("""### 4.1b — Apply Masking Policies to Columns
Attach each policy to its target column with `ALTER TABLE ... SET MASKING POLICY`.""")

sql("""ALTER TABLE raw_data.counterparties
    MODIFY COLUMN pii_contact_email
    SET MASKING POLICY governance.email_mask;

ALTER TABLE raw_data.counterparties
    MODIFY COLUMN pii_phone
    SET MASKING POLICY governance.phone_mask;""")

md("""### 4.2 — Test Masking: Admin View (Unmasked)
As `risk_admin`, you see the real PII values.""")

sql("""USE ROLE risk_admin;

SELECT counterparty_id, legal_name, pii_contact_email, pii_phone
FROM raw_data.counterparties
LIMIT 5;""")

md("""### 4.2b — Test Masking: Analyst View (Masked)
As `risk_analyst`, email and phone are automatically redacted.""")

sql("""GRANT SELECT ON TABLE raw_data.counterparties TO ROLE risk_analyst;

USE ROLE risk_analyst;

SELECT counterparty_id, legal_name, pii_contact_email, pii_phone
FROM raw_data.counterparties
LIMIT 5;""")

sql("""USE ROLE risk_admin;""")

md("""### 4.3 — Row Access Policy (Row-Level Security)
A **Row Access Policy** filters rows at query time based on the caller's role. This policy limits visibility by event severity:

| Role | Sees |
|---|---|
| `risk_admin` | All rows |
| `risk_analyst` | Everything except CRITICAL |
| `risk_auditor` | HIGH and CRITICAL only |""")

sql("""CREATE OR REPLACE ROW ACCESS POLICY governance.risk_severity_policy
    AS (severity STRING) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('RISK_ADMIN', 'ACCOUNTADMIN', 'SYSADMIN')
            THEN TRUE
        WHEN CURRENT_ROLE() = 'RISK_ANALYST' AND severity != 'CRITICAL'
            THEN TRUE
        WHEN CURRENT_ROLE() = 'RISK_AUDITOR' AND severity IN ('CRITICAL', 'HIGH')
            THEN TRUE
        ELSE FALSE
    END;""")

sql("""ALTER DYNAMIC TABLE analytics.risk_events
    ADD ROW ACCESS POLICY governance.risk_severity_policy ON (severity);""")

md("""### 4.3b — Test RLS: Auditor View
The auditor should only see HIGH and CRITICAL severity events.""")

sql("""GRANT SELECT ON DYNAMIC TABLE analytics.risk_events TO ROLE risk_auditor;

USE ROLE risk_auditor;

SELECT severity, COUNT(*) AS event_count
FROM analytics.risk_events
GROUP BY severity;""")

sql("""USE ROLE risk_admin;""")

# =============================================================================
# STEP 5 — TIME TRAVEL & CLONING
# =============================================================================
md("""---
## 5 · Time Travel & Zero-Copy Cloning""")

md("""### 5.1 — Zero-Copy Clone
`CLONE` creates an instant, metadata-only copy of a table (or database/schema). No data is physically duplicated — storage is shared until one side diverges.""")

sql("""USE ROLE risk_admin;

CREATE OR REPLACE TABLE raw_data.counterparties_dev
    CLONE raw_data.counterparties;

SELECT 'PRODUCTION' AS source, COUNT(*) AS row_count FROM raw_data.counterparties
UNION ALL
SELECT 'DEV CLONE'  AS source, COUNT(*) AS row_count FROM raw_data.counterparties_dev;""")

md("""### 5.2 — Simulate an Accidental Update
Oops — someone ran an UPDATE without a WHERE clause.""")

sql("""UPDATE raw_data.counterparties
SET is_active = FALSE;

SELECT COUNT(*) AS active_count
FROM raw_data.counterparties
WHERE is_active = TRUE;""")

md("""### 5.3 — Recover with Time Travel
**Time Travel** lets you query or restore data as it existed at any point within the retention window (1 day on trial, up to 90 days on Enterprise). Use `AT(OFFSET => -N)` where N is seconds in the past.""")

sql("""SELECT
    (SELECT COUNT(*) FROM raw_data.counterparties WHERE is_active = TRUE)
        AS current_active,
    (SELECT COUNT(*) FROM raw_data.counterparties AT(OFFSET => -60*5) WHERE is_active = TRUE)
        AS five_min_ago_active;""")

sql("""CREATE OR REPLACE TABLE raw_data.counterparties
    AS SELECT * FROM raw_data.counterparties AT(OFFSET => -60*5);

SELECT COUNT(*) AS active_count
FROM raw_data.counterparties
WHERE is_active = TRUE;""")

md("""### 5.4 — UNDROP
`UNDROP` recovers a dropped table, schema, or database within the Time Travel retention window — no backup restore needed.""")

sql("""DROP TABLE raw_data.counterparties_dev;""")

sql("""UNDROP TABLE raw_data.counterparties_dev;

SELECT COUNT(*) AS row_count FROM raw_data.counterparties_dev;""")

sql("""-- Final cleanup of the dev clone
DROP TABLE raw_data.counterparties_dev;""")

# =============================================================================
# STEP 6 — UNSTRUCTURED DATA
# =============================================================================
md("""---
## 6 · Unstructured Data""")

md("""### 6.1 — Internal Stage with Directory Table
An **Internal Stage** stores files (PDFs, images, CSVs, etc.) inside Snowflake. Enabling `DIRECTORY` adds an auto-populated metadata catalogue you can query with SQL.""")

sql("""USE SCHEMA unstructured;

CREATE OR REPLACE STAGE risk_documents_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT   = 'Internal stage for regulatory and risk report documents';""")

md("""### 6.2 — Document Catalogue (Simulated)
In production, upload files via `PUT` or the Snowsight UI and query `DIRECTORY(@stage)`. Here we simulate a catalogue table for six typical risk management documents.""")

sql("""CREATE OR REPLACE TABLE document_catalogue (
    doc_id       VARCHAR(10)  PRIMARY KEY,
    file_name    VARCHAR(200),
    doc_type     VARCHAR(50),
    department   VARCHAR(50),
    upload_date  DATE,
    file_size_kb NUMBER,
    summary      VARCHAR(500)
);

INSERT INTO document_catalogue VALUES
    ('DOC001', 'Q4_2025_VaR_Report.pdf',          'Risk Report',       'Market Risk',
        '2025-12-15', 2450, 'Quarterly Value-at-Risk report covering equity, FX, and rates portfolios'),
    ('DOC002', 'Basel_III_Capital_Adequacy.pdf',   'Regulatory Filing', 'Compliance',
        '2025-11-30', 5800, 'Basel III capital adequacy submission for the supervisory authority'),
    ('DOC003', 'Operational_Risk_Incident_Log.pdf','Incident Report',   'Operational Risk',
        '2026-01-10', 1200, 'Monthly operational risk incident log and loss event summary'),
    ('DOC004', 'Counterparty_Credit_Review.pdf',   'Credit Report',     'Credit Risk',
        '2026-02-01', 3400, 'Annual counterparty credit worthiness review and rating assessment'),
    ('DOC005', 'Stress_Test_Results_2025.pdf',     'Stress Test',       'Enterprise Risk',
        '2025-12-20', 7800, 'Annual stress test results under adverse and severely adverse scenarios'),
    ('DOC006', 'AML_SAR_Filing_Template.pdf',      'Compliance',        'Financial Crime',
        '2026-01-15',  890, 'Suspicious Activity Report template and filing guidance');""")

sql("""SELECT doc_type, COUNT(*) AS doc_count, SUM(file_size_kb) AS total_size_kb
FROM document_catalogue
GROUP BY doc_type
ORDER BY doc_count DESC;""")

# =============================================================================
# STEP 7 — MARKETPLACE
# =============================================================================
md("""---
## 7 · Snowflake Marketplace""")

md("""### 7.1 — Install a Free Dataset
The **Snowflake Marketplace** provides live, zero-copy datasets from third-party providers.

1. In Snowsight → **Data Products > Marketplace**
2. Search for **Cybersyn Financial & Economic Essentials**
3. Click **Get** and accept the terms

No data is copied — you query the provider's data in place.""")

sql("""SHOW DATABASES LIKE '%CYBERSYN%';""")

md("""### 7.2 — Enrich Risk Data with Marketplace Data
Join your internal risk events with economic indicators (e.g. Federal Funds Rate). Uncomment the view once the dataset is installed.""")

sql("""/*
CREATE OR REPLACE VIEW analytics.risk_with_economic_context AS
SELECT
    re.event_date,
    re.event_type,
    re.severity,
    re.exposure_usd,
    re.region,
    fed.value AS fed_funds_rate
FROM analytics.risk_events re
LEFT JOIN FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.FINANCIAL_FRED_TIMESERIES fed
    ON  re.event_date = fed.date
    AND fed.variable_name ILIKE '%federal funds effective%'
WHERE re.event_date >= '2024-01-01';
*/

SELECT 'Marketplace integration ready' AS status;""")

# =============================================================================
# STEP 10 — CLEANUP
# =============================================================================
md("""---
## 10 · Cleanup (Optional)
Drop all lab objects when you are finished. This removes the database (and everything inside it), the warehouse, and the custom roles.""")

sql("""USE ROLE accountadmin;

DROP DATABASE  IF EXISTS risk_hol;
DROP WAREHOUSE IF EXISTS risk_wh;
DROP ROLE      IF EXISTS risk_admin;
DROP ROLE      IF EXISTS risk_analyst;
DROP ROLE      IF EXISTS risk_auditor;

SELECT 'Cleanup complete' AS status;""")

# =============================================================================
# WRITE NOTEBOOK
# =============================================================================
notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "SQL", "language": "sql", "name": "sql"},
        "language_info": {"name": "sql"}
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

with open("/Users/nbaxter/Downloads/neil-fs-hol/scripts/risk_hol_workbook.ipynb", "w") as f:
    json.dump(notebook, f, indent=1)

md_count = sum(1 for c in cells if c["cell_type"] == "markdown")
code_count = sum(1 for c in cells if c["cell_type"] == "code")
print(f"Notebook written: {len(cells)} cells ({md_count} markdown, {code_count} SQL)")
