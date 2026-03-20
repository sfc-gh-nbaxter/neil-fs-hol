# Snowflake Financial Services Risk Management - Hands-On Lab

A comprehensive hands-on lab demonstrating Snowflake capabilities for Financial Services Risk Management.

## Modules

1. **Account Setup** - Sign up for Snowflake and configure your environment
2. **Infrastructure** - Create warehouse, database, schemas, and RBAC roles
3. **Ingest & Transform Semi-Structured Data** - JSON ingestion, VARIANT queries, FLATTEN, Dynamic Tables
4. **Security & Governance** - Dynamic Data Masking (PII), Row-Level Security (RLS)
5. **Time Travel & Cloning** - Zero-copy clones, accidental update recovery, UNDROP
6. **Unstructured Data** - Internal stages with Directory Tables, document catalogues
7. **Snowflake Marketplace** - Enrich internal risk data with Cybersyn economic data
8. **Cortex Code AI** - Use Snowflake's AI coding agent for risk analytics
9. **Streamlit Dashboard** - Build an interactive risk dashboard in Snowflake
10. **Cleanup & Git** - Remove lab objects and push to version control

## Repository Structure

```
neil-fs-hol/
  docs/                          # HoL HTML guide
    Snowflake_FinServ_Risk_HOL.html
  scripts/                       # Snowflake SQL workbook
    risk_hol_workbook.ipynb
  streamlit/                     # Streamlit in Snowflake app
    risk_dashboard.py
```

## Prerequisites

- Snowflake Enterprise account (or 30-day free trial)
- Supported browser (Chrome, Firefox, Safari, Edge)

## Duration

~90 minutes

## Quick Start

1. Open `docs/Snowflake_FinServ_Risk_HOL.html` in your browser for the full guided lab
2. Or open `scripts/risk_hol_workbook.ipynb` in Snowsight as a Snowflake Notebook
3. The Streamlit app code is in `streamlit/risk_dashboard.py`
