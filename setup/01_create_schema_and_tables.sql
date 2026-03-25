-- =============================================================================
-- DBSQL Optimization Workshop - Schema and Table Creation
-- =============================================================================
-- Catalog: serverless_stable_swv01_catalog
-- Schema:  dbsql_opt
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt;

-- Members table (~5M rows)
-- Health plan member enrollment and demographics
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt.members (
  member_id        STRING  COMMENT 'Unique identifier for each health plan member',
  plan_id          STRING  COMMENT 'Health insurance plan identifier the member is enrolled in',
  lob              STRING  COMMENT 'Line of business - Commercial, Medicare, or Medicaid',
  dob              DATE    COMMENT 'Date of birth of the member (PHI)',
  gender           STRING  COMMENT 'Gender of the member (M/F/O)',
  county           STRING  COMMENT 'County of residence for the member (PII)',
  state            STRING  COMMENT 'State of residence used for geographic analysis and partitioning (PII)',
  enrollment_start DATE    COMMENT 'Date when member enrollment in the plan began',
  enrollment_end   DATE    COMMENT 'Date when member enrollment in the plan ended or NULL if active',
  risk_score       DOUBLE  COMMENT 'HCC risk adjustment score for the member - higher indicates more predicted cost'
) USING DELTA
PARTITIONED BY (state)
COMMENT 'Health plan member enrollment and demographics table. Contains ~5M member records with enrollment periods, risk scores, and geographic information. Partitioned by state for efficient geographic queries.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Medical claims (~50M rows)
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt.claims (
  claim_id         STRING  COMMENT 'Unique identifier for each medical claim',
  member_id        STRING  COMMENT 'Foreign key to members table - identifies the member who received services',
  provider_id      STRING  COMMENT 'Foreign key to providers table - identifies the rendering provider',
  service_date     DATE    COMMENT 'Date the medical service was rendered',
  claim_type       STRING  COMMENT 'Type of claim - IP (Inpatient), OP (Outpatient), Prof (Professional)',
  primary_dx_code  STRING  COMMENT 'Primary ICD-10 diagnosis code for the claim',
  procedure_code   STRING  COMMENT 'CPT/HCPCS procedure code for the service rendered',
  allowed_amount   DOUBLE  COMMENT 'Total allowed amount per the provider contract',
  paid_amount      DOUBLE  COMMENT 'Actual amount paid by the health plan after member cost-sharing',
  claim_status     STRING  COMMENT 'Current status of the claim - PAID, DENIED, PENDING'
) USING DELTA
PARTITIONED BY (service_date)
COMMENT 'Medical claims transaction table containing ~50M claim records. Each row represents a single medical claim with financial and clinical detail. Partitioned by service_date for efficient date-range queries.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Claim-level diagnosis detail (~150M rows)
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt.claim_diagnoses (
  claim_id         STRING  COMMENT 'Foreign key to claims table - identifies the parent claim',
  dx_sequence      INT     COMMENT 'Sequence number of the diagnosis on the claim (1=primary, 2+=secondary)',
  icd10_code       STRING  COMMENT 'ICD-10-CM diagnosis code',
  dx_description   STRING  COMMENT 'Human-readable description of the ICD-10 diagnosis code'
) USING DELTA
COMMENT 'Claim-level diagnosis detail table with ~150M rows. Supports multiple diagnoses per claim (multi-dx). Used for chronic condition analysis, risk adjustment, and quality reporting.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Provider master (~800K rows)
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt.providers (
  provider_id      STRING  COMMENT 'Unique internal identifier for each provider',
  npi              STRING  COMMENT 'National Provider Identifier - 10-digit unique ID assigned by CMS (PII)',
  provider_name    STRING  COMMENT 'Full legal name of the provider or practice (PII)',
  specialty        STRING  COMMENT 'Medical specialty of the provider (e.g., Cardiology, Orthopedics)',
  network_status   STRING  COMMENT 'Provider network participation status - In-Network or Out-of-Network',
  county           STRING  COMMENT 'County where the provider practices',
  state            STRING  COMMENT 'State where the provider practices'
) USING DELTA
COMMENT 'Provider master reference table with ~800K provider records. Contains provider demographics, specialty, and network status. Used for network adequacy analysis and provider profiling.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Pharmacy claims (~30M rows)
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt.drug_claims (
  rx_claim_id      STRING  COMMENT 'Unique identifier for each pharmacy claim',
  member_id        STRING  COMMENT 'Foreign key to members table - identifies the member filling the prescription',
  ndc_code         STRING  COMMENT 'National Drug Code - 11-digit identifier for the dispensed medication',
  drug_name        STRING  COMMENT 'Brand or generic name of the dispensed drug',
  drug_class       STRING  COMMENT 'Therapeutic drug class (e.g., Antidiabetics, Statins, ACE Inhibitors)',
  fill_date        DATE    COMMENT 'Date the prescription was filled at the pharmacy',
  days_supply      INT     COMMENT 'Number of days of medication dispensed',
  paid_amount      DOUBLE  COMMENT 'Amount paid by the health plan for this pharmacy claim'
) USING DELTA
PARTITIONED BY (fill_date)
COMMENT 'Pharmacy claims table with ~30M prescription fill records. Contains drug dispensing details, costs, and therapeutic classification. Partitioned by fill_date for efficient date-range analysis.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ICD-10 lookup (~75K rows)
CREATE TABLE IF NOT EXISTS serverless_stable_swv01_catalog.dbsql_opt.icd10_lookup (
  icd10_code       STRING  COMMENT 'ICD-10-CM diagnosis code (e.g., E11.9, I10)',
  description      STRING  COMMENT 'Full text description of the ICD-10 diagnosis code',
  category         STRING  COMMENT 'High-level clinical category grouping (e.g., Diabetes, Hypertension, Respiratory)',
  chronic_flag     BOOLEAN COMMENT 'Indicates if this diagnosis is classified as a chronic condition for quality reporting'
) USING DELTA
COMMENT 'ICD-10-CM diagnosis code reference lookup table with ~75K codes. Used for joining to claim diagnoses for categorization, chronic condition flagging, and quality measure computation.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
