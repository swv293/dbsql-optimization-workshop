-- =============================================================================
-- DBSQL Optimization Workshop - Apply PHI/PII Governed Tags
-- =============================================================================
-- Tags follow the existing catalog convention:
--   phi: member_id, demographics, diagnosis, financial, service_date, zip_code
--   pii: ssn, address
-- =============================================================================

-- Members table PHI/PII tags
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN dob SET TAGS ('phi' = 'demographics');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN gender SET TAGS ('phi' = 'demographics');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN county SET TAGS ('pii' = 'address');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN state SET TAGS ('pii' = 'address');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN enrollment_start SET TAGS ('phi' = 'service_date');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN enrollment_end SET TAGS ('phi' = 'service_date');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.members ALTER COLUMN risk_score SET TAGS ('phi' = 'diagnosis');

-- Claims table PHI tags
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claims ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claims ALTER COLUMN service_date SET TAGS ('phi' = 'service_date');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claims ALTER COLUMN primary_dx_code SET TAGS ('phi' = 'diagnosis');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claims ALTER COLUMN paid_amount SET TAGS ('phi' = 'financial');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claims ALTER COLUMN allowed_amount SET TAGS ('phi' = 'financial');

-- Claim Diagnoses PHI tags
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claim_diagnoses ALTER COLUMN icd10_code SET TAGS ('phi' = 'diagnosis');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.claim_diagnoses ALTER COLUMN dx_description SET TAGS ('phi' = 'diagnosis');

-- Providers PII tags
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.providers ALTER COLUMN npi SET TAGS ('pii' = 'ssn');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.providers ALTER COLUMN provider_name SET TAGS ('pii' = 'ssn');

-- Drug Claims PHI tags
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.drug_claims ALTER COLUMN member_id SET TAGS ('phi' = 'member_id');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.drug_claims ALTER COLUMN ndc_code SET TAGS ('phi' = 'diagnosis');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.drug_claims ALTER COLUMN drug_name SET TAGS ('phi' = 'diagnosis');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.drug_claims ALTER COLUMN fill_date SET TAGS ('phi' = 'service_date');
ALTER TABLE serverless_stable_swv01_catalog.dbsql_opt.drug_claims ALTER COLUMN paid_amount SET TAGS ('phi' = 'financial');
