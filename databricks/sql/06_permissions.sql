-- GRANT statements for ADF service principal and users
GRANT USE CATALOG ON CATALOG dbw_policy_lakehouse_dev_v1 TO `eaae74a0-482e-414b-992b-34a3279f62d0`;
GRANT USE SCHEMA  ON SCHEMA  dbw_policy_lakehouse_dev_v1.default TO `eaae74a0-482e-414b-992b-34a3279f62d0`;

-- Grants inherited to tables under the schema
GRANT SELECT, MODIFY ON SCHEMA dbw_policy_lakehouse_dev_v1.default TO `eaae74a0-482e-414b-992b-34a3279f62d0`;

GRANT READ VOLUME ON SCHEMA dbw_policy_lakehouse_dev_v1.default TO `eaae74a0-482e-414b-992b-34a3279f62d0`;

GRANT WRITE VOLUME ON VOLUME dbw_policy_lakehouse_dev_v1.default.vol_gold_csv_exports TO `eaae74a0-482e-414b-992b-34a3279f62d0`;
