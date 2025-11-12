using DuckDB
using Dates
using Printf
using FilePathsBase

date = ARGS[1]
in_root = abspath(joinpath("data", "external", "ror", date, "parquet"))
out_derived = abspath(joinpath("data", "external", "ror", date, "derived"))
out_audit = abspath(joinpath("data", "external", "ror", date, "audit"))
mkpath(out_derived)
mkpath(out_audit)
threads = try
    parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
catch
    Sys.CPU_THREADS
end
memlim = get(ENV, "DUCKDB_MEM", "8GiB")
tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
mkpath(tmpdir)
decided_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

db = DuckDB.DB()
DuckDB.execute(db, "SET threads=$threads")
DuckDB.execute(db, "SET memory_limit='$memlim'")
DuckDB.execute(db, "SET temp_directory='$tmpdir'")
DuckDB.execute(db, "SET preserve_insertion_order=false")

DuckDB.execute(
    db,
    "CREATE TEMP VIEW organization AS SELECT * FROM read_parquet('$(joinpath(in_root,"organization.parquet"))')",
)
DuckDB.execute(
    db,
    "CREATE TEMP VIEW name AS SELECT * FROM read_parquet('$(joinpath(in_root,"name.parquet"))')",
)
DuckDB.execute(
    db,
    "CREATE TEMP VIEW link AS SELECT * FROM read_parquet('$(joinpath(in_root,"link.parquet"))')",
)
DuckDB.execute(
    db,
    "CREATE TEMP VIEW external_id AS SELECT * FROM read_parquet('$(joinpath(in_root,"external_id.parquet"))')",
)
DuckDB.execute(
    db,
    "CREATE TEMP VIEW location AS SELECT * FROM read_parquet('$(joinpath(in_root,"location.parquet"))')",
)
DuckDB.execute(
    db,
    "CREATE TEMP VIEW relationship AS SELECT * FROM read_parquet('$(joinpath(in_root,"relationship.parquet"))')",
)
DuckDB.execute(
    db,
    "CREATE TEMP VIEW domain AS SELECT * FROM read_parquet('$(joinpath(in_root,"domain.parquet"))')",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE org_type AS
SELECT id::VARCHAR AS id, t::VARCHAR AS type
FROM organization, unnest(coalesce(types,[])) AS u(t)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE name_preferred AS
WITH ranked AS (
  SELECT
    id::VARCHAR AS id,
    value::VARCHAR AS value,
    lang::VARCHAR AS lang,
    type::VARCHAR AS type,
    CASE
      WHEN type='ror_display' THEN 1
      WHEN type='label' THEN 2
      WHEN type='alias' THEN 3
      WHEN type='acronym' THEN 4
      ELSE 9
    END AS prio,
    row_number() OVER (PARTITION BY id ORDER BY
      CASE
        WHEN type='ror_display' THEN 1
        WHEN type='label' THEN 2
        WHEN type='alias' THEN 3
        WHEN type='acronym' THEN 4
        ELSE 9
      END, value
    ) AS rk
  FROM name
)
SELECT id, value AS preferred_name, lang AS preferred_lang, type AS preferred_type
FROM ranked
WHERE rk=1
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE org_core AS
SELECT
  o.id::VARCHAR AS id,
  o.status::VARCHAR AS status,
  o.established AS established,
  o.admin_created_date::VARCHAR AS admin_created_date,
  o.admin_created_schema_version::VARCHAR AS admin_created_schema_version,
  o.admin_last_modified_date::VARCHAR AS admin_last_modified_date,
  o.admin_last_modified_schema_version::VARCHAR AS admin_last_modified_schema_version
FROM organization o
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE org_enriched AS
SELECT
  c.id,
  c.status,
  c.established,
  c.admin_created_date,
  c.admin_created_schema_version,
  c.admin_last_modified_date,
  c.admin_last_modified_schema_version,
  p.preferred_name,
  p.preferred_lang,
  p.preferred_type
FROM org_core c
LEFT JOIN name_preferred p USING(id)
""",
)

DuckDB.execute(
    db,
    "COPY org_type      TO '$(joinpath(out_derived,"org_type.parquet"))'      WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY name_preferred TO '$(joinpath(out_derived,"name_preferred.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY org_core      TO '$(joinpath(out_derived,"org_core.parquet"))'      WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY org_enriched  TO '$(joinpath(out_derived,"org_enriched.parquet"))'  WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE audit AS
WITH
a_org AS (SELECT COUNT(*) AS n FROM organization),
a_name AS (SELECT COUNT(*) AS n FROM name),
a_name_pref AS (SELECT COUNT(*) AS n FROM name_preferred),
a_type AS (SELECT COUNT(*) AS n FROM org_type),
a_rel AS (SELECT COUNT(*) AS n FROM relationship),
a_ext AS (SELECT COUNT(*) AS n FROM external_id),
a_dom AS (SELECT COUNT(*) AS n FROM domain),
a_loc AS (SELECT COUNT(*) AS n FROM location),
a_cov AS (
  SELECT
    COUNT(*) AS org_total,
    SUM(CASE WHEN preferred_name IS NOT NULL AND length(trim(preferred_name))>0 THEN 1 ELSE 0 END) AS org_with_preferred_name
  FROM org_enriched
)
SELECT
  '$decided_at' AS decided_at,
  (SELECT n FROM a_org) AS organization_rows,
  (SELECT n FROM a_name) AS name_rows,
  (SELECT n FROM a_name_pref) AS name_preferred_rows,
  (SELECT n FROM a_type) AS org_type_rows,
  (SELECT n FROM a_rel) AS relationship_rows,
  (SELECT n FROM a_ext) AS external_id_rows,
  (SELECT n FROM a_dom) AS domain_rows,
  (SELECT n FROM a_loc) AS location_rows,
  (SELECT org_total FROM a_cov) AS org_total,
  (SELECT org_with_preferred_name FROM a_cov) AS org_with_preferred_name,
  ROUND(100.0 * (SELECT org_with_preferred_name FROM a_cov) / NULLIF((SELECT org_total FROM a_cov),0), 2) AS preferred_name_coverage_pct
""",
)
DuckDB.execute(
    db,
    "COPY audit TO '$(joinpath(out_audit,"derived.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)

DuckDB.close(db)
println("done")
