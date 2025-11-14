using DuckDB
using Dates
using FilePathsBase

orcid_date = ARGS[1]
role_title_date = ARGS[2]

in_role_root = abspath(joinpath("data", "orcid", "standardized", orcid_date, "role_title_text"))
in_role_glob = joinpath(in_role_root, "*.parquet")

in_map_path = abspath(joinpath("data", "external", "role_title", role_title_date, "parquet", "map.parquet"))

out_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "role_title"))
mkpath(out_root)

threads = try
    parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
catch
    Sys.CPU_THREADS
end

memlim = get(ENV, "DUCKDB_MEM", "16GiB")
tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
mkpath(tmpdir)

decided_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

db = DuckDB.DB()
DuckDB.execute(db, "SET threads=$threads")
DuckDB.execute(db, "SET memory_limit='$memlim'")
DuckDB.execute(db, "SET temp_directory='$tmpdir'")
DuckDB.execute(db, "SET preserve_insertion_order=false")

println("loading role_title_text orcid_date=$orcid_date")

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE role_title_text AS
SELECT *
FROM read_parquet('$in_role_glob')
"""
)

println("loading role_title_map role_title_date=$role_title_date")

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE role_title_map AS
SELECT
  role_title_norm::VARCHAR     AS role_title_norm,
  edu_level_isced2011::VARCHAR AS edu_level_isced2011,
  career_stage_efrc::VARCHAR   AS career_stage_efrc,
  occupation_isco08::VARCHAR   AS occupation_isco08,
  field_fos2015::VARCHAR       AS field_fos2015,
  is_academic::BOOLEAN         AS is_academic
FROM read_parquet('$in_map_path')
"""
)

println("joining role_title_text with role_title_map")

DuckDB.execute(
    db,
    """
CREATE TABLE role_title_match AS
SELECT
  t.*,
  m.edu_level_isced2011,
  m.career_stage_efrc,
  m.occupation_isco08,
  m.field_fos2015,
  m.is_academic
FROM role_title_text t
LEFT JOIN role_title_map m
  ON t.role_title_norm = m.role_title_norm
"""
)

println("building audit")

DuckDB.execute(
    db,
    """
CREATE TABLE role_title_match_audit AS
SELECT
  '$decided_at'::VARCHAR  AS decided_at,
  '$orcid_date'::VARCHAR  AS orcid_date,
  '$role_title_date'::VARCHAR AS role_title_date,
  COUNT(*)::UBIGINT       AS total_rows,
  COUNT(DISTINCT role_title_norm)::UBIGINT AS distinct_role_title_norm,
  SUM(
    CASE
      WHEN mflag THEN 1 ELSE 0
    END
  )::UBIGINT              AS matched_rows,
  COUNT(
    DISTINCT CASE
      WHEN mflag THEN role_title_norm
      ELSE NULL
    END
  )::UBIGINT              AS matched_role_title_norm,
  CASE
    WHEN COUNT(*) = 0 THEN NULL
    ELSE 100.0 * SUM(CASE WHEN mflag THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)
  END                     AS matched_pct_rows
FROM (
  SELECT
    role_title_norm,
    CASE
      WHEN edu_level_isced2011 IS NOT NULL
        OR career_stage_efrc IS NOT NULL
        OR occupation_isco08 IS NOT NULL
        OR field_fos2015 IS NOT NULL
        OR is_academic IS NOT NULL
      THEN TRUE ELSE FALSE
    END AS mflag
  FROM role_title_match
) q
"""
)

println("writing outputs")

DuckDB.execute(
    db,
    "COPY role_title_match TO '$(joinpath(out_root, "role_title_match.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)"
)

DuckDB.execute(
    db,
    "COPY role_title_match_audit TO '$(joinpath(out_root, "audit.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)"
)

DuckDB.close(db)

println("role_title_match done orcid_date=$orcid_date role_title_date=$role_title_date")
