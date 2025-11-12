using DuckDB
using Dates
using Printf
using FilePathsBase

date = ARGS[1]
in_path = abspath(joinpath("data", "external", "ror", date, "raw", "ror.json"))
out_root = abspath(joinpath("data", "external", "ror", date, "parquet"))
mkpath(out_root)
threads = try
    parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
catch
    Sys.CPU_THREADS
end
memlim = get(ENV, "DUCKDB_MEM", "8GiB")
tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
mkpath(tmpdir)
ingested_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

db = DuckDB.DB()
DuckDB.execute(db, "SET threads=$threads")
DuckDB.execute(db, "SET memory_limit='$memlim'")
DuckDB.execute(db, "SET temp_directory='$tmpdir'")
DuckDB.execute(db, "SET preserve_insertion_order=false")

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE raw AS
SELECT * FROM read_json('$in_path')
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE organization AS
SELECT
  id::VARCHAR AS id,
  coalesce(status,'')::VARCHAR AS status,
  coalesce(types,[]) AS types,
  established,
  coalesce(CAST(admin.created.date AS VARCHAR),'')::VARCHAR AS admin_created_date,
  coalesce(CAST(admin.created.schema_version AS VARCHAR),'')::VARCHAR AS admin_created_schema_version,
  coalesce(CAST(admin.last_modified.date AS VARCHAR),'')::VARCHAR AS admin_last_modified_date,
  coalesce(CAST(admin.last_modified.schema_version AS VARCHAR),'')::VARCHAR AS admin_last_modified_schema_version,
  '$ingested_at' AS ingested_at
FROM raw
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE name AS
SELECT
  id::VARCHAR AS id,
  coalesce((name).value,'')::VARCHAR AS value,
  coalesce((name).lang,'')::VARCHAR AS lang,
  coalesce(type,'')::VARCHAR AS type,
  '$ingested_at' AS ingested_at
FROM raw,
     unnest(coalesce(names,[])) AS n(name),
     unnest(coalesce((name).types,[])) AS t(type)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE link AS
SELECT
  id::VARCHAR AS id,
  coalesce((link).type,'')::VARCHAR AS type,
  coalesce((link).value,'')::VARCHAR AS value,
  '$ingested_at' AS ingested_at
FROM raw,
     unnest(coalesce(links,[])) AS l(link)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE external_id AS
SELECT
  id::VARCHAR AS id,
  coalesce((ext).type,'')::VARCHAR AS type,
  coalesce(value,'')::VARCHAR AS value,
  coalesce((ext).preferred,'')::VARCHAR AS preferred,
  '$ingested_at' AS ingested_at
FROM raw,
     unnest(coalesce(external_ids,[])) AS e(ext),
     unnest(coalesce((ext).all,[])) AS a(value)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE location AS
SELECT
  id::VARCHAR AS id,
  CAST((loc).geonames_id AS VARCHAR) AS geonames_id,
  coalesce((loc).geonames_details.country_code,'')::VARCHAR AS country_code,
  coalesce((loc).geonames_details.country_subdivision_code,'')::VARCHAR AS admin1_code,
  TRY_CAST((loc).geonames_details.lat AS DOUBLE) AS lat,
  TRY_CAST((loc).geonames_details.lng AS DOUBLE) AS lng,
  coalesce((loc).geonames_details.name,'')::VARCHAR AS city_name,
  '$ingested_at' AS ingested_at
FROM raw,
     unnest(coalesce(locations,[])) AS l(loc)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE relationship AS
SELECT
  id::VARCHAR AS id,
  coalesce((rel).type,'')::VARCHAR AS type,
  coalesce((rel).label,'')::VARCHAR AS label,
  coalesce((rel).id,'')::VARCHAR AS related_id,
  '$ingested_at' AS ingested_at
FROM raw,
     unnest(coalesce(relationships,[])) AS r(rel)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE domain AS
SELECT
  id::VARCHAR AS id,
  d.value::VARCHAR AS value_raw,
  lower(
    regexp_replace(
      regexp_replace(trim(d.value), '^"+', ''),
      '\"+\$',
      ''
    )
  )::VARCHAR AS value,
  '$ingested_at' AS ingested_at
FROM raw,
     unnest(coalesce(domains,[])) AS d(value)
""",
)

DuckDB.execute(
    db,
    "COPY organization TO '$(joinpath(out_root,"organization.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY name         TO '$(joinpath(out_root,"name.parquet"))'          WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY link         TO '$(joinpath(out_root,"link.parquet"))'          WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY external_id  TO '$(joinpath(out_root,"external_id.parquet"))'   WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY location     TO '$(joinpath(out_root,"location.parquet"))'      WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY relationship TO '$(joinpath(out_root,"relationship.parquet"))'  WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY domain       TO '$(joinpath(out_root,"domain.parquet"))'        WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)

DuckDB.close(db)
println("done")
