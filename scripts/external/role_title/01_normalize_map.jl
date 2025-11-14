using DuckDB
using Dates
using FilePathsBase

date = ARGS[1]

in_path = abspath(joinpath("data", "external", "role_title", date, "raw", "map.tsv"))
out_root = abspath(joinpath("data", "external", "role_title", date, "parquet"))
mkpath(out_root)

threads = try
    parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
catch
    Sys.CPU_THREADS
end

memlim = get(ENV, "DUCKDB_MEM", "8GiB")
tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
mkpath(tmpdir)

db = DuckDB.DB()
DuckDB.execute(db, "SET threads=$threads")
DuckDB.execute(db, "SET memory_limit='$memlim'")
DuckDB.execute(db, "SET temp_directory='$tmpdir'")
DuckDB.execute(db, "SET preserve_insertion_order=false")

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE src AS
SELECT *
FROM read_csv(
  '$in_path',
  delim='\t',
  header=false,
  skip=2,
  nullstr='',
  columns={
    role_title_raw:        'VARCHAR',
    edu_level_isced2011:   'VARCHAR',
    career_stage_efrc:     'VARCHAR',
    occupation_isco08:     'VARCHAR',
    field_fos2015:         'VARCHAR',
    is_academic:           'TINYINT'
  }
)
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE norm AS
SELECT
  role_title_raw::VARCHAR                                      AS role_title_raw,
  lower(trim(role_title_raw))::VARCHAR                         AS role_title_norm,
  nullif(trim(edu_level_isced2011),'')::VARCHAR                AS edu_level_isced2011,
  nullif(trim(career_stage_efrc),'')::VARCHAR                  AS career_stage_efrc,
  nullif(trim(occupation_isco08),'')::VARCHAR                  AS occupation_isco08,
  nullif(trim(field_fos2015),'')::VARCHAR                      AS field_fos2015,
  coalesce(is_academic = 1, false)                             AS is_academic,
  row_number() OVER ()                                         AS src_rownum
FROM src
WHERE role_title_raw IS NOT NULL
  AND length(trim(role_title_raw)) > 0
""",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE map_norm AS
SELECT
  role_title_raw,
  role_title_norm,
  edu_level_isced2011,
  career_stage_efrc,
  occupation_isco08,
  field_fos2015,
  is_academic
FROM (
  SELECT
    role_title_raw,
    role_title_norm,
    edu_level_isced2011,
    career_stage_efrc,
    occupation_isco08,
    field_fos2015,
    is_academic,
    row_number() OVER (
      PARTITION BY role_title_norm
      ORDER BY src_rownum
    ) AS rn
  FROM norm
) t
WHERE rn = 1
""",
)

DuckDB.execute(
    db,
    "COPY map_norm TO '$(joinpath(out_root, "map.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)

DuckDB.close(db)
println("done")
