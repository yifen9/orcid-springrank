using DuckDB
using Dates
using Printf

function bar(prefix::String, done::Int, total::Int, t0::Float64)
    pct = total == 0 ? 0 : clamp(round(Int, done * 100 / total), 0, 100)
    w = 60
    k = total == 0 ? 0 : clamp(round(Int, w * pct / 100), 0, w)
    b = string(repeat('█', k), repeat(' ', w - k))
    el = time() - t0
    r = el / max(done, 1)
    rm = max(total - done, 0) * r
    s = round(Int, rm)
    h = s ÷ 3600
    m = (s % 3600) ÷ 60
    ss = s % 60
    @sprintf(
        "%s %3d%%|%s| %d/%d  ETA %02d:%02d:%02d (%.2f s/unit)",
        prefix,
        pct,
        b,
        done,
        total,
        h,
        m,
        ss,
        r
    )
end

function main()
    date = ARGS[1]
    in_root = abspath(joinpath("data", "orcid", "curated", date))
    std_root = abspath(joinpath("data", "orcid", "standardized", date))
    map_country_dir = joinpath(std_root, "country")
    out_dir = joinpath(std_root, "city_text")
    audit_dir = joinpath(std_root, "audit")
    mkpath(out_dir);
    mkpath(audit_dir)
    chunk_rows = 16384
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
        ;
        Sys.CPU_THREADS
    end
    memlim = get(ENV, "DUCKDB_MEM", "16GiB")
    tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")));
    mkpath(tmpdir)
    decided_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    t0=time()
    db = DuckDB.DB()
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")
    aff_glob = string(in_root, "/dim_org_raw/*.parquet")
    cmap_glob = string(map_country_dir, "/*.parquet")
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE aff AS
SELECT
  organization_country AS organization_country_raw,
  organization_city    AS organization_city_raw,
  COUNT(*)             AS cnt
FROM read_parquet('$aff_glob')
GROUP BY organization_country_raw, organization_city_raw
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE cmap AS SELECT organization_country_raw, organization_country_iso2 FROM read_parquet('$cmap_glob')",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE aff2 AS
SELECT
  a.organization_country_raw,
  c.organization_country_iso2,
  a.organization_city_raw,
  a.cnt
FROM aff a
LEFT JOIN cmap c USING(organization_country_raw)
""",
    )
    qtot = DuckDB.execute(
        db,
        "SELECT COUNT(*) AS distinct_pairs, COALESCE(SUM(cnt),0) AS total_rows FROM aff2",
    )
    distinct_pairs = 0;
    total_rows = 0
    for r in qtot
        distinct_pairs = Int(r[:distinct_pairs]);
        total_rows = Int(r[:total_rows])
    end
    print("\r", bar("scan", 1, 1, t0));
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE citymap2 AS
WITH base AS (
  SELECT
    organization_city_raw,
    organization_country_iso2,
    cnt,
    regexp_replace(trim(COALESCE(organization_city_raw,'')), '\\s+', ' ') AS trimmed
  FROM aff2
),
normed AS (
  SELECT
    organization_city_raw,
    organization_country_iso2,
    cnt,
    NULLIF(lower(trimmed), '') AS organization_city_norm
  FROM base
)
SELECT
  organization_city_raw,
  organization_city_norm,
  organization_country_iso2,
  cnt AS pair_cnt,
  '$decided_at' AS decided_at,
  ROW_NUMBER() OVER (ORDER BY COALESCE(organization_country_iso2,'ZZ'), COALESCE(organization_city_norm,''), COALESCE(organization_city_raw,'')) - 1 AS rn
FROM normed
""",
    )
    qgc = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt FROM citymap2 GROUP BY 1 ORDER BY 1",
    )
    groups = Tuple{Int,Int}[]
    for r in qgc
        push!(groups, (Int(r[:chunk_id]), Int(r[:cnt])))
    end
    written = 0
    for (chunk_id, cnt) in groups
        out_file = joinpath(out_dir, @sprintf("%04d.parquet", chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT organization_city_raw, organization_city_norm, organization_country_iso2, decided_at, pair_cnt
  FROM citymap2
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $chunk_id
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write", written, distinct_pairs, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE city_audit AS
SELECT
  SUM(pair_cnt) AS total_pairs_weighted,
  COUNT(*) AS distinct_pairs,
  SUM(CASE WHEN organization_city_norm IS NOT NULL THEN pair_cnt ELSE 0 END) AS normalized_weighted,
  ROUND(100.0 * SUM(CASE WHEN organization_city_norm IS NOT NULL THEN pair_cnt ELSE 0 END) / NULLIF(SUM(pair_cnt),0), 2) AS normalized_pct,
  '$decided_at' AS decided_at
FROM citymap2
""",
    )
    DuckDB.execute(
        db,
        "COPY city_audit TO '$(joinpath(audit_dir, "city_text_report.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
