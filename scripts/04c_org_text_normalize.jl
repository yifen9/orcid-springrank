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
    out_dir = joinpath(std_root, "org_text")
    audit_dir = joinpath(std_root, "audit")
    map_country_dir = joinpath(std_root, "country")
    map_city_dir = joinpath(std_root, "city_text")
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
    tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
    mkpath(tmpdir)
    decided_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    t0 = time()
    db = DuckDB.DB()
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")
    aff_glob = string(in_root, "/dim_org_raw/*.parquet")
    cmap_glob = string(map_country_dir, "/*.parquet")
    citymap_glob = string(map_city_dir, "/*.parquet")
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_base AS
SELECT
  organization_country AS organization_country_raw,
  organization_city    AS organization_city_raw,
  organization_name    AS organization_name_raw,
  COUNT(*)             AS cnt
FROM read_parquet('$aff_glob')
GROUP BY organization_country_raw, organization_city_raw, organization_name_raw
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE cmap AS SELECT organization_country_raw, organization_country_iso2 FROM read_parquet('$cmap_glob')",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE citymap AS SELECT organization_country_iso2 AS c_iso2, organization_city_raw AS c_raw, organization_city_norm AS c_norm FROM read_parquet('$citymap_glob')",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_join AS
SELECT
  o.organization_name_raw,
  lower(regexp_replace(trim(coalesce(o.organization_name_raw,'')),'\\s+',' ')) AS organization_name_norm,
  o.organization_country_raw,
  c.organization_country_iso2,
  o.organization_city_raw,
  cm.c_norm AS organization_city_norm,
  o.cnt
FROM org_base o
LEFT JOIN cmap c ON c.organization_country_raw = o.organization_country_raw
LEFT JOIN citymap cm ON cm.c_iso2 = c.organization_country_iso2 AND cm.c_raw = o.organization_city_raw
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_rn AS
SELECT
  organization_name_raw,
  organization_name_norm,
  organization_country_raw,
  organization_country_iso2,
  organization_city_raw,
  organization_city_norm,
  cnt AS pair_cnt,
  '$decided_at' AS decided_at,
  row_number() over (order by coalesce(organization_country_iso2,'ZZ'), coalesce(organization_name_norm,''), coalesce(organization_city_norm,''), coalesce(organization_name_raw,'')) - 1 AS rn
FROM org_join
""",
    )
    qg = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS gid, COUNT(*) AS cnt FROM org_rn GROUP BY 1 ORDER BY 1",
    )
    groups = Tuple{Int,Int}[]
    for r in qg
        push!(groups, (Int(r[:gid]), Int(r[:cnt])))
    end
    total = sum(last, groups; init = 0)
    written = 0
    for (gid, cnt) in groups
        out_file = joinpath(out_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT organization_name_raw, organization_name_norm,
         organization_country_iso2, organization_city_norm,
         decided_at, pair_cnt
  FROM org_rn
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_org_text", written, total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  SUM(pair_cnt) AS total_pairs_weighted,
  COUNT(*) AS distinct_pairs,
  SUM(CASE WHEN organization_name_norm IS NOT NULL THEN pair_cnt ELSE 0 END) AS normalized_weighted,
  ROUND(100.0 * SUM(CASE WHEN organization_name_norm IS NOT NULL THEN pair_cnt ELSE 0 END) / NULLIF(SUM(pair_cnt),0), 2) AS normalized_pct,
  '$decided_at' AS decided_at
FROM org_rn
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "org_text_report.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
