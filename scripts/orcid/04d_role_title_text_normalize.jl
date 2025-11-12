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
    out_dir = joinpath(std_root, "role_title_text")
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
    tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
    mkpath(tmpdir)
    decided_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    t0 = time()
    db = DuckDB.DB()
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")
    fact_glob = string(in_root, "/fact_affiliation/*/*.parquet")
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE rt_base AS
SELECT role_title AS role_title_raw, COUNT(*) AS cnt
FROM read_parquet('$fact_glob')
GROUP BY role_title_raw
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE rt_norm AS
SELECT
  role_title_raw,
  NULLIF(lower(regexp_replace(trim(coalesce(role_title_raw,'')),'\\s+',' ')),'') AS role_title_norm,
  cnt AS pair_cnt,
  '$decided_at' AS decided_at,
  row_number() over (order by coalesce(role_title_norm,''), coalesce(role_title_raw,'')) - 1 AS rn
FROM rt_base
""",
    )
    qg = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS gid, COUNT(*) AS cnt FROM rt_norm GROUP BY 1 ORDER BY 1",
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
  SELECT role_title_raw, role_title_norm, decided_at, pair_cnt
  FROM rt_norm
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_role_title_text", written, total, t0));
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
  SUM(CASE WHEN role_title_norm IS NOT NULL THEN pair_cnt ELSE 0 END) AS normalized_weighted,
  ROUND(100.0 * SUM(CASE WHEN role_title_norm IS NOT NULL THEN pair_cnt ELSE 0 END) / NULLIF(SUM(pair_cnt),0), 2) AS normalized_pct,
  '$decided_at' AS decided_at
FROM rt_norm
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "role_title_text_report.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
