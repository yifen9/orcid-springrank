using DuckDB
using Dates
using Printf
using FilePathsBase

function bar(p::String, d::Int, t::Int, t0::Float64)
    pct = t == 0 ? 0 : clamp(round(Int, d*100÷t), 0, 100)
    w = 60
    k = t == 0 ? 0 : clamp(round(Int, w*pct÷100), 0, w)
    b = string(repeat('█', k), repeat(' ', w-k))
    el = time() - t0
    r = el / max(d, 1)
    rm = max(t-d, 0) * r
    s = round(Int, rm)
    h = s ÷ 3600
    m = (s % 3600) ÷ 60
    ss = s % 60
    @sprintf(
        "%s %3d%%|%s| %d/%d  ETA %02d:%02d:%02d (%.2f s/unit)",
        p,
        pct,
        b,
        d,
        t,
        h,
        m,
        ss,
        r,
    )
end

function main()
    orcid_date = ARGS[1]

    res_org_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "org"))
    src_root = joinpath(res_org_root, "sources")

    in_fundref_best_glob = string(joinpath(src_root, "fundref", "best"), "/*.parquet")
    in_grid_best_glob = string(joinpath(src_root, "grid", "best"), "/*.parquet")
    in_ringgold_best_glob =
        string(joinpath(src_root, "ringgold_isni", "best"), "/*.parquet")
    in_ror_best_glob = string(joinpath(src_root, "ror", "best"), "/*.parquet")
    in_ror_geo_exact_best_glob =
        string(joinpath(src_root, "ror_geo_exact", "best"), "/*.parquet")
    in_ror_geo_fuzzy_best_glob =
        string(joinpath(src_root, "ror_geo_fuzzy", "best"), "/*.parquet")

    out_root = res_org_root
    out_merged = joinpath(out_root, "merged")
    out_best_by_source = joinpath(out_merged, "best_by_source")
    mkpath(out_best_by_source)

    chunk_rows = 16384
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
        Sys.CPU_THREADS
    end
    memlim = get(ENV, "DUCKDB_MEM", "16GiB")
    tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
    mkpath(tmpdir)
    t0 = time()

    db = DuckDB.DB()
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best_union AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  CAST(NULL AS BIGINT) AS inter_sz,
  CAST(NULL AS BIGINT) AS org_len,
  confidence,
  match_source,
  source_value_norm,
  match_priority,
  match_rule,
  decided_at
FROM read_parquet('$in_fundref_best_glob')
UNION ALL
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  CAST(NULL AS BIGINT) AS inter_sz,
  CAST(NULL AS BIGINT) AS org_len,
  confidence,
  match_source,
  source_value_norm,
  match_priority,
  match_rule,
  decided_at
FROM read_parquet('$in_grid_best_glob')
UNION ALL
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  CAST(NULL AS BIGINT) AS inter_sz,
  CAST(NULL AS BIGINT) AS org_len,
  confidence,
  match_source,
  source_value_norm,
  match_priority,
  match_rule,
  decided_at
FROM read_parquet('$in_ringgold_best_glob')
UNION ALL
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  CAST(NULL AS BIGINT) AS inter_sz,
  CAST(NULL AS BIGINT) AS org_len,
  confidence,
  match_source,
  source_value_norm,
  match_priority,
  match_rule,
  decided_at
FROM read_parquet('$in_ror_best_glob')
UNION ALL
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  CAST(NULL AS BIGINT) AS inter_sz,
  CAST(NULL AS BIGINT) AS org_len,
  confidence,
  match_source,
  source_value_norm,
  match_priority,
  match_rule,
  decided_at
FROM read_parquet('$in_ror_geo_exact_best_glob')
UNION ALL
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  inter_sz,
  org_len,
  confidence,
  match_source,
  source_value_norm,
  match_priority,
  match_rule,
  decided_at
FROM read_parquet('$in_ror_geo_fuzzy_best_glob')
""",
    )

    q_tot = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM best_union")
    total_n = 0
    for r in q_tot
        total_n = Int(r[:n])
    end
    print("\r", bar("aggregate_best_by_source", total_n, total_n, t0))
    println()
    flush(stdout)
    if total_n == 0
        DuckDB.close(db)
        println("no_best_by_source")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best_union_rn AS
SELECT
  *,
  row_number() OVER (ORDER BY org_row_id, match_priority, match_source, ror_id) - 1 AS rn
FROM best_union
""",
    )

    qg = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS gid, COUNT(*) AS cnt FROM best_union_rn GROUP BY 1 ORDER BY 1",
    )
    groups = Tuple{Int,Int}[]
    for r in qg
        push!(groups, (Int(r[:gid]), Int(r[:cnt])))
    end
    total = sum(last, groups; init = 0)

    written = 0
    for (gid, cnt) in groups
        out_file = joinpath(out_best_by_source, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT
    org_row_id,
    organization_name_raw,
    organization_name_norm,
    organization_country_iso2,
    pair_cnt,
    ror_id,
    inter_sz,
    org_len,
    confidence,
    match_source,
    source_value_norm,
    match_priority,
    match_rule,
    decided_at
  FROM best_union_rn
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_best_by_source", written, total, t0))
        flush(stdout)
    end
    println()
    flush(stdout)

    DuckDB.close(db)
    println("done")
end

main()
