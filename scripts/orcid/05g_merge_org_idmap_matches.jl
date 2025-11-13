using DuckDB
using Dates
using Printf
using FilePathsBase

function bar(p::String, d::Int, t::Int, t0::Float64)
    pct = t == 0 ? 0 : clamp(round(Int, d*100/t), 0, 100)
    w = 60
    k = t == 0 ? 0 : clamp(round(Int, w*pct/100), 0, w)
    b = string(repeat('█', k), repeat(' ', w-k))
    el = time()-t0
    r = el/max(d, 1)
    rm = max(t-d, 0)*r
    s = round(Int, rm)
    h = s÷3600;
    m = (s%3600)÷60;
    ss = s%60
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
        r
    )
end

function has_parquet(dir::String)
    isdir(dir) || return false
    for f in readdir(dir)
        endswith(f, ".parquet") && return true
    end
    return false
end

function main()
    orcid_date = ARGS[1]

    std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
    res_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "org"))

    in_org_text_glob = string(joinpath(std_root, "org_text"), "/*.parquet")

    src_root = joinpath(res_root, "sources")
    fundref_best_dir = joinpath(src_root, "fundref", "best")
    grid_best_dir = joinpath(src_root, "grid", "best")
    ror_best_dir = joinpath(src_root, "ror", "best")
    ring_best_dir = joinpath(src_root, "ringgold_isni", "best")

    fundref_best_glob = string(fundref_best_dir, "/*.parquet")
    grid_best_glob = string(grid_best_dir, "/*.parquet")
    ror_best_glob = string(ror_best_dir, "/*.parquet")
    ring_best_glob = string(ring_best_dir, "/*.parquet")

    out_merged_root = joinpath(res_root, "merged")
    out_best = joinpath(out_merged_root, "best_stage3")
    out_unmatched = joinpath(out_merged_root, "unmatched_stage3")
    audit_dir = joinpath(res_root, "audit")

    mkpath(out_best);
    mkpath(out_unmatched);
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
    t0 = time()

    db = DuckDB.DB()
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE org_text AS SELECT organization_country_iso2, organization_name_raw, organization_name_norm, pair_cnt FROM read_parquet('$in_org_text_glob')",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE org_text_rn AS SELECT *, row_number() OVER (ORDER BY organization_country_iso2, organization_name_norm, organization_name_raw) - 1 AS org_row_id FROM org_text",
    )
    q_tot = DuckDB.execute(
        db,
        "SELECT COUNT(*)::BIGINT AS n, COALESCE(SUM(pair_cnt),0)::BIGINT AS w FROM org_text_rn",
    )
    tot_pairs = 0;
    tot_w = 0
    for r in q_tot
        tot_pairs = Int(r[:n]);
        tot_w = Int(r[:w]);
    end
    print("\r", bar("materialize_org_text", tot_pairs, tot_pairs, t0));
    println();
    flush(stdout)
    if tot_pairs == 0
        DuckDB.close(db);
        println("no_org_text");
        return
    end

    has_fundref = has_parquet(fundref_best_dir)
    has_grid = has_parquet(grid_best_dir)
    has_ror = has_parquet(ror_best_dir)
    has_ring = has_parquet(ring_best_dir)

    selects = String[]

    if has_fundref
        DuckDB.execute(
            db,
            """
CREATE TEMP TABLE m_f_best AS
SELECT
  org_row_id::BIGINT                         AS org_row_id,
  organization_name_raw::VARCHAR             AS organization_name_raw,
  organization_name_norm::VARCHAR            AS organization_name_norm,
  organization_country_iso2::VARCHAR         AS organization_country_iso2,
  pair_cnt::BIGINT                           AS pair_cnt,
  ror_id::VARCHAR                            AS ror_id,
  match_source::VARCHAR                      AS match_source,
  source_value_norm::VARCHAR                 AS source_value_norm,
  match_priority::INTEGER                    AS match_priority,
  CAST(confidence AS DOUBLE)                 AS confidence,
  match_rule::INTEGER                        AS match_rule,
  decided_at::VARCHAR                        AS decided_at
FROM read_parquet('$fundref_best_glob')
""",
        )
        push!(selects, "SELECT * FROM m_f_best")
    end

    if has_grid
        DuckDB.execute(
            db,
            """
CREATE TEMP TABLE m_g_best AS
SELECT
  org_row_id::BIGINT                         AS org_row_id,
  organization_name_raw::VARCHAR             AS organization_name_raw,
  organization_name_norm::VARCHAR            AS organization_name_norm,
  organization_country_iso2::VARCHAR         AS organization_country_iso2,
  pair_cnt::BIGINT                           AS pair_cnt,
  ror_id::VARCHAR                            AS ror_id,
  match_source::VARCHAR                      AS match_source,
  source_value_norm::VARCHAR                 AS source_value_norm,
  match_priority::INTEGER                    AS match_priority,
  CAST(confidence AS DOUBLE)                 AS confidence,
  match_rule::INTEGER                        AS match_rule,
  decided_at::VARCHAR                        AS decided_at
FROM read_parquet('$grid_best_glob')
""",
        )
        push!(selects, "SELECT * FROM m_g_best")
    end

    if has_ror
        DuckDB.execute(
            db,
            """
CREATE TEMP TABLE m_r_best AS
SELECT
  org_row_id::BIGINT                         AS org_row_id,
  organization_name_raw::VARCHAR             AS organization_name_raw,
  organization_name_norm::VARCHAR            AS organization_name_norm,
  organization_country_iso2::VARCHAR         AS organization_country_iso2,
  pair_cnt::BIGINT                           AS pair_cnt,
  ror_id::VARCHAR                            AS ror_id,
  match_source::VARCHAR                      AS match_source,
  source_value_norm::VARCHAR                 AS source_value_norm,
  match_priority::INTEGER                    AS match_priority,
  CAST(confidence AS DOUBLE)                 AS confidence,
  match_rule::INTEGER                        AS match_rule,
  decided_at::VARCHAR                        AS decided_at
FROM read_parquet('$ror_best_glob')
""",
        )
        push!(selects, "SELECT * FROM m_r_best")
    end

    if has_ring
        DuckDB.execute(
            db,
            """
CREATE TEMP TABLE m_ring_best AS
SELECT
  org_row_id::BIGINT                         AS org_row_id,
  organization_name_raw::VARCHAR             AS organization_name_raw,
  organization_name_norm::VARCHAR            AS organization_name_norm,
  organization_country_iso2::VARCHAR         AS organization_country_iso2,
  pair_cnt::BIGINT                           AS pair_cnt,
  ror_id::VARCHAR                            AS ror_id,
  match_source::VARCHAR                      AS match_source,
  source_value_norm::VARCHAR                 AS source_value_norm,
  match_priority::INTEGER                    AS match_priority,
  CAST(confidence AS DOUBLE)                 AS confidence,
  match_rule::INTEGER                        AS match_rule,
  decided_at::VARCHAR                        AS decided_at
FROM read_parquet('$ring_best_glob')
""",
        )
        push!(selects, "SELECT * FROM m_ring_best")
    end

    if isempty(selects)
        DuckDB.close(db);
        println("no_source_matches");
        return
    end

    union_sql = join(selects, " UNION ALL ")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE all_best_src AS
$union_sql
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE all_best AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  match_source,
  source_value_norm,
  match_priority,
  confidence,
  match_rule,
  decided_at,
  CASE
    WHEN match_source = 'ror'           THEN 0
    WHEN match_source = 'fundref'       THEN 1
    WHEN match_source = 'grid'          THEN 2
    WHEN match_source = 'ringgold_isni' THEN 3
    ELSE 9
  END AS source_priority
FROM all_best_src
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best_stage12 AS
SELECT *
FROM (
  SELECT
    a.*,
    row_number() OVER (
      PARTITION BY org_row_id
      ORDER BY source_priority, match_priority, confidence DESC, ror_id
    ) AS rk
  FROM all_best a
) t
WHERE rk = 1
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE unmatched_stage12 AS
SELECT
  o.org_row_id,
  o.organization_name_raw,
  o.organization_name_norm,
  o.organization_country_iso2,
  o.pair_cnt,
  '$decided_at' AS decided_at
FROM org_text_rn o
LEFT JOIN best_stage12 b USING(org_row_id)
WHERE b.org_row_id IS NULL
""",
    )

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE best_stage12_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM best_stage12",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE unmatched_stage12_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM unmatched_stage12",
    )

    qg_best = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM best_stage12_rn GROUP BY 1 ORDER BY 1",
    )
    qg_unm = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM unmatched_stage12_rn GROUP BY 1 ORDER BY 1",
    )

    groups_best = Tuple{Int,Int}[]
    groups_unm = Tuple{Int,Int}[]
    for r in qg_best
        push!(groups_best, (Int(r[:gid]), Int(r[:cnt])))
    end
    for r in qg_unm
        push!(groups_unm, (Int(r[:gid]), Int(r[:cnt])))
    end

    best_total = sum(last, groups_best; init = 0)
    unm_total = sum(last, groups_unm; init = 0)

    written = 0
    for (gid, cnt) in groups_best
        out_file = joinpath(out_best, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, decided_at
  FROM best_stage12_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_best_stage12", written, best_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_unm
        out_file = joinpath(out_unmatched, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, decided_at
  FROM unmatched_stage12_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_unmatched_stage12", written, unm_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_total AS SELECT COUNT(*) AS total_pairs_distinct, COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM org_text_rn",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_matched AS SELECT COUNT(*) AS matched_distinct, COALESCE(SUM(pair_cnt),0) AS matched_weighted FROM best_stage12",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  3 AS stage,
  (SELECT total_pairs_distinct FROM audit_total) AS total_pairs_distinct,
  (SELECT total_pairs_weighted  FROM audit_total) AS total_pairs_weighted,
  (SELECT matched_distinct      FROM audit_matched) AS matched_distinct,
  (SELECT matched_weighted      FROM audit_matched) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM audit_matched) / NULLIF((SELECT total_pairs_weighted FROM audit_total),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "stage3_summary.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit_src AS
SELECT
  match_source,
  COUNT(*) AS matched_distinct,
  COALESCE(SUM(pair_cnt),0) AS matched_weighted
FROM best_stage12
GROUP BY match_source
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit_src2 AS
SELECT
  match_source,
  matched_distinct,
  matched_weighted,
  ROUND(100.0 * matched_weighted / NULLIF((SELECT total_pairs_weighted FROM audit_total),0), 2) AS matched_pct_weighted_source,
  '$decided_at' AS decided_at
FROM audit_src
""",
    )
    DuckDB.execute(
        db,
        "COPY audit_src2 TO '$(joinpath(audit_dir, "stage3_by_source.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.close(db)
    println("done")
end

main()
