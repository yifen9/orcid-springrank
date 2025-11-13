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
    h=s÷3600;
    m=(s%3600)÷60;
    ss=s%60
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

function main()
    orcid_date = ARGS[1]
    ror_date = ARGS[2]

    std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
    res_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "org"))
    ror_root = abspath(joinpath("data", "external", "ror", ror_date))

    in_org_text_glob = string(joinpath(std_root, "org_text"), "/*.parquet")
    in_unmatched3_glob =
        string(joinpath(res_root, "merged", "unmatched_stage3"), "/*.parquet")
    in_best3_glob = string(joinpath(res_root, "merged", "best_stage3"), "/*.parquet")
    in_ror_enriched = abspath(joinpath(ror_root, "derived", "org_enriched.parquet"))
    in_ror_location = abspath(joinpath(ror_root, "parquet", "location.parquet"))

    out_src_root = joinpath(res_root, "sources", "ror_geo_exact")
    out_cand_dir = joinpath(out_src_root, "candidates")
    out_best_dir = joinpath(out_src_root, "best")
    out_best_stage4_dir = joinpath(res_root, "merged", "best_stage4")
    out_unmatched_stage4_dir = joinpath(res_root, "merged", "unmatched_stage4")
    audit_dir = joinpath(res_root, "audit")
    mkpath(out_cand_dir);
    mkpath(out_best_dir)
    mkpath(out_best_stage4_dir)
    mkpath(out_unmatched_stage4_dir)
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
        """
CREATE TEMP TABLE org_text_rn AS
SELECT
  *,
  row_number() OVER (ORDER BY organization_country_iso2, organization_name_norm, organization_name_raw) - 1 AS org_row_id
FROM org_text
""",
    )
    q_tot = DuckDB.execute(
        db,
        "SELECT COUNT(*)::BIGINT AS n, COALESCE(SUM(pair_cnt),0)::BIGINT AS w FROM org_text_rn",
    )
    tot_pairs = 0;
    tot_w = 0
    for r in q_tot
        ;
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

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE unmatched3 AS SELECT DISTINCT org_row_id FROM read_parquet('$in_unmatched3_glob')",
    )
    q_um = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM unmatched3")
    um_n = 0
    for r in q_um
        ;
        um_n = Int(r[:n]);
    end
    print("\r", bar("materialize_unmatched_stage3", um_n, um_n, t0));
    println();
    flush(stdout)
    if um_n == 0
        DuckDB.execute(
            db,
            "COPY read_parquet('$in_best3_glob') TO '$(joinpath(out_best_stage4_dir, "0000.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
        )
        DuckDB.execute(
            db,
            "COPY read_parquet('$in_unmatched3_glob') TO '$(joinpath(out_unmatched_stage4_dir, "0000.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
        )
        DuckDB.execute(
            db,
            "CREATE TEMP TABLE total_w AS SELECT COUNT(*) AS total_pairs_distinct, COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM org_text_rn",
        )
        DuckDB.execute(
            db,
            "CREATE TEMP TABLE best3 AS SELECT * FROM read_parquet('$in_best3_glob')",
        )
        DuckDB.execute(
            db,
            "CREATE TEMP TABLE matched_ids AS SELECT DISTINCT org_row_id FROM best3",
        )
        DuckDB.execute(
            db,
            "CREATE TEMP TABLE matched_dist AS SELECT COUNT(*) AS matched_distinct FROM matched_ids",
        )
        DuckDB.execute(
            db,
            "CREATE TEMP TABLE matched_weighted AS SELECT COALESCE(SUM(o.pair_cnt),0) AS matched_weighted FROM matched_ids d JOIN org_text_rn o USING(org_row_id)",
        )
        DuckDB.execute(
            db,
            """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  4 AS stage,
  (SELECT total_pairs_distinct FROM total_w) AS total_pairs_distinct,
  (SELECT total_pairs_weighted  FROM total_w) AS total_pairs_weighted,
  (SELECT matched_distinct      FROM matched_dist) AS matched_distinct,
  (SELECT matched_weighted      FROM matched_weighted) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_weighted) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
        )
        DuckDB.execute(
            db,
            "COPY audit TO '$(joinpath(audit_dir, "stage4_summary.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
        )
        DuckDB.execute(
            db,
            """
CREATE TEMP TABLE audit_src AS
SELECT
  match_source,
  COUNT(*) AS matched_distinct,
  COALESCE(SUM(pair_cnt),0) AS matched_weighted
FROM best3
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
  ROUND(100.0 * matched_weighted / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted_source,
  '$decided_at' AS decided_at
FROM audit_src
""",
        )
        DuckDB.execute(
            db,
            "COPY audit_src2 TO '$(joinpath(audit_dir, "stage4_by_source.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
        )
        DuckDB.close(db)
        println("no_unmatched_stage3")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_base AS
SELECT o.*
FROM org_text_rn o
JOIN unmatched3 u USING(org_row_id)
""",
    )
    q_ob = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM org_base")
    ob_n = 0
    for r in q_ob
        ;
        ob_n = Int(r[:n]);
    end
    print("\r", bar("materialize_org_base", ob_n, ob_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_name_norm AS
SELECT
  id::VARCHAR AS ror_id,
  lower(regexp_replace(trim(preferred_name),'\\s+',' '))::VARCHAR AS name_norm
FROM read_parquet('$in_ror_enriched')
WHERE preferred_name IS NOT NULL AND length(trim(preferred_name))>0
""",
    )
    q_rn = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM ror_name_norm")
    rn_n = 0
    for r in q_rn
        ;
        rn_n = Int(r[:n]);
    end
    print("\r", bar("materialize_ror_name", rn_n, rn_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_loc_country AS
SELECT DISTINCT
  id::VARCHAR AS ror_id,
  country_code::VARCHAR AS country_code
FROM read_parquet('$in_ror_location')
WHERE country_code IS NOT NULL AND length(trim(country_code))>0
""",
    )
    q_rl = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM ror_loc_country")
    rl_n = 0
    for r in q_rl
        ;
        rl_n = Int(r[:n]);
    end
    print("\r", bar("materialize_ror_location", rl_n, rl_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_geo_name AS
SELECT
  l.ror_id,
  l.country_code,
  n.name_norm
FROM ror_loc_country l
JOIN ror_name_norm n USING(ror_id)
WHERE n.name_norm IS NOT NULL AND length(trim(n.name_norm))>0
""",
    )
    q_rg = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM ror_geo_name")
    rg_n = 0
    for r in q_rg
        ;
        rg_n = Int(r[:n]);
    end
    print("\r", bar("materialize_ror_geo_name", rg_n, rg_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE INDEX idx_org_geo ON org_base(organization_country_iso2, organization_name_norm)",
    )
    DuckDB.execute(db, "CREATE INDEX idx_ror_geo ON ror_geo_name(country_code, name_norm)")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_ror_geo AS
SELECT
  o.org_row_id,
  o.organization_name_raw,
  o.organization_name_norm,
  o.organization_country_iso2,
  o.pair_cnt,
  g.ror_id,
  'ror_geo_exact'::VARCHAR AS match_source,
  o.organization_name_norm::VARCHAR AS source_value_norm,
  4 AS match_priority,
  1.0::DOUBLE AS confidence,
  2 AS match_rule,
  '$decided_at' AS decided_at
FROM org_base o
JOIN ror_geo_name g
  ON g.country_code = o.organization_country_iso2
 AND g.name_norm    = o.organization_name_norm
""",
    )
    q_m = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM m_ror_geo")
    m_n = 0
    for r in q_m
        ;
        m_n = Int(r[:n]);
    end
    print("\r", bar("match_ror_geo_exact", m_n, m_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_ror_geo_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id, match_priority, ror_id) - 1 AS rn FROM m_ror_geo",
    )
    qgr = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_ror_geo_rn GROUP BY 1 ORDER BY 1",
    )
    groups_r = Tuple{Int,Int}[]
    for r in qgr
        ;
        push!(groups_r, (Int(r[:gid]), Int(r[:cnt])));
    end
    rtotal = sum(last, groups_r; init = 0)

    written = 0
    for (gid, cnt) in groups_r
        out_file = joinpath(out_cand_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_ror_geo_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_ror_geo_exact_candidates", written, rtotal, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_ror_geo_best AS
SELECT *
FROM (
  SELECT
    *,
    row_number() OVER (PARTITION BY org_row_id ORDER BY match_priority, ror_id) AS rk
  FROM m_ror_geo
) t
WHERE rk = 1
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_ror_geo_best_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM m_ror_geo_best",
    )
    qbr = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_ror_geo_best_rn GROUP BY 1 ORDER BY 1",
    )
    groups_r_best = Tuple{Int,Int}[]
    for r in qbr
        ;
        push!(groups_r_best, (Int(r[:gid]), Int(r[:cnt])));
    end
    rbest_total = sum(last, groups_r_best; init = 0)

    written = 0
    for (gid, cnt) in groups_r_best
        out_file = joinpath(out_best_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_ror_geo_best_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_ror_geo_exact_best", written, rbest_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE best3 AS SELECT * FROM read_parquet('$in_best3_glob')",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best3_norm AS
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
  decided_at
FROM best3
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best4 AS
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
  decided_at
FROM best3_norm
UNION ALL
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
  decided_at
FROM m_ror_geo_best
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE best4_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM best4",
    )
    qg4 = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM best4_rn GROUP BY 1 ORDER BY 1",
    )
    groups_b4 = Tuple{Int,Int}[]
    for r in qg4
        ;
        push!(groups_b4, (Int(r[:gid]), Int(r[:cnt])));
    end
    b4_total = sum(last, groups_b4; init = 0)

    written = 0
    for (gid, cnt) in groups_b4
        out_file = joinpath(out_best_stage4_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, decided_at
  FROM best4_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_best_stage4", written, b4_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched4_ids AS SELECT DISTINCT org_row_id FROM best4",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE unmatched4 AS
SELECT o.*
FROM org_text_rn o
LEFT JOIN matched4_ids m USING(org_row_id)
WHERE m.org_row_id IS NULL
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE unmatched4_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM unmatched4",
    )
    qgu4 = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM unmatched4_rn GROUP BY 1 ORDER BY 1",
    )
    groups_u4 = Tuple{Int,Int}[]
    for r in qgu4
        ;
        push!(groups_u4, (Int(r[:gid]), Int(r[:cnt])));
    end
    u4_total = sum(last, groups_u4; init = 0)

    written = 0
    for (gid, cnt) in groups_u4
        out_file = joinpath(out_unmatched_stage4_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT organization_country_iso2, organization_name_raw, organization_name_norm, pair_cnt, org_row_id
  FROM unmatched4_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_unmatched_stage4", written, u4_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE total_w AS SELECT COUNT(*) AS total_pairs_distinct, COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM org_text_rn",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_ids4 AS SELECT DISTINCT org_row_id FROM best4",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_dist4 AS SELECT COUNT(*) AS matched_distinct FROM matched_ids4",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_weighted4 AS SELECT COALESCE(SUM(o.pair_cnt),0) AS matched_weighted FROM matched_ids4 d JOIN org_text_rn o USING(org_row_id)",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  4 AS stage,
  (SELECT total_pairs_distinct FROM total_w) AS total_pairs_distinct,
  (SELECT total_pairs_weighted  FROM total_w) AS total_pairs_weighted,
  (SELECT matched_distinct      FROM matched_dist4) AS matched_distinct,
  (SELECT matched_weighted      FROM matched_weighted4) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_weighted4) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "stage4_summary.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit_src AS
SELECT
  match_source,
  COUNT(*) AS matched_distinct,
  COALESCE(SUM(pair_cnt),0) AS matched_weighted
FROM best4
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
  ROUND(100.0 * matched_weighted / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted_source,
  '$decided_at' AS decided_at
FROM audit_src
""",
    )
    DuckDB.execute(
        db,
        "COPY audit_src2 TO '$(joinpath(audit_dir, "stage4_by_source.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.close(db)
    println("done")
end

main()
