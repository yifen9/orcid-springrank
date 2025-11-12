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
    cur_root = abspath(joinpath("data", "orcid", "curated", orcid_date))
    ror_root = abspath(joinpath("data", "external", "ror", ror_date))
    in_org_text_glob = string(joinpath(std_root, "org_text"), "/*.parquet")
    in_country_map_glob = string(joinpath(std_root, "country"), "/*.parquet")
    in_dim_org_raw_glob = string(joinpath(cur_root, "dim_org_raw"), "/*.parquet")
    in_ror_ext = abspath(joinpath(ror_root, "parquet", "external_id.parquet"))

    out_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "org"))
    out_src = joinpath(out_root, "sources")
    out_fundref_cand = joinpath(out_src, "fundref", "candidates")
    out_fundref_best = joinpath(out_src, "fundref", "best")
    out_grid_cand = joinpath(out_src, "grid", "candidates")
    out_grid_best = joinpath(out_src, "grid", "best")
    out_ror_cand = joinpath(out_src, "ror", "candidates")
    out_ror_best = joinpath(out_src, "ror", "best")
    audit_dir = joinpath(out_root, "audit")
    mkpath(out_fundref_cand);
    mkpath(out_fundref_best)
    mkpath(out_grid_cand);
    mkpath(out_grid_best)
    mkpath(out_ror_cand);
    mkpath(out_ror_best)
    mkpath(audit_dir)

    chunk_rows = 16384
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
        ;
        Sys.CPU_THREADS
    end
    memlim = get(ENV, "DUCKDB_MEM", "16GiB")
    tmpdir = abspath(joinpath("data", "_duckdb_tmp"));
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
        "CREATE TEMP TABLE country_map AS SELECT organization_country_raw, organization_country_iso2 FROM read_parquet('$in_country_map_glob')",
    )
    q_cm = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM country_map")
    cm_n = 0
    for r in q_cm
        ;
        cm_n = Int(r[:n]);
    end
    print("\r", bar("materialize_country_map", cm_n, cm_n, t0));
    println();
    flush(stdout)
    if cm_n == 0
        DuckDB.close(db);
        println("no_country_map");
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE dim_src AS
SELECT
  m.organization_country_iso2 AS organization_country_iso2,
  d.organization_name::VARCHAR AS organization_name_raw,
  lower(coalesce(d.disambiguation_source,''))::VARCHAR AS disambiguation_source_l,
  coalesce(d.disambiguated_organization_identifier,'')::VARCHAR AS disambiguated_organization_identifier
FROM read_parquet('$in_dim_org_raw_glob') d
LEFT JOIN country_map m
  ON m.organization_country_raw = d.organization_country
WHERE lower(coalesce(d.disambiguation_source,'')) IN ('fundref','grid','ror')
""",
    )
    q_sig = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM dim_src")
    sig_n = 0
    for r in q_sig
        ;
        sig_n = Int(r[:n]);
    end
    print("\r", bar("materialize_dim_src", sig_n, sig_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_signals AS
SELECT
  t.org_row_id,
  t.organization_name_raw,
  t.organization_name_norm,
  t.organization_country_iso2,
  t.pair_cnt,
  s.disambiguation_source_l,
  s.disambiguated_organization_identifier
FROM org_text_rn t
LEFT JOIN dim_src s
  ON s.organization_country_iso2 = t.organization_country_iso2
 AND s.organization_name_raw     = t.organization_name_raw
""",
    )
    q_os = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM org_signals")
    os_n = 0
    for r in q_os
        ;
        os_n = Int(r[:n]);
    end
    print("\r", bar("join_signals", os_n, os_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_xwalk AS
SELECT lower(type)::VARCHAR AS type, lower(trim(value))::VARCHAR AS value_norm, id::VARCHAR AS ror_id
FROM read_parquet('$in_ror_ext')
WHERE lower(type) IN ('fundref','grid')
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE x_f AS SELECT value_norm, any_value(ror_id) AS ror_id FROM ror_xwalk WHERE type='fundref' GROUP BY 1",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE x_g AS SELECT value_norm, any_value(ror_id) AS ror_id FROM ror_xwalk WHERE type='grid' GROUP BY 1",
    )
    q_xw = DuckDB.execute(
        db,
        "SELECT (SELECT COUNT(*) FROM x_f) + (SELECT COUNT(*) FROM x_g) AS n",
    )
    xw_n = 0
    for r in q_xw
        ;
        xw_n = Int(r[:n]);
    end
    print("\r", bar("materialize_xwalk", xw_n, xw_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE signals_norm AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  disambiguation_source_l,
  disambiguated_organization_identifier,
  CASE
    WHEN disambiguation_source_l = 'fundref' THEN regexp_extract(lower(disambiguated_organization_identifier), '10\\.13039/([0-9]+)', 1)
    ELSE NULL
  END AS fundref_num,
  CASE
    WHEN disambiguation_source_l = 'grid' THEN lower(disambiguated_organization_identifier)
    ELSE NULL
  END AS grid_id_norm,
  CASE
    WHEN disambiguation_source_l = 'ror' THEN lower(disambiguated_organization_identifier)
    ELSE NULL
  END AS ror_id_norm
FROM org_signals
""",
    )
    q_sn = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM signals_norm")
    sn_n = 0
    for r in q_sn
        ;
        sn_n = Int(r[:n]);
    end
    print("\r", bar("normalize_signals", sn_n, sn_n, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE s_f AS SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, fundref_num AS source_value_norm FROM signals_norm WHERE fundref_num IS NOT NULL",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE s_g AS SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, grid_id_norm AS source_value_norm FROM signals_norm WHERE grid_id_norm IS NOT NULL",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE s_r AS SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, ror_id_norm  AS source_value_norm FROM signals_norm WHERE ror_id_norm  IS NOT NULL",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_f AS
SELECT
  s.org_row_id,
  s.organization_name_raw,
  s.organization_name_norm,
  s.organization_country_iso2,
  s.pair_cnt,
  x.ror_id,
  'fundref'::VARCHAR AS match_source,
  s.source_value_norm::VARCHAR AS source_value_norm,
  1 AS match_priority,
  1.0::DOUBLE AS confidence,
  1 AS match_rule
FROM s_f s
JOIN x_f x ON x.value_norm = s.source_value_norm
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_g AS
SELECT
  s.org_row_id,
  s.organization_name_raw,
  s.organization_name_norm,
  s.organization_country_iso2,
  s.pair_cnt,
  x.ror_id,
  'grid'::VARCHAR AS match_source,
  s.source_value_norm::VARCHAR AS source_value_norm,
  2 AS match_priority,
  1.0::DOUBLE AS confidence,
  1 AS match_rule
FROM s_g s
JOIN x_g x ON x.value_norm = s.source_value_norm
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_r AS
SELECT
  s.org_row_id,
  s.organization_name_raw,
  s.organization_name_norm,
  s.organization_country_iso2,
  s.pair_cnt,
  s.source_value_norm::VARCHAR AS ror_id,
  'ror'::VARCHAR AS match_source,
  s.source_value_norm::VARCHAR AS source_value_norm,
  0 AS match_priority,
  1.0::DOUBLE AS confidence,
  1 AS match_rule
FROM s_r s
""",
    )

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_f_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id, match_priority, ror_id) - 1 AS rn FROM m_f",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_g_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id, match_priority, ror_id) - 1 AS rn FROM m_g",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_r_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id, match_priority, ror_id) - 1 AS rn FROM m_r",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_f_best AS
SELECT *
FROM (
  SELECT *, row_number() OVER (PARTITION BY org_row_id ORDER BY match_priority, ror_id) AS rk
  FROM m_f
) t WHERE rk = 1
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_g_best AS
SELECT *
FROM (
  SELECT *, row_number() OVER (PARTITION BY org_row_id ORDER BY match_priority, ror_id) AS rk
  FROM m_g
) t WHERE rk = 1
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_r_best AS
SELECT *
FROM (
  SELECT *, row_number() OVER (PARTITION BY org_row_id ORDER BY match_priority, ror_id) AS rk
  FROM m_r
) t WHERE rk = 1
""",
    )

    qgf = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_f_rn GROUP BY 1 ORDER BY 1",
    )
    qgg = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_g_rn GROUP BY 1 ORDER BY 1",
    )
    qgr = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_r_rn GROUP BY 1 ORDER BY 1",
    )
    groups_f = Tuple{Int,Int}[];
    groups_g = Tuple{Int,Int}[];
    groups_r = Tuple{Int,Int}[]
    for r in qgf
        ;
        push!(groups_f, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qgg
        ;
        push!(groups_g, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qgr
        ;
        push!(groups_r, (Int(r[:gid]), Int(r[:cnt])));
    end
    ftotal = sum(last, groups_f; init = 0)
    gtotal = sum(last, groups_g; init = 0)
    rtotal = sum(last, groups_r; init = 0)

    written = 0
    for (gid, cnt) in groups_f
        out_file = joinpath(out_fundref_cand, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_f_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_fundref_candidates", written, ftotal, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_g
        out_file = joinpath(out_grid_cand, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_g_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_grid_candidates", written, gtotal, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_r
        out_file = joinpath(out_ror_cand, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_r_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_ror_candidates", written, rtotal, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_f_best_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM m_f_best",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_g_best_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM m_g_best",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_r_best_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM m_r_best",
    )

    qbf = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_f_best_rn GROUP BY 1 ORDER BY 1",
    )
    qbg = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_g_best_rn GROUP BY 1 ORDER BY 1",
    )
    qbr = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_r_best_rn GROUP BY 1 ORDER BY 1",
    )
    groups_f_best = Tuple{Int,Int}[];
    groups_g_best = Tuple{Int,Int}[];
    groups_r_best = Tuple{Int,Int}[]
    for r in qbf
        ;
        push!(groups_f_best, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qbg
        ;
        push!(groups_g_best, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qbr
        ;
        push!(groups_r_best, (Int(r[:gid]), Int(r[:cnt])));
    end
    fbest_total = sum(last, groups_f_best; init = 0)
    gbest_total = sum(last, groups_g_best; init = 0)
    rbest_total = sum(last, groups_r_best; init = 0)

    written = 0
    for (gid, cnt) in groups_f_best
        out_file = joinpath(out_fundref_best, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_f_best_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_fundref_best", written, fbest_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_g_best
        out_file = joinpath(out_grid_best, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_g_best_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_grid_best", written, gbest_total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_r_best
        out_file = joinpath(out_ror_best, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_r_best_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_ror_best", written, rbest_total, t0));
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
        "CREATE TEMP TABLE best_union AS SELECT org_row_id, pair_cnt FROM m_f_best UNION ALL SELECT org_row_id, pair_cnt FROM m_g_best UNION ALL SELECT org_row_id, pair_cnt FROM m_r_best",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE best_union_dist AS SELECT DISTINCT org_row_id FROM best_union",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_union AS SELECT COUNT(*) AS matched_distinct FROM best_union_dist",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_weighted AS SELECT COALESCE(SUM(o.pair_cnt),0) AS matched_weighted FROM best_union_dist d JOIN org_text_rn o USING(org_row_id)",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  1 AS stage,
  (SELECT total_pairs_distinct FROM total_w) AS total_pairs_distinct,
  (SELECT total_pairs_weighted  FROM total_w) AS total_pairs_weighted,
  (SELECT matched_distinct FROM matched_union) AS matched_distinct,
  (SELECT matched_weighted FROM matched_weighted) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_weighted) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "stage1_summary.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_src AS SELECT 'fundref'::VARCHAR AS match_source, COUNT(*) AS matched_distinct, COALESCE(SUM(pair_cnt),0) AS matched_weighted FROM m_f_best
         UNION ALL SELECT 'grid', COUNT(*), COALESCE(SUM(pair_cnt),0) FROM m_g_best
         UNION ALL SELECT 'ror',  COUNT(*), COALESCE(SUM(pair_cnt),0) FROM m_r_best",
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
        "COPY audit_src2 TO '$(joinpath(audit_dir, "stage1_by_source.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.close(db)
    println("done")
end

main()
