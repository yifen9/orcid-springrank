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
    out_match_all_dir = joinpath(out_root, "match_idmap_all")
    out_match_best_dir = joinpath(out_root, "match_idmap_best")
    out_unmatch_dir = joinpath(out_root, "unmatched_idmap")
    audit_dir = joinpath(out_root, "audit")
    mkpath(out_match_all_dir);
    mkpath(out_match_best_dir);
    mkpath(out_unmatch_dir);
    mkpath(audit_dir)
    chunk_rows = 16384
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
        ; Sys.CPU_THREADS
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
    if xw_n == 0
        DuckDB.close(db);
        println("no_ror_xwalk");
        return
    end

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
        "CREATE TEMP TABLE s_r AS SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, ror_id_norm AS ror_id FROM signals_norm WHERE ror_id_norm IS NOT NULL",
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
  s.ror_id,
  'ror'::VARCHAR AS match_source,
  s.ror_id::VARCHAR AS source_value_norm,
  0 AS match_priority,
  1.0::DOUBLE AS confidence,
  1 AS match_rule
FROM s_r s
""",
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
        "CREATE TEMP TABLE m_idmap_all AS SELECT * FROM m_r UNION ALL SELECT * FROM m_f UNION ALL SELECT * FROM m_g",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_best AS
SELECT *
FROM (
  SELECT
    *,
    row_number() OVER (PARTITION BY org_row_id ORDER BY match_priority, ror_id) AS rk
  FROM m_idmap_all
) t
WHERE rk = 1
""",
    )

    q_all = DuckDB.execute(
        db,
        "SELECT COUNT(*)::BIGINT AS n, COALESCE(SUM(pair_cnt),0)::BIGINT AS w FROM m_idmap_all",
    )
    n_all = 0;
    w_all = 0
    for r in q_all
        ;
        n_all = Int(r[:n]);
        w_all = Int(r[:w]);
    end
    print("\r", bar("match_candidates", n_all, n_all, t0));
    println();
    flush(stdout)

    q_best = DuckDB.execute(
        db,
        "SELECT COUNT(*)::BIGINT AS n, COALESCE(SUM(pair_cnt),0)::BIGINT AS w FROM m_best",
    )
    n_best = 0;
    w_best = 0
    for r in q_best
        ;
        n_best = Int(r[:n]);
        w_best = Int(r[:w]);
    end
    print("\r", bar("match_best", n_best, n_best, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_ids AS SELECT DISTINCT org_row_id FROM m_best",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE still_unmatched AS
SELECT
  t.org_row_id,
  t.organization_name_raw,
  t.organization_name_norm,
  t.organization_country_iso2,
  t.pair_cnt
FROM org_text_rn t
LEFT JOIN matched_ids m USING(org_row_id)
WHERE m.org_row_id IS NULL
""",
    )
    q2 = DuckDB.execute(
        db,
        "SELECT COUNT(*)::BIGINT AS n, COALESCE(SUM(pair_cnt),0)::BIGINT AS w FROM still_unmatched",
    )
    n_unmatch = 0;
    w_unmatch = 0
    for r in q2
        ;
        n_unmatch = Int(r[:n]);
        w_unmatch = Int(r[:w]);
    end
    print("\r", bar("residual", n_unmatch, n_unmatch, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_all_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id, match_priority, ror_id) - 1 AS rn FROM m_idmap_all",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_best_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM m_best",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE u_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM still_unmatched",
    )

    qga = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_all_rn GROUP BY 1 ORDER BY 1",
    )
    qgb = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_best_rn GROUP BY 1 ORDER BY 1",
    )
    qgu = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM u_rn GROUP BY 1 ORDER BY 1",
    )
    groups_all = Tuple{Int,Int}[];
    groups_best = Tuple{Int,Int}[];
    groups_u = Tuple{Int,Int}[]
    for r in qga
        ;
        push!(groups_all, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qgb
        ;
        push!(groups_best, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qgu
        ;
        push!(groups_u, (Int(r[:gid]), Int(r[:cnt])));
    end
    atotal = sum(last, groups_all; init = 0)
    btotal = sum(last, groups_best; init = 0)
    utotal = sum(last, groups_u; init = 0)

    written = 0
    for (gid, cnt) in groups_all
        out_file = joinpath(out_match_all_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_all_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_candidates", written, atotal, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_best
        out_file = joinpath(out_match_best_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt,
         ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, '$decided_at' AS decided_at
  FROM m_best_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_best", written, btotal, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    uwritten = 0
    for (gid, cnt) in groups_u
        out_file = joinpath(out_unmatch_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, '$decided_at' AS decided_at
  FROM u_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        uwritten += cnt
        print("\r", bar("write_unmatched", uwritten, utotal, t0));
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
        "CREATE TEMP TABLE matched_w AS SELECT COUNT(*) AS matched_distinct, COALESCE(SUM(pair_cnt),0) AS matched_weighted FROM m_best",
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
  (SELECT matched_distinct      FROM matched_w) AS matched_distinct,
  (SELECT matched_weighted      FROM matched_w) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_w) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "org_match_idmap.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.close(db)
    println("done")
end

main()
