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
    rm = max(t - d, 0) * r
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
        r
    )
end

function main()
    orcid_date = ARGS[1]
    ror_date = ARGS[2]
    geo_date = ARGS[3]

    std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
    cur_root = abspath(joinpath("data", "orcid", "curated", orcid_date))
    res_org_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "org"))
    res_city_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "city"))
    ror_parquet_root = abspath(joinpath("data", "external", "ror", ror_date, "parquet"))
    ror_derived_root = abspath(joinpath("data", "external", "ror", ror_date, "derived"))
    geo_path =
        abspath(joinpath("data", "external", "geonames", geo_date, "cities1000.parquet"))

    in_org_text_glob = string(joinpath(std_root, "org_text"), "/*.parquet")
    in_country_map_glob = string(joinpath(std_root, "country"), "/*.parquet")
    in_dim_org_raw_glob = string(joinpath(cur_root, "dim_org_raw"), "/*.parquet")
    in_unmatched_glob =
        string(joinpath(res_org_root, "merged", "unmatched_stage4"), "/*.parquet")
    in_city_best_glob = string(joinpath(res_city_root, "merged", "best"), "/*.parquet")
    in_ror_name = abspath(joinpath(ror_derived_root, "name_preferred.parquet"))
    in_ror_loc = abspath(joinpath(ror_parquet_root, "location.parquet"))

    out_root = res_org_root
    out_src = joinpath(out_root, "sources")
    out_ror_geo_fuzzy_cand = joinpath(out_src, "ror_geo_fuzzy", "candidates")
    out_ror_geo_fuzzy_best = joinpath(out_src, "ror_geo_fuzzy", "best")
    out_unmatched5_dir = joinpath(out_root, "merged", "unmatched_stage5")
    audit_dir = joinpath(out_root, "audit")
    mkpath(out_ror_geo_fuzzy_cand)
    mkpath(out_ror_geo_fuzzy_best)
    mkpath(out_unmatched5_dir)
    mkpath(audit_dir)

    chunk_rows = 16384
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
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
    tot_pairs = 0
    tot_w = 0
    for r in q_tot
        tot_pairs = Int(r[:n])
        tot_w = Int(r[:w])
    end
    print("\r", bar("materialize_org_text", tot_pairs, tot_pairs, t0))
    println()
    flush(stdout)
    if tot_pairs == 0
        DuckDB.close(db)
        println("no_org_text")
        return
    end

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE unmatched AS SELECT DISTINCT org_row_id FROM read_parquet('$in_unmatched_glob')",
    )
    q_um = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM unmatched")
    um_n = 0
    for r in q_um
        um_n = Int(r[:n])
    end
    print("\r", bar("materialize_unmatched_stage4", um_n, um_n, t0))
    println()
    flush(stdout)
    if um_n == 0
        DuckDB.close(db)
        println("no_unmatched_stage4")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_base AS
SELECT
  t.org_row_id,
  t.organization_country_iso2,
  t.organization_name_raw,
  t.organization_name_norm,
  t.pair_cnt
FROM org_text_rn t
JOIN unmatched u USING(org_row_id)
""",
    )
    q_ob = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM org_base")
    ob_n = 0
    for r in q_ob
        ob_n = Int(r[:n])
    end
    print("\r", bar("materialize_org_base", ob_n, ob_n, t0))
    println()
    flush(stdout)
    if ob_n == 0
        DuckDB.close(db)
        println("no_org_base")
        return
    end

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE country_map AS SELECT organization_country_raw, organization_country_iso2 FROM read_parquet('$in_country_map_glob')",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE dim_org_city AS
SELECT
  m.organization_country_iso2 AS organization_country_iso2,
  d.organization_name::VARCHAR AS organization_name_raw,
  d.organization_city::VARCHAR AS organization_city_raw
FROM read_parquet('$in_dim_org_raw_glob') d
LEFT JOIN country_map m
  ON m.organization_country_raw = d.organization_country
WHERE d.organization_city IS NOT NULL
  AND length(trim(d.organization_city))>0
  AND m.organization_country_iso2 IS NOT NULL
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE city_best AS
SELECT
  organization_country_iso2,
  organization_city_raw,
  geoname_id::VARCHAR AS city_geoname_id,
  admin1_code::VARCHAR AS city_admin1_code
FROM read_parquet('$in_city_best_glob')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_city_geo AS
SELECT
  d.organization_country_iso2,
  d.organization_name_raw,
  d.organization_city_raw,
  c.city_geoname_id,
  c.city_admin1_code
FROM dim_org_city d
JOIN city_best c
  ON c.organization_country_iso2 = d.organization_country_iso2
 AND c.organization_city_raw     = d.organization_city_raw
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_geo AS
SELECT DISTINCT
  b.org_row_id,
  b.organization_name_raw,
  b.organization_name_norm,
  b.organization_country_iso2,
  b.pair_cnt,
  g.city_geoname_id
FROM org_base b
JOIN org_city_geo g
  ON g.organization_country_iso2 = b.organization_country_iso2
 AND g.organization_name_raw     = b.organization_name_raw
WHERE g.city_geoname_id IS NOT NULL
  AND b.organization_name_norm IS NOT NULL
""",
    )
    q_og = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM org_geo")
    og_n = 0
    for r in q_og
        og_n = Int(r[:n])
    end
    print("\r", bar("materialize_org_geo_base", og_n, og_n, t0))
    println()
    flush(stdout)
    if og_n == 0
        DuckDB.close(db)
        println("no_org_geo_base")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_name AS
SELECT
  id::VARCHAR AS ror_id,
  lower(regexp_replace(coalesce(preferred_name,''),'\\s+',' ')) AS preferred_name_norm
FROM read_parquet('$in_ror_name')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_name_tok AS
SELECT
  ror_id,
  preferred_name_norm,
  list_distinct(
    list_filter(
      string_split(preferred_name_norm, ' '),
      x -> length(trim(x))>0
    )
  ) AS name_toks
FROM ror_name
""",
    )
    q_rn = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM ror_name_tok")
    rn_n = 0
    for r in q_rn
        rn_n = Int(r[:n])
    end
    print("\r", bar("materialize_ror_name", rn_n, rn_n, t0))
    println()
    flush(stdout)
    if rn_n == 0
        DuckDB.close(db)
        println("no_ror_name")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_location AS
SELECT
  id::VARCHAR AS ror_id,
  country_code::VARCHAR AS country_code,
  geonames_id::VARCHAR AS city_geoname_id
FROM read_parquet('$in_ror_loc')
WHERE geonames_id IS NOT NULL
  AND country_code IS NOT NULL
""",
    )
    q_rl = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM ror_location")
    rl_n = 0
    for r in q_rl
        rl_n = Int(r[:n])
    end
    print("\r", bar("materialize_ror_location", rl_n, rl_n, t0))
    println()
    flush(stdout)
    if rl_n == 0
        DuckDB.close(db)
        println("no_ror_location")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE ror_geo AS
SELECT
  l.ror_id,
  l.country_code,
  l.city_geoname_id,
  n.preferred_name_norm,
  n.name_toks
FROM ror_location l
JOIN ror_name_tok n USING(ror_id)
""",
    )
    q_rg = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM ror_geo")
    rg_n = 0
    for r in q_rg
        rg_n = Int(r[:n])
    end
    print("\r", bar("materialize_ror_geo_name", rg_n, rg_n, t0))
    println()
    flush(stdout)
    if rg_n == 0
        DuckDB.close(db)
        println("no_ror_geo")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_geo_filtered AS
SELECT g.*
FROM org_geo g
JOIN (
  SELECT DISTINCT country_code, city_geoname_id
  FROM ror_geo
) rc
  ON rc.country_code    = g.organization_country_iso2
 AND rc.city_geoname_id = g.city_geoname_id
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE org_geo_tok AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  city_geoname_id,
  list_distinct(
    list_filter(
      string_split(
        lower(regexp_replace(coalesce(organization_name_norm,''),'\\s+',' ')),
        ' '
      ),
      x -> length(trim(x))>0
    )
  ) AS name_toks
FROM org_geo_filtered
""",
    )
    q_ogt = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM org_geo_tok")
    ogt_n = 0
    for r in q_ogt
        ogt_n = Int(r[:n])
    end
    print("\r", bar("tokenize_org_names", ogt_n, ogt_n, t0))
    println()
    flush(stdout)
    if ogt_n == 0
        DuckDB.close(db)
        println("no_org_geo_tok")
        return
    end

    DuckDB.execute(
        db,
        "CREATE INDEX idx_org_geo ON org_geo_tok(organization_country_iso2, city_geoname_id)",
    )
    DuckDB.execute(db, "CREATE INDEX idx_ror_geo ON ror_geo(country_code, city_geoname_id)")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE cand AS
SELECT
  o.org_row_id,
  o.organization_name_raw,
  o.organization_name_norm,
  o.organization_country_iso2,
  o.pair_cnt,
  o.city_geoname_id,
  o.name_toks AS org_toks,
  r.ror_id,
  r.name_toks AS ror_toks
FROM org_geo_tok o
JOIN ror_geo r
  ON r.country_code    = o.organization_country_iso2
 AND r.city_geoname_id = o.city_geoname_id
""",
    )
    q_c = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM cand")
    c_n = 0
    for r in q_c
        c_n = Int(r[:n])
    end
    print("\r", bar("build_candidates", c_n, c_n, t0))
    println()
    flush(stdout)
    if c_n == 0
        DuckDB.close(db)
        println("no_candidates")
        return
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE scored AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  coalesce(list_sum(list_transform(list_intersect(org_toks, ror_toks), x -> 1)), 0) AS inter_sz,
  coalesce(list_sum(list_transform(org_toks, x -> 1)), 0) AS org_len
FROM cand
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE scored2 AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  inter_sz,
  org_len,
  CASE
    WHEN org_len>0 THEN CAST(inter_sz AS DOUBLE)/CAST(org_len AS DOUBLE)
    ELSE 0.0
  END AS conf
FROM scored
""",
    )
    q_s = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM scored2")
    s_n = 0
    for r in q_s
        s_n = Int(r[:n])
    end
    print("\r", bar("score_candidates", s_n, s_n, t0))
    println()
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_all AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  inter_sz,
  org_len,
  conf,
  'ror_geo_fuzzy'::VARCHAR AS match_source,
  ''::VARCHAR AS source_value_norm,
  4 AS match_priority,
  2 AS match_rule
FROM scored2
WHERE inter_sz > 0
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_best AS
SELECT
  org_row_id,
  organization_name_raw,
  organization_name_norm,
  organization_country_iso2,
  pair_cnt,
  ror_id,
  inter_sz,
  org_len,
  conf,
  'ror_geo_fuzzy'::VARCHAR AS match_source,
  ''::VARCHAR AS source_value_norm,
  4 AS match_priority,
  2 AS match_rule
FROM (
  SELECT
    *,
    row_number() OVER (PARTITION BY org_row_id ORDER BY conf DESC, inter_sz DESC, ror_id) AS rk
  FROM scored2
  WHERE inter_sz > 0
) t
WHERE rk = 1
""",
    )
    q_ma = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM m_all")
    q_mb = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM m_best")
    ma_n = 0
    mb_n = 0
    for r in q_ma
        ma_n = Int(r[:n])
    end
    for r in q_mb
        mb_n = Int(r[:n])
    end
    print("\r", bar("match_ror_geo_fuzzy", mb_n, mb_n, t0))
    println()
    flush(stdout)
    if mb_n == 0
        DuckDB.close(db)
        println("no_ror_geo_fuzzy_match")
        return
    end

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_all_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id, match_priority, ror_id) - 1 AS rn FROM m_all",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_best_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM m_best",
    )

    qga = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS gid, COUNT(*) AS cnt FROM m_all_rn GROUP BY 1 ORDER BY 1",
    )
    qgb = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS gid, COUNT(*) AS cnt FROM m_best_rn GROUP BY 1 ORDER BY 1",
    )
    groups_all = Tuple{Int,Int}[]
    groups_best = Tuple{Int,Int}[]
    for r in qga
        push!(groups_all, (Int(r[:gid]), Int(r[:cnt])))
    end
    for r in qgb
        push!(groups_best, (Int(r[:gid]), Int(r[:cnt])))
    end
    atotal = sum(last, groups_all; init = 0)
    btotal = sum(last, groups_best; init = 0)

    written = 0
    for (gid, cnt) in groups_all
        out_file = joinpath(out_ror_geo_fuzzy_cand, @sprintf("%04d.parquet", gid))
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
    conf AS confidence,
    match_source,
    source_value_norm,
    match_priority,
    match_rule,
    '$decided_at' AS decided_at
  FROM m_all_rn
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_ror_geo_fuzzy_candidates", written, atotal, t0))
        flush(stdout)
    end
    println()
    flush(stdout)

    written = 0
    for (gid, cnt) in groups_best
        out_file = joinpath(out_ror_geo_fuzzy_best, @sprintf("%04d.parquet", gid))
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
    conf AS confidence,
    match_source,
    source_value_norm,
    match_priority,
    match_rule,
    '$decided_at' AS decided_at
  FROM m_best_rn
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_ror_geo_fuzzy_best", written, btotal, t0))
        flush(stdout)
    end
    println()
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE unmatched5 AS
SELECT
  b.org_row_id,
  b.organization_country_iso2,
  b.organization_name_raw,
  b.organization_name_norm,
  b.pair_cnt
FROM org_base b
LEFT JOIN m_best m USING(org_row_id)
WHERE m.org_row_id IS NULL
""",
    )
    q_um5 = DuckDB.execute(
        db,
        "SELECT COUNT(*)::BIGINT AS n, COALESCE(SUM(pair_cnt),0)::BIGINT AS w FROM unmatched5",
    )
    um5_n = 0
    um5_w = 0
    for r in q_um5
        um5_n = Int(r[:n])
        um5_w = Int(r[:w])
    end

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE unmatched5_rn AS SELECT *, row_number() OVER (ORDER BY org_row_id) - 1 AS rn FROM unmatched5",
    )
    qgu = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS gid, COUNT(*) AS cnt FROM unmatched5_rn GROUP BY 1 ORDER BY 1",
    )
    groups_unmatched = Tuple{Int,Int}[]
    for r in qgu
        push!(groups_unmatched, (Int(r[:gid]), Int(r[:cnt])))
    end
    utotal = sum(last, groups_unmatched; init = 0)

    written = 0
    for (gid, cnt) in groups_unmatched
        out_file = joinpath(out_unmatched5_dir, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT
    organization_country_iso2,
    organization_name_raw,
    organization_name_norm,
    pair_cnt,
    org_row_id,
    '$decided_at' AS decided_at
  FROM unmatched5_rn
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_unmatched_stage5", written, utotal, t0))
        flush(stdout)
    end
    println()
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  5 AS stage,
  $tot_pairs::BIGINT AS total_pairs_distinct,
  $tot_w::BIGINT AS total_pairs_weighted,
  ($tot_pairs::BIGINT - $um5_n::BIGINT) AS matched_distinct,
  ($tot_w::BIGINT - $um5_w::BIGINT) AS matched_weighted,
  ROUND(100.0 * ($tot_w::DOUBLE - $um5_w::DOUBLE) / NULLIF($tot_w::DOUBLE,0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "stage5_summary.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    best_fundref_glob = string(joinpath(out_src, "fundref", "best"), "/*.parquet")
    best_grid_glob = string(joinpath(out_src, "grid", "best"), "/*.parquet")
    best_ror_glob = string(joinpath(out_src, "ror", "best"), "/*.parquet")
    best_ringgold_glob = string(joinpath(out_src, "ringgold_isni", "best"), "/*.parquet")
    best_ror_geo_exact_glob =
        string(joinpath(out_src, "ror_geo_exact", "best"), "/*.parquet")
    best_ror_geo_fuzzy_glob =
        string(joinpath(out_src, "ror_geo_fuzzy", "best"), "/*.parquet")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best_all AS
SELECT match_source, org_row_id, pair_cnt
FROM read_parquet('$best_fundref_glob')
UNION ALL
SELECT match_source, org_row_id, pair_cnt FROM read_parquet('$best_grid_glob')
UNION ALL
SELECT match_source, org_row_id, pair_cnt FROM read_parquet('$best_ror_glob')
UNION ALL
SELECT match_source, org_row_id, pair_cnt FROM read_parquet('$best_ringgold_glob')
UNION ALL
SELECT match_source, org_row_id, pair_cnt FROM read_parquet('$best_ror_geo_exact_glob')
UNION ALL
SELECT match_source, org_row_id, pair_cnt FROM read_parquet('$best_ror_geo_fuzzy_glob')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit_src AS
SELECT
  match_source,
  COUNT(DISTINCT org_row_id) AS matched_distinct,
  COALESCE(SUM(pair_cnt),0) AS matched_weighted
FROM best_all
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
  ROUND(100.0 * matched_weighted / NULLIF($tot_w::DOUBLE,0), 2) AS matched_pct_weighted_source,
  '$decided_at' AS decided_at
FROM audit_src
""",
    )
    DuckDB.execute(
        db,
        "COPY audit_src2 TO '$(joinpath(audit_dir, "stage5_by_source.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.close(db)
    println("done")
end

main()
