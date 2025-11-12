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
    orcid_date = ARGS[1]
    geo_date = ARGS[2]
    res_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "city"))
    std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
    geo_path =
        abspath(joinpath("data", "external", "geonames", geo_date, "cities1000.parquet"))
    in_unmatched = string(joinpath(res_root, "unmatched_alias"), "/*.parquet")
    out_match_root = joinpath(res_root, "match_fuzzy")
    out_unmatch_root = joinpath(res_root, "unmatched_fuzzy")
    audit_dir = joinpath(res_root, "audit")
    mkpath(out_match_root);
    mkpath(out_unmatch_root);
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
        """
CREATE TEMP TABLE pending AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt
FROM read_parquet('$in_unmatched')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE p_tok AS
SELECT
  city_row_id,
  organization_city_raw,
  organization_city_norm,
  organization_country_iso2,
  pair_cnt,
  list_distinct(list_filter(string_split(lower(regexp_replace(coalesce(organization_city_norm,''),'\\s+',' ')), ' '), x -> length(trim(x))>0)) AS toks,
  coalesce(list_first(list_filter(string_split(lower(regexp_replace(coalesce(organization_city_norm,''),'\\s+',' ')), ' '), x -> length(trim(x))>0)), '') AS first_tok
FROM pending
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_tok AS
SELECT
  g.geoname_id::VARCHAR AS geoname_id,
  g.country_code::VARCHAR AS country_code,
  list_distinct(list_filter(string_split(lower(regexp_replace(coalesce(g.ascii_name,''),'\\s+',' ')), ' '), x -> length(trim(x))>0)) AS toks_all,
  coalesce(list_first(list_filter(string_split(lower(regexp_replace(coalesce(g.ascii_name,''),'\\s+',' ')), ' '), x -> length(trim(x))>0)), '') AS first_tok
FROM read_parquet('$geo_path') g
WHERE g.country_code IS NOT NULL AND g.ascii_name IS NOT NULL
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_alias_first AS
SELECT
  g.geoname_id::VARCHAR AS geoname_id,
  g.country_code::VARCHAR AS country_code,
  coalesce(list_first(list_filter(string_split(lower(regexp_replace(trim(x.alt),'\\s+',' ')), ' '), y -> length(trim(y))>0)), '') AS first_tok
FROM read_parquet('$geo_path') g,
     unnest(string_split(coalesce(g.alternate_names,''), ',')) AS x(alt)
WHERE g.country_code IS NOT NULL AND x.alt IS NOT NULL AND length(trim(x.alt))>0
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_first AS
SELECT geoname_id, country_code, first_tok FROM geo_tok
UNION ALL
SELECT geoname_id, country_code, first_tok FROM geo_alias_first
""",
    )
    DuckDB.execute(db, "CREATE INDEX idx_p ON p_tok(organization_country_iso2, first_tok)")
    DuckDB.execute(db, "CREATE INDEX idx_gf ON geo_first(country_code, first_tok)")
    DuckDB.execute(db, "CREATE INDEX idx_gt ON geo_tok(geoname_id)")
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE cand AS
SELECT
  p.city_row_id,
  p.organization_city_raw,
  p.organization_city_norm,
  p.organization_country_iso2,
  p.pair_cnt,
  p.toks AS p_toks,
  geo_tok.geoname_id,
  geo_tok.toks_all AS g_toks
FROM p_tok p
JOIN geo_first gf
  ON gf.country_code = p.organization_country_iso2
 AND gf.first_tok = p.first_tok
JOIN geo_tok
  ON geo_tok.geoname_id = gf.geoname_id
WHERE length(p.first_tok)>0
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE scored AS
SELECT
  city_row_id,
  organization_city_raw,
  organization_city_norm,
  organization_country_iso2,
  pair_cnt,
  geoname_id,
  coalesce(list_sum(list_transform(list_intersect(p_toks, g_toks), x -> 1)), 0) AS inter_sz,
  coalesce(list_sum(list_transform(p_toks, x -> 1)), 0) AS p_len
FROM cand
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best AS
SELECT
  city_row_id,
  geoname_id,
  inter_sz,
  p_len,
  row_number() OVER (PARTITION BY city_row_id ORDER BY inter_sz DESC, geoname_id) AS rk
FROM scored
WHERE inter_sz > 0
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_full AS
SELECT
  p.city_row_id,
  p.organization_city_raw,
  p.organization_city_norm,
  p.organization_country_iso2,
  p.pair_cnt,
  f.geoname_id::VARCHAR AS geoname_id,
  f.ascii_name::VARCHAR AS city_official_name,
  f.admin1_code::VARCHAR AS admin1_code,
  f.admin2_code::VARCHAR AS admin2_code,
  f.timezone::VARCHAR AS timezone,
  CAST(CASE WHEN b.p_len>0 THEN CAST(b.inter_sz AS DOUBLE)/CAST(b.p_len AS DOUBLE) ELSE 0.0 END AS DOUBLE) AS confidence,
  3 AS match_rule,
  '$decided_at' AS decided_at
FROM best b
JOIN p_tok p USING(city_row_id)
JOIN read_parquet('$geo_path') f USING(geoname_id)
WHERE b.rk = 1
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE still_unmatched AS
SELECT p.city_row_id, p.organization_city_raw, p.organization_city_norm, p.organization_country_iso2, p.pair_cnt
FROM p_tok p
LEFT JOIN m_full m USING(city_row_id)
WHERE m.city_row_id IS NULL
""",
    )
    q1 = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM m_full")
    q2 = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM still_unmatched")
    n_match = 0;
    n_unmatch = 0
    for r in q1
        ;
        n_match = Int(r[:n]);
    end
    for r in q2
        ;
        n_unmatch = Int(r[:n]);
    end
    print("\r", bar("scan", 1, 1, t0));
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE m_rn AS SELECT *, row_number() over (order by city_row_id) - 1 AS rn FROM m_full",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE u_rn AS SELECT *, row_number() over (order by city_row_id) - 1 AS rn FROM still_unmatched",
    )
    qgm = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_rn GROUP BY 1 ORDER BY 1",
    )
    qgu = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM u_rn GROUP BY 1 ORDER BY 1",
    )
    groups_m = Tuple{Int,Int}[];
    groups_u = Tuple{Int,Int}[]
    for r in qgm
        ;
        push!(groups_m, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qgu
        ;
        push!(groups_u, (Int(r[:gid]), Int(r[:cnt])));
    end
    written = 0
    for (gid, cnt) in groups_m
        out_file = joinpath(out_match_root, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
         geoname_id, city_official_name, admin1_code, admin2_code, timezone,
         confidence, match_rule, '$decided_at' AS decided_at
  FROM m_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_fuzzy", written, n_match, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    uwritten = 0
    for (gid, cnt) in groups_u
        out_file = joinpath(out_unmatch_root, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
         '$decided_at' AS decided_at
  FROM u_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        uwritten += cnt
        print("\r", bar("write_unmatched", uwritten, n_unmatch, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE total_w AS SELECT COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM read_parquet('$(joinpath(std_root, "city_text", "*.parquet"))')",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE cumul_ids AS
SELECT DISTINCT city_row_id, pair_cnt FROM read_parquet('$(joinpath(res_root, "match_exact", "*.parquet"))')
UNION ALL
SELECT DISTINCT city_row_id, pair_cnt FROM read_parquet('$(joinpath(res_root, "match_alias", "*.parquet"))')
UNION ALL
SELECT DISTINCT city_row_id, pair_cnt FROM m_full
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE cumul_ids_dist AS SELECT DISTINCT city_row_id, pair_cnt FROM cumul_ids",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_w AS SELECT COUNT(*) AS matched_distinct, COALESCE(SUM(pair_cnt),0) AS matched_weighted FROM cumul_ids_dist",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  3 AS stage,
  (SELECT COUNT(*) FROM read_parquet('$(joinpath(std_root, "city_text", "*.parquet"))')) AS total_pairs_distinct,
  (SELECT total_pairs_weighted FROM total_w) AS total_pairs_weighted,
  (SELECT matched_distinct FROM matched_w) AS matched_distinct,
  (SELECT matched_weighted FROM matched_w) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_w) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "city_match_fuzzy.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
