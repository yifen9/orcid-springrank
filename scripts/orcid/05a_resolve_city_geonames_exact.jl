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
    std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
    geo_path =
        abspath(joinpath("data", "external", "geonames", geo_date, "cities1000.parquet"))
    in_city_glob = string(joinpath(std_root, "city_text"), "/*.parquet")
    out_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "city"))
    out_match_dir = joinpath(out_root, "match_exact")
    out_unmatch_dir = joinpath(out_root, "unmatched_exact")
    audit_dir = joinpath(out_root, "audit")
    mkpath(out_match_dir);
    mkpath(out_unmatch_dir);
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
        "CREATE TEMP TABLE t_in AS SELECT * FROM read_parquet('$in_city_glob')",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE t_city AS
SELECT
  organization_city_raw,
  organization_city_norm,
  organization_country_iso2,
  pair_cnt,
  '$decided_at' AS decided_at,
  row_number() OVER () AS city_row_id
FROM t_in
""",
    )
    DuckDB.execute(
        db,
        "CREATE INDEX idx_city ON t_city(organization_country_iso2, organization_city_norm)",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_base AS
SELECT
  geoname_id::VARCHAR AS geoname_id,
  ascii_name::VARCHAR AS city_official_name,
  country_code::VARCHAR AS iso2,
  admin1_code::VARCHAR AS admin1_code,
  admin2_code::VARCHAR AS admin2_code,
  timezone::VARCHAR AS timezone
FROM read_parquet('$geo_path')
WHERE country_code IS NOT NULL AND ascii_name IS NOT NULL
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_names AS
SELECT
  geoname_id,
  city_official_name,
  iso2,
  admin1_code,
  admin2_code,
  timezone,
  lower(regexp_replace(trim(city_official_name), '\\s+', ' ')) AS name_norm
FROM geo_base
""",
    )
    DuckDB.execute(db, "CREATE INDEX idx_geo ON geo_names(iso2, name_norm)")
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE t_match AS
SELECT
  c.city_row_id,
  c.organization_city_raw,
  c.organization_city_norm,
  c.organization_country_iso2,
  c.pair_cnt,
  g.geoname_id,
  g.city_official_name,
  g.admin1_code,
  g.admin2_code,
  g.timezone,
  1.0::DOUBLE AS confidence,
  1 AS match_rule
FROM t_city c
JOIN geo_names g
  ON c.organization_country_iso2 = g.iso2
 AND c.organization_city_norm IS NOT NULL
 AND c.organization_city_norm = g.name_norm
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE t_unmatch AS
SELECT c.*
FROM t_city c
LEFT JOIN t_match m USING(city_row_id)
WHERE m.city_row_id IS NULL
""",
    )
    q1 = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM t_match")
    q2 = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM t_unmatch")
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
        "CREATE TEMP TABLE match2 AS SELECT *, ROW_NUMBER() OVER (ORDER BY city_row_id) - 1 AS rn FROM t_match",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE unmatch2 AS SELECT *, ROW_NUMBER() OVER (ORDER BY city_row_id) - 1 AS rn FROM t_unmatch",
    )
    qgm = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt FROM match2 GROUP BY 1 ORDER BY 1",
    )
    qgu = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt FROM unmatch2 GROUP BY 1 ORDER BY 1",
    )
    groups_m = Tuple{Int,Int}[];
    groups_u = Tuple{Int,Int}[]
    for r in qgm
        ;
        push!(groups_m, (Int(r[:chunk_id]), Int(r[:cnt])));
    end
    for r in qgu
        ;
        push!(groups_u, (Int(r[:chunk_id]), Int(r[:cnt])));
    end
    written = 0
    for (chunk_id, cnt) in groups_m
        out_file = joinpath(out_match_dir, @sprintf("%04d.parquet", chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt, geoname_id, city_official_name, admin1_code, admin2_code, timezone, confidence, match_rule, '$decided_at' AS decided_at
  FROM match2
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $chunk_id
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_exact", written, n_match, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    written = 0
    for (chunk_id, cnt) in groups_u
        out_file = joinpath(out_unmatch_dir, @sprintf("%04d.parquet", chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt, '$decided_at' AS decided_at
  FROM unmatch2
  WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $chunk_id
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_unmatched", written, n_unmatch, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE total_w AS SELECT COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM read_parquet('$in_city_glob')",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_cum_ids AS SELECT DISTINCT city_row_id, pair_cnt FROM t_match",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE matched_w AS SELECT COUNT(*) AS matched_distinct, COALESCE(SUM(pair_cnt),0) AS matched_weighted FROM matched_cum_ids",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  1 AS stage,
  (SELECT COUNT(*) FROM t_city) AS total_pairs_distinct,
  (SELECT total_pairs_weighted FROM total_w) AS total_pairs_weighted,
  (SELECT matched_distinct FROM matched_w) AS matched_distinct,
  (SELECT matched_weighted FROM matched_w) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_w) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "match_exact.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
