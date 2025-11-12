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
    std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
    res_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "city"))
    in_exact = string(joinpath(res_root, "match_exact"), "/*.parquet")
    in_alias = string(joinpath(res_root, "match_alias"), "/*.parquet")
    in_fuzzy = string(joinpath(res_root, "match_fuzzy"), "/*.parquet")
    in_city_text = string(joinpath(std_root, "city_text"), "/*.parquet")
    out_all = joinpath(res_root, "merged", "all")
    out_best = joinpath(res_root, "merged", "best")
    audit_dir = joinpath(res_root, "audit")
    mkpath(out_all);
    mkpath(out_best);
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
        """
CREATE TEMP TABLE m_exact AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2,
       geoname_id, city_official_name, admin1_code, admin2_code, timezone,
       CAST(confidence AS DOUBLE) AS confidence, match_rule, decided_at
FROM read_parquet('$in_exact')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_alias AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2,
       geoname_id, city_official_name, admin1_code, admin2_code, timezone,
       CAST(confidence AS DOUBLE) AS confidence, match_rule, decided_at
FROM read_parquet('$in_alias')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_fuzzy AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2,
       geoname_id, city_official_name, admin1_code, admin2_code, timezone,
       CAST(confidence AS DOUBLE) AS confidence, match_rule, decided_at
FROM read_parquet('$in_fuzzy')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_all AS
SELECT * FROM m_exact
UNION ALL
SELECT * FROM m_alias
UNION ALL
SELECT * FROM m_fuzzy
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best AS
SELECT *
FROM (
  SELECT m.*,
         row_number() OVER (PARTITION BY city_row_id ORDER BY match_rule, confidence DESC, geoname_id) AS rk
  FROM m_all m
) t
WHERE rk = 1
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE city_base AS
SELECT organization_country_iso2,
       organization_city_norm,
       organization_city_raw,
       SUM(pair_cnt)::BIGINT AS pair_cnt
FROM read_parquet('$in_city_text')
GROUP BY 1,2,3
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE best_w AS
SELECT b.city_row_id,
       b.organization_city_raw,
       b.organization_city_norm,
       b.organization_country_iso2,
       cb.pair_cnt,
       b.geoname_id, b.city_official_name, b.admin1_code, b.admin2_code, b.timezone,
       b.confidence, b.match_rule, '$decided_at' AS decided_at
FROM best b
JOIN city_base cb
  ON cb.organization_country_iso2 = b.organization_country_iso2
 AND cb.organization_city_norm    = b.organization_city_norm
 AND cb.organization_city_raw     = b.organization_city_raw
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE all_w AS
SELECT a.city_row_id,
       a.organization_city_raw,
       a.organization_city_norm,
       a.organization_country_iso2,
       cb.pair_cnt,
       a.geoname_id, a.city_official_name, a.admin1_code, a.admin2_code, a.timezone,
       a.confidence, a.match_rule, a.decided_at
FROM m_all a
JOIN city_base cb
  ON cb.organization_country_iso2 = a.organization_country_iso2
 AND cb.organization_city_norm    = a.organization_city_norm
 AND cb.organization_city_raw     = a.organization_city_raw
""",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE all_rn AS SELECT *, row_number() OVER (ORDER BY city_row_id, match_rule, confidence DESC, geoname_id) - 1 AS rn FROM all_w",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE best_rn AS SELECT *, row_number() OVER (ORDER BY city_row_id) - 1 AS rn FROM best_w",
    )
    qga = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM all_rn GROUP BY 1 ORDER BY 1",
    )
    qgb = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM best_rn GROUP BY 1 ORDER BY 1",
    )
    groups_all = Tuple{Int,Int}[];
    groups_best = Tuple{Int,Int}[]
    for r in qga
        ;
        push!(groups_all, (Int(r[:gid]), Int(r[:cnt])));
    end
    for r in qgb
        ;
        push!(groups_best, (Int(r[:gid]), Int(r[:cnt])));
    end
    written = 0
    for (gid, cnt) in groups_all
        out_file = joinpath(out_all, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
         geoname_id, city_official_name, admin1_code, admin2_code, timezone,
         confidence, match_rule, decided_at
  FROM all_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_all", written, sum(last, groups_all; init = 0), t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    written = 0
    for (gid, cnt) in groups_best
        out_file = joinpath(out_best, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
         geoname_id, city_official_name, admin1_code, admin2_code, timezone,
         confidence, match_rule, '$decided_at' AS decided_at
  FROM best_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_best", written, sum(last, groups_best; init = 0), t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_counts AS SELECT COUNT(*) AS total_pairs_distinct, coalesce(SUM(pair_cnt),0) AS total_pairs_weighted FROM read_parquet('$in_city_text')",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_matched_best AS SELECT COUNT(*) AS matched_distinct, coalesce(SUM(pair_cnt),0) AS matched_weighted FROM best_w",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE audit AS
SELECT
  '$decided_at' AS decided_at,
  4 AS stage,
  (SELECT total_pairs_distinct FROM audit_counts) AS total_pairs_distinct,
  (SELECT total_pairs_weighted FROM audit_counts) AS total_pairs_weighted,
  (SELECT matched_distinct FROM audit_matched_best) AS matched_distinct,
  (SELECT matched_weighted FROM audit_matched_best) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM audit_matched_best) / NULLIF((SELECT total_pairs_weighted FROM audit_counts),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "merge_overall.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
