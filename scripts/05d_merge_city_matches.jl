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
CREATE TEMP TABLE m_exact AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
       geoname_id, city_official_name, admin1_code, admin2_code, timezone,
       CAST(confidence AS DOUBLE) AS confidence, match_rule, decided_at
FROM read_parquet('$in_exact')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_alias AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
       geoname_id, city_official_name, admin1_code, admin2_code, timezone,
       CAST(confidence AS DOUBLE) AS confidence, match_rule, decided_at
FROM read_parquet('$in_alias')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m_fuzzy AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt,
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
        "CREATE TEMP TABLE m_all_rn AS SELECT *, row_number() over (order by city_row_id, match_rule, confidence desc, geoname_id) - 1 AS rn FROM m_all",
    )

    qga = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_all_rn GROUP BY 1 ORDER BY 1",
    )
    groups_all = Tuple{Int,Int}[]
    for r in qga
        ;
        push!(groups_all, (Int(r[:gid]), Int(r[:cnt])));
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
  FROM m_all_rn
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
        "CREATE TEMP TABLE best_rn AS SELECT *, row_number() over (order by city_row_id) - 1 AS rn FROM best",
    )
    qgb = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM best_rn GROUP BY 1 ORDER BY 1",
    )
    groups_best = Tuple{Int,Int}[]
    for r in qgb
        ;
        push!(groups_best, (Int(r[:gid]), Int(r[:cnt])));
    end

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
        """
CREATE TEMP TABLE city_base AS
SELECT organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt
FROM read_parquet('$in_city_text')
""",
    )

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_counts AS SELECT COUNT(*) AS total_pairs_distinct, coalesce(SUM(pair_cnt),0) AS total_pairs_weighted FROM city_base",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit_matched AS SELECT COUNT(*) AS matched_distinct, coalesce(SUM(pair_cnt),0) AS matched_weighted FROM best",
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
  (SELECT matched_distinct FROM audit_matched) AS matched_distinct,
  (SELECT matched_weighted FROM audit_matched) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM audit_matched) / NULLIF((SELECT total_pairs_weighted FROM audit_counts),0), 2) AS matched_pct_weighted
""",
    )

    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "city_merge_overall.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )

    DuckDB.close(db)
    println("done")
end

main()
