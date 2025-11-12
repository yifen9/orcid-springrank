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
    res_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "city"))
    geo_path =
        abspath(joinpath("data", "external", "geonames", geo_date, "cities1000.parquet"))
    unmatched_glob = string(joinpath(res_root, "unmatched_exact"), "/*.parquet")
    out_match = joinpath(res_root, "match_alias")
    out_unmatched = joinpath(res_root, "unmatched_alias")
    audit_dir = joinpath(res_root, "audit")
    mkpath(out_match);
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
        """
CREATE TEMP TABLE geo_names_ext AS
SELECT lower(regexp_replace(coalesce(ascii_name,''),'\\s+',' ')) AS name_norm, country_code, geoname_id
FROM read_parquet('$geo_path')
WHERE ascii_name IS NOT NULL AND country_code IS NOT NULL
UNION
SELECT lower(regexp_replace(trim(x.alt),'\\s+',' ')) AS name_norm, g.country_code, g.geoname_id
FROM read_parquet('$geo_path') g, unnest(string_split(coalesce(g.alternate_names,''), ',')) AS x(alt)
WHERE x.alt IS NOT NULL AND length(trim(x.alt))>0 AND g.country_code IS NOT NULL
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_full AS
SELECT geoname_id::VARCHAR AS geoname_id, ascii_name::VARCHAR AS city_official_name, country_code::VARCHAR AS country_code, admin1_code::VARCHAR AS admin1_code, admin2_code::VARCHAR AS admin2_code, timezone::VARCHAR AS timezone
FROM read_parquet('$geo_path')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE pending AS
SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt
FROM read_parquet('$unmatched_glob')
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m1 AS
SELECT p.city_row_id, p.organization_city_raw, p.organization_city_norm, p.organization_country_iso2, p.pair_cnt, g.geoname_id
FROM pending p
JOIN geo_names_ext g
  ON g.country_code = p.organization_country_iso2
 AND g.name_norm   = p.organization_city_norm
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE m1_full AS
SELECT m.city_row_id, m.organization_city_raw, m.organization_city_norm, m.organization_country_iso2, m.pair_cnt, f.geoname_id, f.city_official_name, f.admin1_code, f.admin2_code, f.timezone, 2 AS match_rule, 1.0 AS confidence, '$decided_at' AS decided_at
FROM m1 m
JOIN geo_full f USING(geoname_id)
""",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE still_unmatched AS
SELECT p.*
FROM pending p
LEFT JOIN m1 USING(city_row_id)
WHERE m1.geoname_id IS NULL
""",
    )
    q1 = DuckDB.execute(db, "SELECT COUNT(*)::BIGINT AS n FROM m1_full")
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
        "CREATE TEMP TABLE mcat_rn AS SELECT *, row_number() over (order by organization_country_iso2, organization_city_norm, city_row_id) - 1 AS rn FROM m1_full",
    )
    qg = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM mcat_rn GROUP BY 1 ORDER BY 1",
    )
    groups = Tuple{Int,Int}[]
    for r in qg
        ;
        push!(groups, (Int(r[:gid]), Int(r[:cnt])));
    end
    written = 0
    for (gid, cnt) in groups
        out_file = joinpath(out_match, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt, geoname_id, city_official_name, admin1_code, admin2_code, timezone, confidence, match_rule, '$decided_at' AS decided_at
  FROM mcat_rn
  WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid
) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
        )
        written += cnt
        print("\r", bar("write_alias", written, n_match, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE um_rn AS SELECT *, row_number() over (order by organization_country_iso2, organization_city_norm, city_row_id) - 1 AS rn FROM still_unmatched",
    )
    qu = DuckDB.execute(
        db,
        "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM um_rn GROUP BY 1 ORDER BY 1",
    )
    ugroups = Tuple{Int,Int}[]
    for r in qu
        ;
        push!(ugroups, (Int(r[:gid]), Int(r[:cnt])));
    end
    utotal = sum(last, ugroups; init = 0);
    uwritten = 0
    for (gid, cnt) in ugroups
        out_file = joinpath(out_unmatched, @sprintf("%04d.parquet", gid))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT city_row_id, organization_city_raw, organization_city_norm, organization_country_iso2, pair_cnt
  FROM um_rn
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
        "CREATE TEMP TABLE total_w AS SELECT COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM read_parquet('$(joinpath(std_root, "city_text", "*.parquet"))')",
    )
    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE cumul_ids AS
SELECT DISTINCT city_row_id, pair_cnt FROM read_parquet('$(joinpath(res_root, "match_exact", "*.parquet"))')
UNION ALL
SELECT DISTINCT city_row_id, pair_cnt FROM m1_full
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
  2 AS stage,
  (SELECT COUNT(*) FROM read_parquet('$(joinpath(std_root, "city_text", "*.parquet"))')) AS total_pairs_distinct,
  (SELECT total_pairs_weighted FROM total_w) AS total_pairs_weighted,
  (SELECT matched_distinct FROM matched_w) AS matched_distinct,
  (SELECT matched_weighted FROM matched_w) AS matched_weighted,
  ROUND(100.0 * (SELECT matched_weighted FROM matched_w) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted
""",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(audit_dir, "match_alias.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
