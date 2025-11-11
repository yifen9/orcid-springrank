using DuckDB
using Printf
using Dates
using FilePathsBase

function bar(prefix, done, total, t0)
    w = 80
    barw = max(w - length(prefix) - 20, 10)
    pct = total == 0 ? 0.0 : 100.0 * done / total
    filled = total == 0 ? 0 : Int(clamp(round(pct/100*barw), 0, barw))
    io = IOBuffer()
    print(
        io,
        prefix,
        " ",
        @sprintf("%3d%%|", Int(clamp(round(pct), 0, 100))),
        repeat("█", filled),
        repeat(" ", barw - filled),
        "|  ",
        @sprintf("%7d/%-7d  ", done, total),
        begin
            if done <= 0
                "ETA --:--:-- (0.00 s/unit)"
            else
                elapsed = time() - t0
                rate = elapsed / done
                remain = max(total - done, 0) * rate
                s = round(Int, remain)
                h = s ÷ 3600
                m = (s % 3600) ÷ 60
                ss = s % 60
                @sprintf("ETA %02d:%02d:%02d (%.2f s/unit)", h, m, ss, rate)
            end
        end,
    )
    String(take!(io))
end

function list_parquet(dir)
    v = String[]
    for (root, _, fs) in walkdir(dir)
        for f in fs
            if endswith(f, ".parquet")
                push!(v, joinpath(root, f))
            end
        end
    end
    sort!(v)
    v
end

function ensure_dir(p)
    mkpath(p)
    p
end

function main()
    orcid_date = ARGS[1]
    geonames_date = ARGS[2]

    in_city_dir =
        abspath(joinpath("data", "orcid", "standardized", orcid_date, "city_text"))
    in_country_dir =
        abspath(joinpath("data", "orcid", "standardized", orcid_date, "country"))
    geonames_file = abspath(
        joinpath("data", "external", "geonames", geonames_date, "cities1000.parquet"),
    )

    out_root = abspath(joinpath("data", "orcid", "resolved", orcid_date))
    out_match_dir = ensure_dir(joinpath(out_root, "city", "matched", "0000"))
    out_unmatch_dir = ensure_dir(joinpath(out_root, "city", "unmatched"))
    out_audit_dir = ensure_dir(joinpath(out_root, "audit"))

    city_files = list_parquet(in_city_dir)
    country_files = list_parquet(in_country_dir)

    db = DuckDB.DB()
    DuckDB.execute(db, "SET preserve_insertion_order=false")
    DuckDB.execute(db, "SET threads=$(Sys.CPU_THREADS)")

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE t_country AS SELECT raw_org_key, organization_country_iso2 FROM read_parquet(['$(join(country_files, ''','''))'])",
    )
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE t_city AS SELECT raw_org_key, organization_city_raw, organization_city_norm, COALESCE(organization_country_iso2, NULL) AS organization_country_iso2 FROM read_parquet(['$(join(city_files, ''','''))'])",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE t_base AS
SELECT
  c.raw_org_key,
  c.organization_city_raw,
  c.organization_city_norm,
  COALESCE(c.organization_country_iso2, k.organization_country_iso2) AS country_iso2
FROM t_city c
LEFT JOIN t_country k USING(raw_org_key)
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_main AS
SELECT
  LOWER(TRIM(ascii_name)) AS name_norm,
  country_code AS iso2,
  geoname_id::VARCHAR AS geoname_id,
  name AS city_official_name,
  admin1_code, admin2_code, timezone
FROM read_parquet('$geonames_file')
WHERE LENGTH(COALESCE(ascii_name,''))>0 AND LENGTH(COALESCE(country_code,''))=2
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE geo_alt AS
WITH src AS (
  SELECT geoname_id::VARCHAR AS geoname_id, country_code AS iso2, city_official_name, admin1_code, admin2_code, timezone,
         UNNEST(string_split(COALESCE(alternate_names,''), ',')) AS alt
  FROM (
    SELECT geoname_id, country_code, name AS city_official_name, admin1_code, admin2_code, timezone, alternate_names
    FROM read_parquet('$geonames_file')
  )
)
SELECT
  LOWER(TRIM(alt)) AS name_norm,
  iso2,
  geoname_id,
  city_official_name,
  admin1_code, admin2_code, timezone
FROM src
WHERE LENGTH(TRIM(alt))>0
""",
    )

    DuckDB.execute(
        db,
        "CREATE TEMP TABLE geo_names AS SELECT * FROM geo_main UNION ALL SELECT * FROM geo_alt",
    )

    DuckDB.execute(
        db,
        "CREATE INDEX idx_base ON t_base(country_iso2, organization_city_norm)",
    )
    DuckDB.execute(db, "CREATE INDEX idx_geo ON geo_names(iso2, name_norm)")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE t_match AS
SELECT
  b.raw_org_key,
  b.organization_city_raw,
  b.organization_city_norm,
  b.country_iso2 AS organization_country_iso2,
  g.geoname_id,
  g.city_official_name,
  g.admin1_code,
  g.admin2_code,
  g.timezone,
  1.0::DOUBLE AS confidence,
  1 AS match_rule
FROM t_base b
JOIN geo_names g
  ON b.country_iso2 = g.iso2
 AND LOWER(TRIM(b.organization_city_norm)) = g.name_norm
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE t_unmatch AS
SELECT b.*
FROM t_base b
LEFT JOIN t_match m USING(raw_org_key)
WHERE m.raw_org_key IS NULL
""",
    )

    tot_match = DuckDB.execute(db, "SELECT COUNT(*) FROM t_match")[1, 1]
    tot_unmatch = DuckDB.execute(db, "SELECT COUNT(*) FROM t_unmatch")[1, 1]

    chunk = 16384
    function copy_chunks(tbl, outdir)
        total = DuckDB.execute(db, "SELECT COUNT(*) FROM $tbl")[1, 1]
        done = 0
        t0 = time()
        i = 0
        while done < total
            path = joinpath(outdir, @sprintf("%04d.parquet", i))
            DuckDB.execute(
                db,
                """
COPY (
  SELECT *
  FROM $tbl
  LIMIT $chunk OFFSET $done
) TO '$path' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
            )
            done = min(done + chunk, total)
            print("\r", bar("write", done, total, t0))
            flush(stdout)
            i += 1
        end
        println()
    end

    copy_chunks("t_match", out_match_dir)
    ensure_dir(out_unmatch_dir)
    copy_chunks("t_unmatch", out_unmatch_dir)

    DuckDB.execute(
        db,
        """
COPY (
  WITH a AS (SELECT COUNT(*) AS cnt FROM t_base),
       b AS (SELECT COUNT(*) AS cnt FROM t_match),
       c AS (SELECT COUNT(*) AS cnt FROM t_unmatch)
  SELECT
    (SELECT cnt FROM a) AS total_rows,
    (SELECT cnt FROM b) AS matched_rows,
    (SELECT cnt FROM c) AS unmatched_rows,
    CAST((SELECT cnt FROM b) AS DOUBLE)/NULLIF((SELECT cnt FROM a),0) AS match_rate
) TO '$(joinpath(out_audit_dir, "resolve_city_report.parquet"))'
WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)
""",
    )

    DuckDB.close(db)
end

main()
