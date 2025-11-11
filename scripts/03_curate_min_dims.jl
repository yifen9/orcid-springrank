using DuckDB
using Dates
using Printf
using FilePathsBase
using Logging

function fmt_bar(prefix::String, done::Int, total::Int, t0::Float64)
    pct = total == 0 ? 0 : clamp(round(Int, done * 100 / total), 0, 100)
    w = 60
    k = total == 0 ? 0 : clamp(round(Int, w * pct / 100), 0, w)
    bar = string(repeat('█', k), repeat(' ', w - k))
    elapsed = time() - t0
    rate = elapsed / max(done, 1)
    remain = max(total - done, 0) * rate
    s = round(Int, remain)
    h = s ÷ 3600;
    m = (s % 3600) ÷ 60;
    ss = s % 60
    @sprintf(
        "%s %3d%%|%s| %d/%d  ETA %02d:%02d:%02d (%.2f s/unit)",
        prefix,
        pct,
        bar,
        done,
        total,
        h,
        m,
        ss,
        rate
    )
end

function scan_parquets(dir::String)
    years = readdir(dir)
    files = String[]
    for y in years
        yd = joinpath(dir, y)
        if isdir(yd)
            for f in readdir(yd)
                if endswith(f, ".parquet")
                    push!(files, joinpath(yd, f))
                end
            end
        end
    end
    sort!(files)
    files
end

function main()
    date = ARGS[1]
    in_root = abspath(joinpath("data", "orcid", "structured", date))
    out_root = abspath(joinpath("data", "orcid", "curated", date))
    mkpath(out_root)
    chunk_rows = 16384
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
        ;
        Sys.CPU_THREADS
    end
    memlim = get(ENV, "DUCKDB_MEM", "16GiB")
    tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
    mkpath(tmpdir)
    source_version = date
    ingested_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")

    @info "start" date=date in_root=in_root out_root=out_root chunk_rows=chunk_rows threads=threads mem=memlim

    files = scan_parquets(in_root)
    @info "found_parquet_parts" count=length(files)

    glob_pat = string(in_root, "/*/*.parquet")
    t0 = time()

    db = DuckDB.DB()
    DuckDB.execute(db, "INSTALL json")
    DuckDB.execute(db, "LOAD json")
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE s AS
SELECT
  orcid::VARCHAR                           AS orcid,
  given_names::VARCHAR                     AS given_names,
  family_name::VARCHAR                     AS family_name,
  affiliation_type::VARCHAR                AS affiliation_type,
  role_title::VARCHAR                      AS role_title,
  department_name::VARCHAR                 AS department_name,
  start_date::VARCHAR                      AS start_date,
  start_year::INTEGER                      AS start_year,
  end_date::VARCHAR                        AS end_date,
  organization_name::VARCHAR               AS organization_name,
  organization_city::VARCHAR               AS organization_city,
  organization_country::VARCHAR            AS organization_country,
  disambiguation_source::VARCHAR           AS disambiguation_source,
  disambiguated_organization_identifier::VARCHAR AS disambiguated_organization_identifier
FROM read_parquet('$glob_pat');
""",
    )

    res_tot = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM s")
    tot = 0
    for r in res_tot
        tot = Int(getproperty(r, :c))
        break
    end
    print("\r", fmt_bar("materialize", tot, tot, t0));
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE fact AS
SELECT
  md5(
    lower(coalesce(orcid,'')) || '|' ||
    lower(coalesce(affiliation_type,'')) || '|' ||
    lower(coalesce(role_title,'')) || '|' ||
    lower(coalesce(department_name,'')) || '|' ||
    lower(coalesce(organization_name,'')) || '|' ||
    lower(coalesce(organization_city,'')) || '|' ||
    lower(coalesce(organization_country,'')) || '|' ||
    lower(coalesce(start_date,'')) || '|' ||
    lower(coalesce(end_date,''))
  ) AS aff_id,
  orcid,
  given_names,
  family_name,
  affiliation_type,
  role_title,
  department_name,
  start_date,
  start_year,
  end_date,
  organization_name,
  organization_city,
  organization_country,
  disambiguation_source,
  disambiguated_organization_identifier,
  CASE WHEN affiliation_type='education' THEN true ELSE false END AS is_edu,
  CASE WHEN affiliation_type='employment' THEN true ELSE false END AS is_emp,
  '$source_version' AS source_version,
  '$ingested_at' AS ingested_at
FROM s;
""",
    )

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE fact2 AS
SELECT
  *,
  COALESCE(LPAD(CAST(start_year AS VARCHAR), 4, '0'), 'null') AS year_dir,
  ROW_NUMBER() OVER (PARTITION BY COALESCE(LPAD(CAST(start_year AS VARCHAR), 4, '0'), 'null')
                     ORDER BY orcid) - 1 AS rn
FROM fact;
""",
    )

    q_counts = DuckDB.execute(
        db,
        """
SELECT
  year_dir,
  CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id,
  COUNT(*) AS cnt
FROM fact2
GROUP BY 1,2
ORDER BY 1,2
""",
    )

    groups = Vector{NamedTuple{(:year_dir, :chunk_id, :cnt),Tuple{String,Int64,Int64}}}()
    for r in q_counts
        push!(
            groups,
            (
                year_dir = String(getproperty(r, :year_dir)),
                chunk_id = Int(getproperty(r, :chunk_id)),
                cnt = Int(getproperty(r, :cnt)),
            ),
        )
    end

    total_groups = length(groups)
    done_rows = 0
    @info "plan_fact_affiliation" groups=total_groups total_rows=tot

    for g in groups
        out_dir = joinpath(out_root, "fact_affiliation", g.year_dir);
        mkpath(out_dir)
        out_file = joinpath(out_dir, @sprintf("%04d.parquet", g.chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT
    aff_id,
    orcid,
    given_names,
    family_name,
    affiliation_type,
    role_title,
    department_name,
    start_date,
    start_year,
    end_date,
    organization_name,
    organization_city,
    organization_country,
    disambiguation_source,
    disambiguated_organization_identifier,
    is_edu,
    is_emp,
    source_version,
    ingested_at
  FROM fact2
  WHERE year_dir = '$(g.year_dir)'
    AND CAST(FLOOR(rn / $(chunk_rows)) AS BIGINT) = $(g.chunk_id)
)
TO '$(out_file)'
WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE);
""",
        )
        done_rows += g.cnt
        print("\r", fmt_bar("write_fact", done_rows, tot, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE dimp AS
SELECT
  orcid AS person_id,
  any_value(given_names) AS given_names,
  any_value(family_name) AS family_name,
  COUNT(*) AS records,
  '$source_version' AS source_version,
  '$ingested_at' AS ingested_at
FROM fact
GROUP BY orcid;
""",
    )

    res_p = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM dimp")
    tot_p = 0
    for r in res_p
        tot_p = Int(getproperty(r, :c))
        break
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE dimp2 AS
SELECT
  *,
  ROW_NUMBER() OVER (ORDER BY person_id) - 1 AS rn
FROM dimp;
""",
    )

    q_pc = DuckDB.execute(
        db,
        """
SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt
FROM dimp2
GROUP BY 1
ORDER BY 1
""",
    )

    groups_p = Vector{NamedTuple{(:chunk_id, :cnt),Tuple{Int64,Int64}}}()
    for r in q_pc
        push!(
            groups_p,
            (chunk_id = Int(getproperty(r, :chunk_id)), cnt = Int(getproperty(r, :cnt))),
        )
    end

    done_p = 0
    out_dir_p = joinpath(out_root, "dim_person");
    mkpath(out_dir_p)
    for g in groups_p
        out_file = joinpath(out_dir_p, @sprintf("%04d.parquet", g.chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT person_id, given_names, family_name, records, source_version, ingested_at
  FROM dimp2
  WHERE CAST(FLOOR(rn / $(chunk_rows)) AS BIGINT) = $(g.chunk_id)
)
TO '$(out_file)'
WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE);
""",
        )
        done_p += g.cnt
        print("\r", fmt_bar("write_person", done_p, tot_p, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE dimo AS
SELECT
  CASE
    WHEN disambiguation_source IS NOT NULL AND disambiguated_organization_identifier IS NOT NULL
    THEN lower(disambiguation_source) || ':' || lower(disambiguated_organization_identifier)
    ELSE md5(
      lower(coalesce(organization_name,'')) || '|' ||
      lower(coalesce(organization_country,'')) || '|' ||
      lower(coalesce(organization_city,''))
    )
  END AS raw_org_key,
  organization_name,
  organization_city,
  organization_country,
  disambiguation_source,
  disambiguated_organization_identifier,
  '$source_version' AS source_version,
  '$ingested_at' AS ingested_at
FROM fact
GROUP BY
  raw_org_key,
  organization_name,
  organization_city,
  organization_country,
  disambiguation_source,
  disambiguated_organization_identifier,
  source_version,
  ingested_at;
""",
    )

    res_o = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM dimo")
    tot_o = 0
    for r in res_o
        tot_o = Int(getproperty(r, :c))
        break
    end

    DuckDB.execute(
        db,
        """
CREATE TEMP TABLE dimo2 AS
SELECT
  *,
  ROW_NUMBER() OVER (ORDER BY raw_org_key) - 1 AS rn
FROM dimo;
""",
    )

    q_oc = DuckDB.execute(
        db,
        """
SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt
FROM dimo2
GROUP BY 1
ORDER BY 1
""",
    )

    groups_o = Vector{NamedTuple{(:chunk_id, :cnt),Tuple{Int64,Int64}}}()
    for r in q_oc
        push!(
            groups_o,
            (chunk_id = Int(getproperty(r, :chunk_id)), cnt = Int(getproperty(r, :cnt))),
        )
    end

    done_o = 0
    out_dir_o = joinpath(out_root, "dim_org_raw");
    mkpath(out_dir_o)
    for g in groups_o
        out_file = joinpath(out_dir_o, @sprintf("%04d.parquet", g.chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT raw_org_key, organization_name, organization_city, organization_country,
         disambiguation_source, disambiguated_organization_identifier,
         source_version, ingested_at
  FROM dimo2
  WHERE CAST(FLOOR(rn / $(chunk_rows)) AS BIGINT) = $(g.chunk_id)
)
TO '$(out_file)'
WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE);
""",
        )
        done_o += g.cnt
        print("\r", fmt_bar("write_org", done_o, tot_o, t0));
        flush(stdout)
    end
    println();
    flush(stdout)

    @info "done" out_root=out_root rows_fact=tot rows_person=tot_p rows_org=tot_o
    DuckDB.close(db)
end

main()
