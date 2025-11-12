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

function scan_files(dir::String)
    t0 = time()
    fs = readdir(dir)
    nd = String[]
    total = length(fs)
    done = 0
    for f in fs
        if endswith(f, ".ndjson")
            push!(nd, joinpath(dir, f))
        end
        done += 1
        if (done % 64) == 0 || done == total
            print("\r", fmt_bar("scan", done, total, t0));
            flush(stdout)
        end
    end
    println();
    flush(stdout)
    sort!(nd)
    nd
end

function main()
    date = ARGS[1]
    in_dir = abspath(joinpath("data", "orcid", "extracted", date))
    out_root = abspath(joinpath("data", "orcid", "structured", date))
    mkpath(out_root)
    chunk_rows = parse(Int, get(ENV, "CHUNK_ROWS", "16384"))
    threads = try
        parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
    catch
        ;
        Sys.CPU_THREADS
    end
    memlim = get(ENV, "DUCKDB_MEM", "8GiB")
    tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")))
    mkpath(tmpdir)

    @info "start" date=date in_dir=in_dir out_root=out_root chunk_rows=chunk_rows threads=threads mem=memlim
    files = scan_files(in_dir)
    @info "found_parts" count=length(files)

    glob_pat = string(in_dir, "/*.ndjson")
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
CREATE TEMP TABLE aff AS
SELECT
  orcid::VARCHAR                           AS orcid,
  given_names::VARCHAR                     AS given_names,
  family_name::VARCHAR                     AS family_name,
  affiliation_type::VARCHAR                AS affiliation_type,
  role_title::VARCHAR                      AS role_title,
  department_name::VARCHAR                 AS department_name,
  start_date::VARCHAR                      AS start_date,
  CASE
    WHEN start_date IS NULL THEN NULL
    WHEN length(start_date) < 4 THEN NULL
    ELSE CAST(substr(start_date,1,4) AS INTEGER)
  END AS start_year,
  end_date::VARCHAR                        AS end_date,
  organization_name::VARCHAR               AS organization_name,
  organization_city::VARCHAR               AS organization_city,
  organization_country::VARCHAR            AS organization_country,
  disambiguation_source::VARCHAR           AS disambiguation_source,
  disambiguated_organization_identifier::VARCHAR AS disambiguated_organization_identifier
FROM read_json('$glob_pat',
               format='newline_delimited',
               columns={
                 orcid: 'VARCHAR',
                 given_names: 'VARCHAR',
                 family_name: 'VARCHAR',
                 affiliation_type: 'VARCHAR',
                 role_title: 'VARCHAR',
                 department_name: 'VARCHAR',
                 start_date: 'VARCHAR',
                 end_date: 'VARCHAR',
                 organization_name: 'VARCHAR',
                 organization_city: 'VARCHAR',
                 organization_country: 'VARCHAR',
                 disambiguation_source: 'VARCHAR',
                 disambiguated_organization_identifier: 'VARCHAR'
               });
""",
    )

    res_tot = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM aff")
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
CREATE TEMP TABLE aff2 AS
SELECT
  *,
  COALESCE(LPAD(CAST(start_year AS VARCHAR), 4, '0'), 'null') AS year_dir,
  ROW_NUMBER() OVER (PARTITION BY COALESCE(LPAD(CAST(start_year AS VARCHAR), 4, '0'), 'null')
                     ORDER BY orcid) - 1 AS rn
FROM aff;
""",
    )

    q_counts = DuckDB.execute(
        db,
        """
SELECT
  year_dir,
  CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id,
  COUNT(*) AS cnt
FROM aff2
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
    @info "plan" groups=total_groups total_rows=tot

    for (idx, g) in pairs(groups)
        out_dir = joinpath(out_root, g.year_dir);
        mkpath(out_dir)
        out_file = joinpath(out_dir, @sprintf("%04d.parquet", g.chunk_id))
        DuckDB.execute(
            db,
            """
COPY (
  SELECT
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
    disambiguated_organization_identifier
  FROM aff2
  WHERE year_dir = '$(g.year_dir)'
    AND CAST(FLOOR(rn / $(chunk_rows)) AS BIGINT) = $(g.chunk_id)
)
TO '$(out_file)'
WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE);
""",
        )
        done_rows += g.cnt
        print("\r", fmt_bar("write", done_rows, tot, t0));
        flush(stdout)
    end

    println();
    flush(stdout)
    @info "done" out_root=out_root rows=tot files=total_groups
    DuckDB.close(db)
end

main()
