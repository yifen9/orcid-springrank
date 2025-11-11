using DuckDB
using Dates
using Printf

function fmt_bar(p::String, d::Int, t::Int, t0::Float64)
    pct = t == 0 ? 0 : clamp(round(Int, d*100/t), 0, 100)
    w = 60;
    k = t == 0 ? 0 : clamp(round(Int, w*pct/100), 0, w)
    bar = string(repeat('█', k), repeat(' ', w-k))
    el = time()-t0;
    r = el/max(d, 1);
    rm = max(t-d, 0)*r
    s = round(Int, rm);
    h=s÷3600;
    m=(s%3600)÷60;
    ss=s%60
    @sprintf(
        "%s %3d%%|%s| %d/%d  ETA %02d:%02d:%02d (%.2f s/unit)",
        p,
        pct,
        bar,
        d,
        t,
        h,
        m,
        ss,
        r
    )
end

function write_freq(
    db::DuckDB.DB,
    sql_source::String,
    out_dir::String,
    chunk_rows::Int,
    label::String,
    step::Int,
    total_steps::Int,
    t0::Float64,
)
    DuckDB.execute(db, "CREATE TEMP TABLE _freq AS $sql_source")
    res = DuckDB.execute(db, "SELECT COUNT(*) AS n FROM _freq")
    total = 0
    for r in res
        total = Int(r[:n])
        break
    end
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE _freq2 AS SELECT value, cnt, ROW_NUMBER() OVER (ORDER BY cnt DESC, COALESCE(value,'')) - 1 AS rn FROM _freq",
    )
    qgc = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt FROM _freq2 GROUP BY 1 ORDER BY 1",
    )
    groups = Vector{NamedTuple{(:chunk_id, :cnt),Tuple{Int64,Int64}}}()
    for r in qgc
        push!(groups, (chunk_id = Int(r[:chunk_id]), cnt = Int(r[:cnt])))
    end
    mkpath(out_dir)
    donew = 0
    for g in groups
        out_file = joinpath(out_dir, @sprintf("%04d.parquet", g.chunk_id))
        DuckDB.execute(
            db,
            "COPY (SELECT value, cnt FROM _freq2 WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $(g.chunk_id)) TO '$(out_file)' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
        )
        donew += g.cnt
        print("\r", fmt_bar(label, donew, total, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(db, "DROP TABLE _freq")
    DuckDB.execute(db, "DROP TABLE _freq2")
    print("\r", fmt_bar(@sprintf("step %d/%d", step, total_steps), step, total_steps, t0));
    println();
    flush(stdout)
end

date = ARGS[1]
in_root = abspath(joinpath("data", "orcid", "curated", date))
out_root = abspath(joinpath("data", "orcid", "standardized", date, "audit", "field_freq"))
mkpath(out_root)
threads = try
    parse(Int, get(ENV, "DUCKDB_THREADS", string(Sys.CPU_THREADS)))
catch
    ;
    Sys.CPU_THREADS
end
memlim = get(ENV, "DUCKDB_MEM", "16GiB")
tmpdir = abspath(get(ENV, "DUCKDB_TMP", joinpath("data", "_duckdb_tmp")));
mkpath(tmpdir)
chunk_rows = 16384

t0 = time()

db = DuckDB.DB()
DuckDB.execute(db, "SET threads=$threads")
DuckDB.execute(db, "SET memory_limit='$memlim'")
DuckDB.execute(db, "SET temp_directory='$tmpdir'")
DuckDB.execute(db, "SET preserve_insertion_order=false")

glob_org = string(in_root, "/dim_org_raw/*.parquet")
glob_fact = string(in_root, "/fact_affiliation/*/*.parquet")

DuckDB.execute(db, "CREATE TEMP VIEW org AS SELECT * FROM read_parquet('$glob_org')")
DuckDB.execute(
    db,
    "CREATE TEMP VIEW fact AS SELECT role_title FROM read_parquet('$glob_fact')",
)

write_freq(
    db,
    "SELECT organization_country AS value, COUNT(*) AS cnt FROM org GROUP BY 1",
    joinpath(out_root, "organization_country"),
    chunk_rows,
    "country",
    1,
    4,
    t0,
)

write_freq(
    db,
    "SELECT organization_city AS value, COUNT(*) AS cnt FROM org GROUP BY 1",
    joinpath(out_root, "organization_city"),
    chunk_rows,
    "city",
    2,
    4,
    t0,
)

write_freq(
    db,
    "SELECT organization_name AS value, COUNT(*) AS cnt FROM org GROUP BY 1",
    joinpath(out_root, "organization_name"),
    chunk_rows,
    "org_name",
    3,
    4,
    t0,
)

write_freq(
    db,
    "SELECT role_title AS value, COUNT(*) AS cnt FROM fact GROUP BY 1",
    joinpath(out_root, "role_title"),
    chunk_rows,
    "role_title",
    4,
    4,
    t0,
)

DuckDB.close(db)
println("done")
