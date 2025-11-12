using DuckDB
using Dates
using Printf
using Unicode
using CSV
using DataFrames

function fmt_bar(prefix::String, done::Int, total::Int, t0::Float64)
    pct = total == 0 ? 0 : clamp(round(Int, done * 100 / total), 0, 100)
    w = 60
    k = total == 0 ? 0 : clamp(round(Int, w * pct / 100), 0, w)
    bar = string(repeat('█', k), repeat(' ', w - k))
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
        bar,
        done,
        total,
        h,
        m,
        ss,
        r
    )
end

function load_iso2(path::String)
    isfile(path) || error("missing iso2.tsv at: "*path)
    df = DataFrame(CSV.File(path; delim = '\t', header = true))
    s = Set{String}()
    for r in eachrow(df)
        push!(s, uppercase(String(r[1])))
    end
    s
end

function load_territorial(path::String)
    isfile(path) || error("missing territorial_map.tsv at: "*path)
    df = DataFrame(CSV.File(path; delim = '\t', header = true))
    d = Dict{String,String}()
    for r in eachrow(df)
        k = uppercase(strip(replace(Unicode.normalize(String(r[1]), :NFKC), r"\s+" => "")))
        v = uppercase(String(r[2]))
        d[k] = v
    end
    d
end

function norm_iso_strict(
    raw::Union{Nothing,Missing,String},
    iso2::Set{String},
    terr::Dict{String,String},
)
    if raw === nothing || raw === missing
        return nothing, "fallback"
    end
    s = Unicode.normalize(raw, :NFKC)
    s = strip(s)
    if isempty(s)
        return nothing, "fallback"
    end
    s2 = uppercase(replace(s, r"\s+" => ""))
    if length(s2) == 2 && all(isletter, s2)
        if in(s2, iso2)
            return s2, "iso2"
        else
            if haskey(terr, s2)
                tgt = terr[s2]
                return in(tgt, iso2) ? (tgt, "territorial") : (nothing, "fallback")
            else
                return nothing, "fallback"
            end
        end
    else
        key =
            uppercase(strip(replace(lowercase(Unicode.normalize(s, :NFKC)), r"\s+" => " ")))
        if haskey(terr, key)
            tgt = terr[key]
            return in(tgt, iso2) ? (tgt, "territorial") : (nothing, "fallback")
        end
        return nothing, "fallback"
    end
end

function main()
    date = ARGS[1]
    in_root = abspath(joinpath("data", "orcid", "curated", date))
    out_root = abspath(joinpath("data", "orcid", "standardized", date))
    cfg_base = abspath(joinpath("data", "external", "iso"))
    iso2_path = joinpath(cfg_base, "iso2.tsv")
    terr_path = joinpath(cfg_base, "territorial_map.tsv")
    out_map_dir = joinpath(out_root, "country")
    out_audit_dir = joinpath(out_root, "audit")
    mkpath(out_map_dir);
    mkpath(out_audit_dir)
    iso2 = load_iso2(iso2_path)
    terr = load_territorial(terr_path)
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
    t0=time()
    db = DuckDB.DB()
    DuckDB.execute(db, "SET threads=$threads")
    DuckDB.execute(db, "SET memory_limit='$memlim'")
    DuckDB.execute(db, "SET temp_directory='$tmpdir'")
    DuckDB.execute(db, "SET preserve_insertion_order=false")
    glob_pat = string(in_root, "/dim_org_raw/*.parquet")
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE orgraw AS SELECT organization_country AS organization_country_raw, COUNT(*) AS cnt FROM read_parquet('$glob_pat') GROUP BY organization_country_raw ORDER BY cnt DESC",
    )
    res_tot = DuckDB.execute(
        db,
        "SELECT SUM(cnt) AS total_rows, COUNT(*) AS distinct_vals FROM orgraw",
    )
    total_rows = 0;
    distinct_vals = 0
    for r in res_tot
        total_rows = Int(r[:total_rows]);
        distinct_vals = Int(r[:distinct_vals]);
        break
    end
    print("\r", fmt_bar("scan", 1, 1, t0));
    println();
    flush(stdout)
    vals = DuckDB.execute(db, "SELECT organization_country_raw, cnt FROM orgraw")
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE cmap(organization_country_raw VARCHAR, organization_country_iso2 VARCHAR, mapping_method VARCHAR, decided_at VARCHAR, cnt BIGINT)",
    )
    DuckDB.execute(db, "BEGIN TRANSACTION")
    stmt = DuckDB.Stmt(
        db,
        "INSERT INTO cmap VALUES (?, ?, ?, ?, ?)",
        DuckDB.MaterializedResult,
    )
    done = 0
    for r in vals
        rc = r[:organization_country_raw]
        rc_str = rc === nothing ? nothing : String(rc)
        code, method = norm_iso_strict(rc_str, iso2, terr)
        DuckDB.execute(stmt, (rc_str, code, method, decided_at, Int(r[:cnt])))
        done += 1
        if (done % 50)==0 || done==distinct_vals
            print("\r", fmt_bar("normalize", done, distinct_vals, t0));
            flush(stdout)
        end
    end
    DuckDB.execute(db, "COMMIT");
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE cmap2 AS SELECT organization_country_raw,organization_country_iso2,mapping_method,decided_at,cnt, ROW_NUMBER() OVER (ORDER BY COALESCE(organization_country_iso2,'ZZ'), COALESCE(organization_country_raw,'')) - 1 AS rn FROM cmap",
    )
    qgc = DuckDB.execute(
        db,
        "SELECT CAST(FLOOR(rn / $chunk_rows) AS BIGINT) AS chunk_id, COUNT(*) AS cnt FROM cmap2 GROUP BY 1 ORDER BY 1",
    )
    groups = Tuple{Int,Int}[]
    for r in qgc
        push!(groups, (Int(r[:chunk_id]), Int(r[:cnt])))
    end
    written = 0
    for (chunk_id, cnt) in groups
        out_file = joinpath(out_map_dir, @sprintf("%04d.parquet", chunk_id))
        DuckDB.execute(
            db,
            "COPY (SELECT organization_country_raw,organization_country_iso2,mapping_method,decided_at,cnt FROM cmap2 WHERE CAST(FLOOR(rn / $chunk_rows) AS BIGINT) = $chunk_id) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
        )
        written += cnt
        print("\r", fmt_bar("write_map", written, distinct_vals, t0));
        flush(stdout)
    end
    println();
    flush(stdout)
    DuckDB.execute(
        db,
        "CREATE TEMP TABLE audit AS SELECT SUM(cnt) AS total_rows, COUNT(*) AS distinct_raw_values, SUM(CASE WHEN organization_country_iso2 IS NOT NULL THEN cnt ELSE 0 END) AS mapped_rows, SUM(CASE WHEN organization_country_iso2 IS NULL THEN cnt ELSE 0 END) AS unmapped_rows, ROUND(100.0 * SUM(CASE WHEN organization_country_iso2 IS NOT NULL THEN cnt ELSE 0 END) / NULLIF(SUM(cnt),0), 2) AS mapped_pct, '$decided_at' AS decided_at FROM cmap",
    )
    DuckDB.execute(
        db,
        "COPY audit TO '$(joinpath(out_audit_dir,"country_report.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    DuckDB.close(db)
    println("done")
end

main()
