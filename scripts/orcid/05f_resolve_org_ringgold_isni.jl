using DuckDB
using Dates
using Printf
using FilePathsBase

function bar(p::String, d::Int, t::Int, t0::Float64)
    pct = t == 0 ? 0 : clamp(round(Int, d*100/t), 0, 100)
    w = 60
    k = t == 0 ? 0 : clamp(round(Int, w*pct/100), 0, w)
    b = string(repeat('█', k), repeat(' ', w-k))
    el = time()-t0
    r = el/max(d, 1)
    rm = max(t-d, 0)*r
    s = round(Int, rm)
    h=s÷3600
    m=(s%3600)÷60
    ss=s%60
    @sprintf(
        "%s %3d%%|%s| %d/%d  ETA %02d:%02d:%02d (%.2f s/unit)",
        p,
        pct,
        b,
        d,
        t,
        h,
        m,
        ss,
        r
    )
end

orcid_date = ARGS[1]
ror_date = ARGS[2]
ring_date = ARGS[3]

cur_root = abspath(joinpath("data", "orcid", "curated", orcid_date))
std_root = abspath(joinpath("data", "orcid", "standardized", orcid_date))
res_root = abspath(joinpath("data", "orcid", "resolved", orcid_date, "org"))
mkpath(res_root)
out_rg = joinpath(res_root, "match_idmap_ringgold_isni")
out_um = joinpath(res_root, "unmatched_ringgold_isni")
audit_dir = joinpath(res_root, "audit")
best_dir = joinpath(res_root, "match_idmap_best")
mkpath(out_rg);
mkpath(out_um);
mkpath(audit_dir);
mkpath(best_dir)

ror_root = abspath(joinpath("data", "external", "ror", ror_date, "parquet"))
ring_root = abspath(joinpath("data", "external", "ringgold", ring_date))

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
decided_at = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
t0 = time()

db = DuckDB.DB()
DuckDB.execute(db, "SET threads=$threads")
DuckDB.execute(db, "SET memory_limit='$memlim'")
DuckDB.execute(db, "SET temp_directory='$tmpdir'")
DuckDB.execute(db, "SET preserve_insertion_order=false")

DuckDB.execute(
    db,
    "CREATE TEMP VIEW org_raw AS SELECT raw_org_key, organization_name AS organization_name_raw, organization_city AS organization_city_raw, organization_country AS organization_country_raw, disambiguation_source AS disambiguation_source_raw, disambiguated_organization_identifier AS disambiguated_organization_identifier FROM read_parquet('$(joinpath(cur_root,"dim_org_raw","*.parquet"))')",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE ctry_map AS SELECT organization_country_raw, organization_country_iso2 FROM read_parquet('$(joinpath(std_root,"country","*.parquet"))')",
)

DuckDB.execute(
    db,
    """
CREATE TEMP TABLE org_signals AS
SELECT
  lower(coalesce(org_raw.disambiguation_source_raw,''))                               AS disambiguation_source_l,
  org_raw.organization_name_raw                                                       AS organization_name_raw,
  lower(regexp_replace(trim(coalesce(org_raw.organization_name_raw,'')),'\\s+',' '))  AS organization_name_norm,
  org_raw.organization_city_raw                                                       AS organization_city_raw,
  org_raw.organization_country_raw                                                    AS organization_country_raw,
  ctry_map.organization_country_iso2                                                  AS organization_country_iso2,
  org_raw.disambiguated_organization_identifier                                       AS disambiguated_organization_identifier,
  COUNT(*)::BIGINT                                                                    AS pair_cnt,
  row_number() over ()                                                                AS org_row_id
FROM org_raw
LEFT JOIN ctry_map
  ON ctry_map.organization_country_raw = org_raw.organization_country_raw
GROUP BY 1,2,3,4,5,6,7;
""",
)

res_tot = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM org_signals")
tot_pairs = 0
for r in res_tot
    ;
    tot_pairs = Int(r[:c]);
end
print("\r", bar("materialize_org_text", tot_pairs, tot_pairs, t0));
println();
flush(stdout)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE x_isni_ror AS SELECT upper(regexp_replace(value,'\\s+','')) AS isni_norm, id AS ror_id FROM read_parquet('$(joinpath(ror_root,"external_id.parquet"))') WHERE lower(type)='isni' AND value IS NOT NULL",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE rg_isni AS SELECT CAST(ringgold AS VARCHAR) AS ringgold_id, upper(regexp_replace(isni,'\\s+','')) AS isni_norm FROM read_csv_auto('$(joinpath(ring_root,"isni.tsv"))', delim='\\t', header=true) WHERE isni IS NOT NULL AND length(trim(isni))>0",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE x_rg_ror AS SELECT r.ringgold_id, i.ror_id FROM rg_isni r JOIN x_isni_ror i USING(isni_norm)",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE s_ringgold AS SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, regexp_extract(lower(coalesce(disambiguated_organization_identifier,'')),'([0-9]+)',1) AS ringgold_num FROM org_signals WHERE disambiguation_source_l='ringgold'",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE m_rg AS SELECT s.org_row_id, s.organization_name_raw, s.organization_name_norm, s.organization_country_iso2, s.pair_cnt, x.ror_id, 'ringgold_isni'::VARCHAR AS match_source, s.ringgold_num::VARCHAR AS source_value_norm, 3 AS match_priority, 1.0::DOUBLE AS confidence, 1 AS match_rule FROM s_ringgold s JOIN x_rg_ror x ON x.ringgold_id = s.ringgold_num",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE um_rg AS SELECT s.* FROM s_ringgold s LEFT JOIN x_rg_ror x ON x.ringgold_id = s.ringgold_num WHERE x.ringgold_id IS NULL",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE m_rg_rn AS SELECT *, row_number() over (order by org_row_id) - 1 AS rn FROM m_rg",
)
DuckDB.execute(
    db,
    "CREATE TEMP TABLE um_rg_rn AS SELECT *, row_number() over (order by org_row_id) - 1 AS rn FROM um_rg",
)

q1 = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM m_rg")
n1 = 0
for r in q1
    ;
    n1 = Int(r[:c]);
end
q2 = DuckDB.execute(db, "SELECT COUNT(*) AS c FROM um_rg")
n2 = 0
for r in q2
    ;
    n2 = Int(r[:c]);
end
print("\r", bar("join_signals", n1+n2, n1+n2, t0));
println();
flush(stdout)

qg1 = DuckDB.execute(
    db,
    "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM m_rg_rn GROUP BY 1 ORDER BY 1",
)
groups1 = Tuple{Int,Int}[]
for r in qg1
    ;
    push!(groups1, (Int(r[:gid]), Int(r[:cnt])));
end
w = 0
for (gid, cnt) in groups1
    out_file = joinpath(out_rg, @sprintf("%04d.parquet", gid))
    DuckDB.execute(
        db,
        "COPY (SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, ror_id, match_source, source_value_norm, 3 AS match_priority, confidence, match_rule, '$decided_at' AS decided_at FROM m_rg_rn WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    w += cnt
    print("\r", bar("write_ringgold", w, n1, t0));
    flush(stdout)
end
println();
flush(stdout)

qg2 = DuckDB.execute(
    db,
    "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM um_rg_rn GROUP BY 1 ORDER BY 1",
)
groups2 = Tuple{Int,Int}[]
for r in qg2
    ;
    push!(groups2, (Int(r[:gid]), Int(r[:cnt])));
end
u = 0
for (gid, cnt) in groups2
    out_file = joinpath(out_um, @sprintf("%04d.parquet", gid))
    DuckDB.execute(
        db,
        "COPY (SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, '$decided_at' AS decided_at FROM um_rg_rn WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    u += cnt
    print("\r", bar("write_unmatched", u, n2, t0));
    flush(stdout)
end
println();
flush(stdout)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE total_w AS SELECT COALESCE(SUM(pair_cnt),0) AS total_pairs_weighted FROM org_signals",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE best_prev AS SELECT * FROM read_parquet('$(joinpath(res_root,"match_idmap_best","*.parquet"))')",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE all_now AS SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, decided_at FROM best_prev UNION ALL SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, ror_id, match_source, source_value_norm, 3 AS match_priority, confidence, match_rule, decided_at FROM read_parquet('$(joinpath(out_rg,"*.parquet"))')",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE best_now AS SELECT * FROM (SELECT a.*, row_number() over (PARTITION BY org_row_id ORDER BY match_priority, confidence DESC, ror_id) AS rk FROM all_now a) t WHERE rk=1",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE audit_now AS SELECT '$decided_at' AS decided_at, 1 AS stage, (SELECT COUNT(*) FROM org_signals) AS total_pairs_distinct, (SELECT total_pairs_weighted FROM total_w) AS total_pairs_weighted, (SELECT COUNT(*) FROM best_now) AS matched_distinct, (SELECT COALESCE(SUM(pair_cnt),0) FROM best_now) AS matched_weighted, ROUND(100.0 * (SELECT COALESCE(SUM(pair_cnt),0) FROM best_now) / NULLIF((SELECT total_pairs_weighted FROM total_w),0), 2) AS matched_pct_weighted",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE audit_by_source AS SELECT match_source, COUNT(*) AS matched_distinct, COALESCE(SUM(pair_cnt),0) AS matched_weighted FROM best_now GROUP BY match_source ORDER BY match_source",
)

DuckDB.execute(
    db,
    "CREATE TEMP TABLE best_rn AS SELECT *, row_number() over (order by org_row_id) - 1 AS rn FROM best_now",
)
qg3 = DuckDB.execute(
    db,
    "SELECT cast(floor(rn / $chunk_rows) as bigint) AS gid, count(*) AS cnt FROM best_rn GROUP BY 1 ORDER BY 1",
)
groups3 = Tuple{Int,Int}[]
for r in qg3
    ;
    push!(groups3, (Int(r[:gid]), Int(r[:cnt])));
end
b = 0
for (gid, cnt) in groups3
    out_file = joinpath(best_dir, @sprintf("%04d.parquet", gid))
    DuckDB.execute(
        db,
        "COPY (SELECT org_row_id, organization_name_raw, organization_name_norm, organization_country_iso2, pair_cnt, ror_id, match_source, source_value_norm, match_priority, confidence, match_rule, decided_at FROM best_rn WHERE cast(floor(rn / $chunk_rows) as bigint) = $gid) TO '$out_file' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
    )
    b += cnt
    print("\r", bar("write_best", b, sum(last, groups3; init = 0), t0));
    flush(stdout)
end
println();
flush(stdout)

DuckDB.execute(
    db,
    "COPY audit_now TO '$(joinpath(audit_dir,"org_match_idmap_overall.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)
DuckDB.execute(
    db,
    "COPY audit_by_source TO '$(joinpath(audit_dir,"org_match_idmap_by_source.parquet"))' WITH (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE)",
)

DuckDB.close(db)
println("done")
