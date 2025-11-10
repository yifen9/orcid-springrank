import DuckDB, DBInterface

function main(in_ndjson::String, out_parquet::String)
    con = DBInterface.connect(DuckDB.DB)
    DBInterface.execute(
        con,
        "COPY (SELECT * FROM read_json_auto(?)) TO ? (FORMAT parquet)",
        (in_ndjson, out_parquet),
    )
    DBInterface.close!(con)
end

length(ARGS) == 2 ||
    error("usage: julia scripts/02_ndjson_to_parquet.jl <in_ndjson> <out_parquet>")
main(ARGS[1], ARGS[2])
