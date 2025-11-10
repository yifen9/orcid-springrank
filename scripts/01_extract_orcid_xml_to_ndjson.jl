include(joinpath(@__DIR__, "..", "src", "etl", "orcid_xml.jl"))
using .OrcidXML
using Logging

in_dir = ARGS[1]
out_arg = ARGS[2]
out_prefix = endswith(out_arg, ".ndjson") ? out_arg[1:(end-7)] : out_arg

@info "start" in_dir=in_dir out_prefix=out_prefix nthreads=64
OrcidXML.extract_dir(
    in_dir,
    out_prefix;
    nthreads = 64,
    part_bytes = 16*1024*1024,
    batch_size = 4096,
    chan_size = 16384,
)
