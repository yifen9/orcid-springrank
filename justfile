up:
	julia -e 'using Pkg; Pkg.update()'

add NAME:
	julia -e 'using Pkg; Pkg.add("{{NAME}}")'

rm NAME:
	julia -e 'using Pkg; Pkg.rm("{{NAME}}")'

init:
	julia -e 'using Pkg; Pkg.instantiate()'

resolve:
	julia -e 'using Pkg; Pkg.resolve()'

test:
	julia -e 'using Pkg; Pkg.test()'

fmt:
	julia -e 'using JuliaFormatter; format("./scripts")'

ci:
	julia -e 'using Pkg; Pkg.activate("."); Pkg.instantiate(); Pkg.test()'

dev:
	just init && just fmt && just test

DATE := "2025_10"

extract:
	julia --project=. scripts/01_extract_source_to_ndjson.jl {{DATE}}

parquet:
	julia --project=. scripts/02_structure_ndjson_to_parquet.jl {{DATE}}
