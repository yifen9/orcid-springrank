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

do-01:
	julia --project=. scripts/01_extract_source_to_ndjson.jl {{DATE}}

do-02:
	julia --project=. scripts/02_structure_ndjson_to_parquet.jl {{DATE}}

do-03:
	julia --project=. scripts/03_curate_dims.jl {{DATE}}

do-04:
	julia --project=. scripts/04_field_frequency.jl {{DATE}}

do-04a:
	julia --project=. scripts/04a_country_normalize.jl {{DATE}}

do-04b:
	julia --project=. scripts/04b_city_text_normalize.jl {{DATE}}
