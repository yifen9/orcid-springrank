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

ORCID_DATE := "2025_10"
GEONAMES_DATE := "2024_03"

do-01:
	julia --project=. scripts/01_extract_source_to_ndjson.jl {{ORCID_DATE}}

do-02:
	julia --project=. scripts/02_structure_ndjson_to_parquet.jl {{ORCID_DATE}}

do-03:
	julia --project=. scripts/03_curate_dims.jl {{ORCID_DATE}}

do-04:
	julia --project=. scripts/04_field_frequency.jl {{ORCID_DATE}}

do-04a:
	julia --project=. scripts/04a_country_normalize.jl {{ORCID_DATE}}

do-04b:
	julia --project=. scripts/04b_city_text_normalize.jl {{ORCID_DATE}}

do-05a:
	julia --project=. scripts/05a_resolve_city_geonames.jl {{ORCID_DATE}} {{GEONAMES_DATE}}
