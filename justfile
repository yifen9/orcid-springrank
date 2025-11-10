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
	julia -e 'using JuliaFormatter; format("./src"); format("./scripts")'

ci:
	julia -e 'using Pkg; Pkg.activate("."); Pkg.instantiate(); Pkg.test()'

dev:
	just init && just fmt && just test

IN := "./data/orcid/raw/ORCID_2025_10_summaries"
NDJSON := "./data/orcid/bronze/affiliations_2025_10.ndjson"
PARQUET := "./data/orcid/silver/affiliations_2025_10.parquet"

extract:
	julia --project=. scripts/01_extract_orcid_xml_to_ndjson.jl {{IN}} {{NDJSON}}

parquet:
	julia --project=. scripts/02_ndjson_to_parquet.jl {{NDJSON}} {{PARQUET}}

etl: extract parquet
