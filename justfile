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
GEONAMES_DATE := "2024_03_10"
ROR_DATE := "2025_10_28"
RINGGOLD_DATE := "2019_06_08"

ror-01:
	julia --project=. scripts/external/ror/01_ingest_to_parquet.jl {{ROR_DATE}}

ror-02:
	julia --project=. scripts/external/ror/02_build_derived.jl {{ROR_DATE}}

orcid-01:
	julia --project=. scripts/orcid/01_extract_source_to_ndjson.jl {{ORCID_DATE}}

orcid-02:
	julia --project=. scripts/orcid/02_structure_ndjson_to_parquet.jl {{ORCID_DATE}}

orcid-03:
	julia --project=. scripts/orcid/03_curate_dims.jl {{ORCID_DATE}}

orcid-04:
	julia --project=. scripts/orcid/04_field_frequency.jl {{ORCID_DATE}}

orcid-04a:
	julia --project=. scripts/orcid/04a_country_normalize.jl {{ORCID_DATE}}

orcid-04b:
	julia --project=. scripts/orcid/04b_city_text_normalize.jl {{ORCID_DATE}}

orcid-04c:
	julia --project=. scripts/orcid/04c_org_text_normalize.jl {{ORCID_DATE}}

orcid-04d:
	julia --project=. scripts/orcid/04d_role_title_text_normalize.jl {{ORCID_DATE}}

orcid-05a:
	julia --project=. scripts/orcid/05a_resolve_city_geonames_exact.jl {{ORCID_DATE}} {{GEONAMES_DATE}}

orcid-05b:
	julia --project=. scripts/orcid/05b_resolve_city_geonames_alias.jl {{ORCID_DATE}} {{GEONAMES_DATE}}

orcid-05c:
	julia --project=. scripts/orcid/05c_resolve_city_geonames_fuzzy.jl {{ORCID_DATE}} {{GEONAMES_DATE}}

orcid-05d:
	julia --project=. scripts/orcid/05d_merge_city_matches.jl {{ORCID_DATE}} {{GEONAMES_DATE}}

orcid-05e:
	julia --project=. scripts/orcid/05e_resolve_org_idmap_ror.jl {{ORCID_DATE}} {{ROR_DATE}}

orcid-05f:
	julia --project=. scripts/orcid/05f_resolve_org_idmap_ringgold_isni.jl {{ORCID_DATE}} {{ROR_DATE}} {{RINGGOLD_DATE}}

orcid-05g:
	julia --project=. scripts/orcid/05g_merge_org_idmap_stage12.jl {{ORCID_DATE}}
