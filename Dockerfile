#syntax=docker/dockerfile

FROM julia

ENV DEBIAN_FRONTEND=noninteractive
ENV JULIA_DEPOT_PATH=/opt/julia-depot
ENV JULIA_PROJECT=@.
ENV JULIA_NUM_THREADS=auto

RUN --mount=type=cache,target=/var/cache/apt --mount=type=cache,target=/var/lib/apt apt-get update && apt-get install -y --no-install-recommends ca-certificates git just && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/julia-depot && chmod -R 777 /opt/julia-depot

WORKDIR /workspace

RUN git config --system --add safe.directory /workspace

COPY Project.toml Manifest.toml* ./

COPY src/ ./src

RUN --mount=type=cache,target=/opt/julia-depot julia -e 'using Pkg; Pkg.instantiate(); Pkg.precompile()'

CMD ["bash","-lc","sleep infinity"]