module OrcidXML
using EzXML
using JSON3
using Printf
using Dates
using Base.Threads
using Logging

const NS = [
    "record" => "http://www.orcid.org/ns/record",
    "common" => "http://www.orcid.org/ns/common",
    "person" => "http://www.orcid.org/ns/person",
    "personal-details" => "http://www.orcid.org/ns/personal-details",
    "activities" => "http://www.orcid.org/ns/activities",
    "education" => "http://www.orcid.org/ns/education",
    "employment" => "http://www.orcid.org/ns/employment",
]

find1(xp, node) =
    node isa EzXML.Document ? EzXML.findfirst(xp, EzXML.root(node), NS) :
    EzXML.findfirst(xp, node, NS)
findall_ns(xp, node) =
    node isa EzXML.Document ? EzXML.findall(xp, EzXML.root(node), NS) :
    EzXML.findall(xp, node, NS)
txt(n) = n === nothing ? nothing : EzXML.nodecontent(n)

function date(y, m, d)
    if y === nothing && m === nothing && d === nothing
        return nothing
    end
    yy = y === nothing ? 0 : parse(Int, EzXML.nodecontent(y))
    mm = m === nothing ? 0 : parse(Int, EzXML.nodecontent(m))
    dd = d === nothing ? 0 : parse(Int, EzXML.nodecontent(d))
    @sprintf("%04d-%02d-%02d", yy, mm, dd)
end

function one_summary(el::EzXML.Node, type::String, base::NamedTuple)
    rt = find1("./common:role-title", el)
    dn = find1("./common:department-name", el)
    sd = find1("./common:start-date", el)
    sy = sd === nothing ? nothing : find1("./common:year", sd)
    sm = sd === nothing ? nothing : find1("./common:month", sd)
    sd2 = sd === nothing ? nothing : find1("./common:day", sd)
    ed = find1("./common:end-date", el)
    ey = ed === nothing ? nothing : find1("./common:year", ed)
    em = ed === nothing ? nothing : find1("./common:month", ed)
    ed2 = ed === nothing ? nothing : find1("./common:day", ed)
    org = find1("./common:organization", el)
    on = org === nothing ? nothing : find1("./common:name", org)
    adr = org === nothing ? nothing : find1("./common:address", org)
    oc = adr === nothing ? nothing : find1("./common:country", adr)
    oci = adr === nothing ? nothing : find1("./common:city", adr)
    dis = org === nothing ? nothing : find1("./common:disambiguated-organization", org)
    src = dis === nothing ? nothing : find1("./common:disambiguation-source", dis)
    did =
        dis === nothing ? nothing :
        find1("./common:disambiguated-organization-identifier", dis)
    (;
        orcid = base.orcid,
        given_names = base.given_names,
        family_name = base.family_name,
        affiliation_type = type,
        role_title = txt(rt),
        department_name = txt(dn),
        start_date = date(sy, sm, sd2),
        end_date = date(ey, em, ed2),
        organization_name = txt(on),
        organization_city = txt(oci),
        organization_country = txt(oc),
        disambiguation_source = txt(src),
        disambiguated_organization_identifier = txt(did),
    )
end

function extract_file(path::String, out::Vector{NamedTuple})
    doc = readxml(path)
    rec = find1("//record:record", doc)
    if rec === nothing
        return
    end
    pid = find1("./common:orcid-identifier/common:path", rec)
    orcid = pid === nothing ? "" : EzXML.nodecontent(pid)
    person = find1("./person:person", rec)
    pname = person === nothing ? nothing : find1("./person:name", person)
    gn =
        pname === nothing ? "" :
        (
            txt(find1("./personal-details:given-names", pname)) === nothing ? "" :
            txt(find1("./personal-details:given-names", pname))
        )
    fn =
        pname === nothing ? "" :
        (
            txt(find1("./personal-details:family-name", pname)) === nothing ? "" :
            txt(find1("./personal-details:family-name", pname))
        )
    base = (; orcid = orcid, given_names = gn, family_name = fn)
    acts = find1("./activities:activities-summary", rec)
    if acts !== nothing
        for (xp, ty) in (
            (
                "./activities:educations/activities:affiliation-group/education:education-summary",
                "education",
            ),
            (
                "./activities:employments/activities:affiliation-group/employment:employment-summary",
                "employment",
            ),
        )
            for el in findall_ns(xp, acts)
                push!(out, one_summary(el, ty, base))
            end
        end
    end
    nothing
end

function fmt_eta(done::Int, total::Int, t0::Float64)
    if done <= 0
        return "ETA --:--:--"
    end
    elapsed = time() - t0
    rate = elapsed / done
    remain = max(total - done, 0) * rate
    s = round(Int, remain)
    h = s ÷ 3600
    m = (s % 3600) ÷ 60
    ss = s % 60
    @sprintf("ETA %02d:%02d:%02d (%.2f ms/it)", h, m, ss, 1000*rate)
end

function render_bar(prefix::String, done::Int, total::Int, t0::Float64)
    w = 80
    barw = max(w - length(prefix) - 20, 10)
    pct = total == 0 ? 0.0 : 100.0 * done / total
    filled = total == 0 ? 0 : Int(clamp(round(pct/100*barw), 0, barw))
    bar = repeat("█", filled) * repeat(" ", barw - filled)
    io = IOBuffer()
    print(
        io,
        prefix,
        " ",
        @sprintf("%3d%%|", Int(clamp(round(pct), 0, 100))),
        bar,
        "|  ",
        @sprintf("%7d/%-7d  ", done, total),
        fmt_eta(done, total, t0),
    )
    String(take!(io))
end

function scan_count(in_dir::String)
    t0 = time()
    subs = [joinpath(in_dir, d) for d in readdir(in_dir) if isdir(joinpath(in_dir, d))]
    tot = Threads.Atomic{Int}(0)
    Threads.@threads for i in eachindex(subs)
        dir = subs[i]
        local c = 0
        for (root, _, fs) in walkdir(dir)
            @inbounds for f in fs
                if endswith(f, ".xml")
                    c += 1
                end
            end
        end
        Threads.atomic_add!(tot, c)
        if (i % 8) == 0
            print("\r", render_bar("scan", tot[], tot[], t0));
            flush(stdout)
        end
    end
    print("\r", render_bar("scan", tot[], tot[], t0), "\n")
    tot[]
end

function discover_files(in_dir::String, total::Int)
    subs = [joinpath(in_dir, d) for d in readdir(in_dir) if isdir(joinpath(in_dir, d))]
    ch = Channel{Vector{String}}(length(subs))
    function worker(dir::String)
        v = String[]
        for (root, _, fs) in walkdir(dir)
            @inbounds for f in fs
                if endswith(f, ".xml")
                    push!(v, joinpath(root, f))
                end
            end
        end
        put!(ch, v)
    end
    for dir in subs
        @async worker(dir)
    end
    files = String[]
    seen = 0
    t0 = time()
    for _ in subs
        v = take!(ch)
        append!(files, v)
        seen += length(v)
        print("\r", render_bar("index", seen, total, t0));
        flush(stdout)
    end
    print("\r", render_bar("index", seen, total, t0), "\n")
    sort!(files)
    files
end

function extract_dir(
    in_dir::String,
    out_prefix::String;
    nthreads::Int = Threads.nthreads(),
    batch_size::Int = 256,
    chan_size::Int = 64,
    part_bytes::Int = 16*1024*1024,
    part_rows::Int = 0,
)
    @info "run" in_dir=in_dir out_prefix=out_prefix nthreads=nthreads
    total = scan_count(in_dir)
    files = discover_files(in_dir, total)
    t0 = time()
    parsed = Threads.Atomic{Int64}(0)
    written_rows = 0
    part = 0
    io = open(@sprintf("%s.part-%08d.ndjson", out_prefix, part), "w")
    bytes = 0
    rows = 0
    ch = Channel{Vector{NamedTuple}}(chan_size)
    function producer(rangeidx::UnitRange{Int})
        buf = Vector{NamedTuple}(undef, 0)
        for i in rangeidx
            empty!(buf)
            extract_file(files[i], buf)
            Threads.atomic_add!(parsed, 1)
            if !isempty(buf)
                put!(ch, copy(buf))
            end
            if (parsed[] % 32) == 0
                print("\r", render_bar("parse", parsed[], total, t0))
                flush(stdout)
            end
        end
    end
    function writer()
        wb = time()
        k = 0
        for batch in ch
            for obj in batch
                JSON3.write(io, obj)
                write(io, '\n')
                rows += 1
                k += 1
            end
            bytes = position(io)
            if (part_rows>0 && rows>=part_rows) || (part_bytes>0 && bytes>=part_bytes)
                close(io)
                part += 1
                io = open(@sprintf("%s.part-%08d.ndjson", out_prefix, part), "w")
                bytes = 0
                rows = 0
            end
            if (k % 1024) == 0
                print("\r", render_bar("write", parsed[], total, t0))
                flush(stdout)
                k = 0
            end
        end
        print("\r", render_bar("write", parsed[], total, t0), "\n")
    end
    rr = firstindex(files):lastindex(files)
    if isempty(rr)
        close(io)
        return
    end
    step = length(rr) ÷ nthreads
    remn = length(rr) % nthreads
    ranges = Vector{UnitRange{Int}}(undef, nthreads)
    lo = first(rr)
    for t = 1:nthreads
        extra = t <= remn ? 1 : 0
        hi = min(lo + step + extra - 1, last(rr))
        ranges[t] = lo:hi
        lo = hi + 1
    end
    t_writer = @async writer()
    ts = Vector{Task}(undef, nthreads)
    for t = 1:nthreads
        ts[t] = @async producer(ranges[t])
    end
    wait.(ts)
    close(ch)
    wait(t_writer)
    close(io)
    print("\r", render_bar("parse", parsed[], total, t0), "\n")
    @info "done" files=length(files)
    nothing
end

end
