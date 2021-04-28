using Pkg
Pkg.activate(@__DIR__)
pkg"up"
pkg"instantiate"

using LibPQ, JSON3

using LibPQ: load!

using HTTP: request

ENV["SAM_KEY"] = "MwvGHSGiyuISuxArFq5wMQ2K2W4G1FhcMElsvCJ2"

conn = LibPQ.Connection("""
                        host = $(get(ENV, "PGHOST", ""))
                        dbname = sdad
                        user = $(get(ENV, "DB_USR", ""))
                        password = $(get(ENV, "DB_PWD", ""))
                        """);

execute(conn,
        """
        CREATE SCHEMA IF NOT EXISTS us_sam
        AUTHORIZATION ncses_oss;
        COMMENT ON SCHEMA us_sam
        IS 'Beta.SAM.Gov Federal Hierarchy Public API
        https://open.gsa.gov/api/fh-public-api/';
        DROP TABLE IF EXISTS us_sam.fh_public_api CASCADE;
        CREATE TABLE IF NOT EXISTS us_sam.fh_public_api (
            fhorgid text,
            fhorgtype text NOT NULL,
            fhorgname text NOT NULL,
            fhdeptindagencyorgid text NOT NULL,
            PRIMARY KEY (fhorgid)
        );
        ALTER TABLE us_sam.fh_public_api OWNER to ncses_oss;
        """
    )
f
helper(elem) =
        (fhorgid = elem.fhorgid,
         fhorgname = :fhorgname ∈ propertynames(elem) ? elem.fhorgname : missing,
         lastupdateddate = :lastupdateddate ∈ propertynames(elem) ? elem.lastupdateddate : missing)
for file in readdir(joinpath(@__DIR__, "data", "oss", "original", "us_sam_fh_public_api"))
    # file = readdir(joinpath(@__DIR__, "data", "oss", "original", "us_sam_fh_public_api"))[1]
    # file = joinpath(@__DIR__, "data", "oss", "original", "us_sam_fh_public_api", "100003657.json")
    println(file)
    obj = JSON3.read(open(joinpath(@__DIR__, "data", "oss", "original", "us_sam_fh_public_api", file)))
    fhdeptindagencyorgid = obj[1].fhdeptindagencyorgid
    fhorgtype = "Sub-Tier"
    data = DataFrame(helper(elem) for elem in obj)
    data = data[.!ismissing.(data.fhorgname),:]
    sort!(data, (:fhorgname, order(:lastupdateddate, rev=true)))
    data = by(data[!,[:fhorgid, :fhorgname]], :fhorgname, x -> first(x, 1))
    data[!,:fhdeptindagencyorgid] .= obj[1].fhdeptindagencyorgid
    data[!,:fhorgtype] .= "Sub-Tier"
    push!(data, (fhorgid = obj[1].fhdeptindagencyorgid,
                 fhorgname = obj[1].fhorgname,
                 fhdeptindagencyorgid = obj[1].fhdeptindagencyorgid,
                 fhorgtype = obj[1].fhorgtype))
    data = sort(data[!,[:fhorgid, :fhorgtype, :fhorgname, :fhdeptindagencyorgid]])
    load!(data, conn, "INSERT INTO us_sam.fh_public_api VALUES ($(join(("\$$i" for i in 1:4), ','))) ON CONFLICT DO NOTHING;")
end
    

# 
CREATE MATERIALIZED VIEW IF NOT EXISTS us_sam.fh_public_clean AS (
	WITH A AS (
		SELECT *
		FROM us_sam.fh_public_api
		WHERE fhdeptindagencyorgid <> '100000000'
	),
	B AS (
		SELECT DISTINCT fhdeptindagencyorgid, fhorgname
		FROM A
		WHERE fhorgtype = 'Department/Ind. Agency'
	)
	SELECT B.fhorgname AS parent, A.fhorgtype, A.fhorgname
	FROM B
	INNER JOIN A
	ON B.fhdeptindagencyorgid = A.fhdeptindagencyorgid
	ORDER BY parent ASC, fhorgtype ASC, fhorgname ASC
)
;
# 

    for elem in obj
        lastupdateddate = :lastupdateddate ∈ propertynames(elem) ? elem.lastupdateddate : missing
        push!(output,
              (string(elem.fhorgid),
               elem.fhorgname,
               elem.fhorgtype,
               string(elem.fhdeptindagencyorgid),
               lastupdateddate
               )
              )
    end
    sort!(output, by = x -> (x[2], x[end]))
    for fhorgname in (elem.fhorgname for elem in output)
        idx = findall(isequal(fhorgname), [])

    tmp = [ (elem.fhorgid, elem.fhorgname, elem.fhorgtype, elem.fhdeptindagencyorgid, elem.lastupdateddate)
            for elem in obj ]
    findall(elem -> elem[2] == "VIRGINIA CONTRACTING AGENCY", tmp)
    obj[596]
    obj[597]

end
d
"fhorgid":100000000,
"fhorgname":"DEPT OF DEFENSE",
"fhorgtype":"Department/Ind. Agency",
"status":"ACTIVE",
"createddate":"2006-04-14 00:00",
"updatedby":"FPDS_START_DATE_FIX",
"lastupdateddate":"2019-11-21 23:04",
"fhdeptindagencyorgid":100000000,
"fhagencyorgname":"DEPT OF DEFENSE",
"agencycode":"9700",
"oldfpdsofficecode":"9700",
"cgaclist":[{"cgac":"057"},{"cgac":"021"},{"cgac":"017"},{"cgac":"096"},{"cgac":"097"}],
"fhorgnamehistory":[{"fhorgname":"DEPT OF DEFENSE","effectivedate":"1957-07-01 00:00"}],
"fhorgparenthistory":[{"fhfullparentpathid":"100000000","fhfullparentpathname":"DEPT OF DEFENSE","effectivedate":"1957-07-01 00:00"}],
"links":[{"rel":"self","href":"https://api.sam.gov/prod/federalorganizations/v1/orgs?fhorgid=100000000","hreflang":null,"media":null,"title":null,"type":null,"deprecation":null},
         {"rel":"nextlevelchildren","href":"https://api.sam.gov/prod/federalorganizations/v1/org/hierarchy?fhorgid=100000000","hreflang":null,"media":null,"title":null,"type":null,"deprecation":null}
        ]
}

response = request("GET",
                   "https://api.sam.gov/prod/federalorganizations/v1/orgs?api_key=$(ENV["SAM_KEY"])&status=active&limit=200")
response.status = 200
json = JSON3.read(response.body)
length(json.orglist)

function find_all_records(output, url)
    limit = parse(Int, match(r"(?<=limit=)\d+", url).match)
    current_offset = parse(Int, match(r"(?<=offset=)\d+", url).match)
    url = replace(url, r"(?<=offset=)\d+" => current_offset + limit)
    response = request("GET", url)
    # output, url = vcat(output, 1), url
    json = JSON3.read(response.body)
    if length(json.orglist) < 200
        println(response)
    end
    println("A: $(length(output))")
    output = vcat(output, json.orglist)
    println("B: $(length(output))")
    output, url
end
function find_all_records(key = ENV["SAM_KEY"])
    url = "https://api.sam.gov/prod/federalorganizations/v1/orgs?api_key=$key&status=active&limit=200"
    response = request("GET", url)
    json = JSON3.read(response.body)
    totalrecords = json.totalrecords
    output = json.orglist
    url = string(url, "&offset=200")

    # response_200 = request("GET", url)
    # json_200 = JSON3.read(response_200.body)
    # totalrecords_200 = json_200.totalrecords
    # output_200 = json_200.orglist
    # url = replace(url, r"(?<=&offset=).*" => 400)

    # response_400 = request("GET", url)
    # json_400 = JSON3.read(response_400.body)
    # totalrecords_400 = json_400.totalrecords
    # output_400 = json_400.orglist
    # url = replace(url, r"(?<=&offset=).*" => 600)
    url == "https://api.sam.gov/prod/federalorganizations/v1/orgs?api_key=MwvGHSGiyuISuxArFq5wMQ2K2W4G1FhcMElsvCJ2&status=active&limit=200&offset=600"
    # response_600 = request("GET", url)
    # json_600 = JSON3.read(response_600.body)
    # totalrecords_600 = json_600.totalrecords
    # output_600 = json_600.orglist
    # url = replace(url, r"(?<=&offset=).*" => 800)

    # response_800 = request("GET", url)
    # json_800 = JSON3.read(response_800.body)
    # totalrecords_800 = json_800.totalrecords
    # output_800 = json_800.orglist
    # url = replace(url, r"(?<=&offset=).*" => 1000)

    # awesome = vcat(output, output_200, output_400, output_600, output_800)
    # length(awesome)

    # totalrecords = 3
    # output = [0]
    sleep(7)
    while length(output) < totalrecords
        println(url)
        output, url = find_all_records(output, url)
        println(length(output))
        sleep(7)
    end
    output
end
orgs = find_all_records()
io = open(joinpath(@__DIR__, "data", "oss", "original", "gsa_federalorganizations", "orglist.json"), write = true)
propertynames(io)
JSON3.write(io, awesome)

orgs = JSON3.read(read(joinpath(@__DIR__, "data", "oss", "original", "gsa_federalorganizations", "orglist.json"), String))
org = orgs[1]
length(orgs)
org
setdiff(orgs, orgs_parents)[1]
findfirst(elem -> elem.fhorgid == 500154577, orgs)
findfirst(elem -> elem.fhorgid == 500155065, orgs)

already = replace.(filter!(filename -> occursin(r"\d+(?=\.json$)", filename), readdir(joinpath(@__DIR__, "data", "oss", "original", "gsa_federalorganizations"))),
                   ".json" => "")
orgs_parents = filter(elem -> length(elem.links) == 2, orgs)
length(orgs_parents)
second_tier = sort!([elem.links[2].href[72:end] for elem in orgs_parents])
to_process = setdiff(second_tier, already)
# This one is weird
filter!(!isequal("300000201"), to_process)
intersect(second_tier, already)[1]
arg = filter(filename -> occursin(r"\d+(?=.json)", filename), readdir(joinpath(@__DIR__, "data", "oss", "original", "gsa_federalorganizations")))

maximum(x -> findfirst(elem -> elem.fhorgid == parse(Int, replace(x, ".json" => "")), orgs), arg)
length("https://api.sam.gov/prod/federalorganizations/v1/org/hierarchy?fhorgid=")
org = to_process[1]
for org in orgs[304:end]
    idx = findfirst(elem -> isequal("nextlevelchildren", elem.rel), org.links)
    if isa(idx, Integer)
        url = string(org.links[idx].href, "&api_key=$key&limit=200")
        fhorgid = org.fhorgid
        response = request("GET", url)
        json = JSON3.read(response.body)
        totalrecords = json.totalrecords
        output = json.orglist
        url = string(url, "&offset=200")
        while length(output) < totalrecords
            response = request("GET", url)
            json = JSON3.read(response.body)
            output = vcat(output, json.orglist)
            limit = parse(Int, match(r"(?<=limit=)\d+", url).match)
            current_offset = parse(Int, match(r"(?<=offset=)\d+", url).match)
            url = replace(url, r"(?<=offset=)\d+" => current_offset + limit)
        end
        io = open(joinpath(@__DIR__, "data", "oss", "original", "gsa_federalorganizations", "$(org.fhorgid).json"), write = true)
        JSON3.write(io, output)
        close(io)
        println(org.fhorgid)
    end
end
org = "300000201"
org.fhorgid
org.links[2].href

for org in to_process
    url = "https://api.sam.gov/prod/federalorganizations/v1/org/hierarchy?fhorgid=$org&status=Active&api_key=$key&limit=200"
    fhorgid = org
    response = request("GET", url)
    json = JSON3.read(response.body)
    totalrecords = json.totalrecords
    println(totalrecords)
    output = json.orglist[1]
    url = string(url, "&offset=200")
    while length(output) < totalrecords
        response = request("GET", url)
        json = JSON3.read(response.body)
        output = vcat(output, json.orglist)
        limit = parse(Int, match(r"(?<=limit=)\d+", url).match)
        current_offset = parse(Int, match(r"(?<=offset=)\d+", url).match)
        url = replace(url, r"(?<=offset=)\d+" => current_offset + limit)
    end
    io = open(joinpath(@__DIR__, "data", "oss", "original", "gsa_federalorganizations", "$org.json"), write = true)
    JSON3.write(io, output)
    close(io)
    println(org)
end



    if length(orgs[1].links) == 2
        @assert orgs[1].links[2].rel == "nextlevelchildren"




json.orglist
response_hier = request("GET",
                        "https://api.sam.gov/prod/federalorganizations/v1/org/hierarchy?api_key=$(ENV["SAM_KEY"])&fhorgid=300000015&status=active&limit=100")
json_hier = JSON3.read(response_hier.body)

response_chk = request("GET",
                        "https://api.sam.gov/prod/federalorganizations/v1/org/hierarchy?api_key=$(ENV["SAM_KEY"])&fhorgid=300000117&status=active&limit=100")
json_chk = JSON3.read(response_chk.body)
json_hier.orglist
json_chk.orglist |> length
close(conn)