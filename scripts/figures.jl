using VegaLite
using CSV
using DataFrames

# This file is 
data = CSV.read(joinpath("data", "oss", "working", "for_figure.csv"), DataFrame)

plt = data |>
    @vlplot(:line,
            x = {"year", axis = {title = "Year", format=:c}},
            y = {"repos", axis = {title = "Repositories Contributed To", format = "s"}},
            color = {"sector", axis = {title = "Series", format = "n"}, legend = {orient = :bottom, title = nothing}},
            height = 275,
            width = 325,
            title = "Federal Government Contribution to OSS on GitHub")
plt |> save(joinpath("repos.svg"))



plt = data |>
    @vlplot(:line,
            x = {"year", axis = {title = "Year", format=:c}},
            y = {"additions", axis = {title = "Lines Added", format = "s"}},
            color = {"sector", axis = {title = nothing, format = "n"}, legend = {orient = :bottom, title = nothing}},
            height = 275,
            width = 325,
            title = "Federal Government Contribution to OSS on GitHub")
plt |> save(joinpath("additions.svg"))
@vlplot(
    :bar,
    data={
        values=[
            {task="A",start=1,stop=3},
            {task="B",start=3,stop=8},
            {task="C",start=8,stop=10}
        ]
    },
    y="task:o",
    x="start:q",
    x2="stop:q"
)
