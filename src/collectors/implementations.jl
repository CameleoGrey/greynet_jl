module Collectors
    using ..Greynet
    using DataStructures
    export count_collector, sum_collector, to_list_collector, unordered_to_list_collector, min_collector, max_collector

    mutable struct CountCollector <: AbstractCollector
        count::Int
        CountCollector() = new(0)
    end

    Greynet.is_empty(c::CountCollector) = c.count == 0
    Greynet.result(c::CountCollector) = c.count
    function Greynet.insert!(c::CountCollector, item)
        c.count += 1
        return () -> (c.count -= 1)
    end
    count_collector() = () -> CountCollector()

    mutable struct ToListCollector{T} <: AbstractCollector
        items::Vector{T}
        ToListCollector{T}() where T = new{T}(Vector{T}())
    end

    Greynet.is_empty(c::ToListCollector) = isempty(c.items)
    Greynet.result(c::ToListCollector) = copy(c.items)
    function Greynet.insert!(c::ToListCollector{T}, item::T) where T
        push!(c.items, item)
        return () -> begin
            idx = findfirst(isequal(item), c.items)
            if !isnothing(idx)
                deleteat!(c.items, idx)
            end
        end
    end
    to_list_collector(T::Type) = () -> ToListCollector{T}()

    mutable struct UnorderedToListCollector{T} <: AbstractCollector
        item_counts::Dict{T, Int}
        UnorderedToListCollector{T}() where T = new{T}(Dict{T, Int}())
    end

    Greynet.is_empty(c::UnorderedToListCollector) = isempty(c.item_counts)
    function Greynet.result(c::UnorderedToListCollector{T}) where T
        items = Vector{T}()
        sizehint!(items, sum(values(c.item_counts); init=0))
        for (item, count) in c.item_counts
            for _ in 1:count
                push!(items, item)
            end
        end
        return items
    end
    function Greynet.insert!(c::UnorderedToListCollector{T}, item::T) where T
        c.item_counts[item] = get(c.item_counts, item, 0) + 1
        return () -> begin
            c.item_counts[item] -= 1
            if c.item_counts[item] == 0
                delete!(c.item_counts, item)
            end
        end
    end
    unordered_to_list_collector(T::Type) = () -> UnorderedToListCollector{T}()

    mutable struct SumCollector{T, V<:Number} <: AbstractCollector
        mapping_function::Function
        total::V
        count::Int
        SumCollector{T,V}(f) where {T,V} = new{T,V}(f, zero(V), 0)
    end

    Greynet.is_empty(c::SumCollector) = c.count == 0
    Greynet.result(c::SumCollector) = c.total
    function Greynet.insert!(c::SumCollector{T,V}, item::T) where {T,V}
        value = c.mapping_function(item)::V
        c.total += value
        c.count += 1
        return () -> begin
            c.total -= value
            c.count -= 1
        end
    end
    
    sum_collector(f::Function, ::Type{T}, ::Type{V}) where {T, V<:Number} = () -> SumCollector{T,V}(f)
    
    # Ergonomic constructors without type inference overhead
    sum_collector(f::Function, ::Type{T}) where T = sum_collector(f, T, Float64)  # Default to Float64
    min_collector(f::Function, ::Type{T}) where T = min_collector(f, T, Float64)
    max_collector(f::Function, ::Type{T}) where T = max_collector(f, T, Float64)

    function handle_sorted_insert!(counts::Accumulator{V, Int}, sorted_keys::Vector{V}, value::V) where V
        if !haskey(counts, value) || counts[value] == 0
            insert!(sorted_keys, searchsortedfirst(sorted_keys, value), value)
        end
        push!(counts, value)
    end

    function handle_sorted_retract!(counts::Accumulator{V, Int}, sorted_keys::Vector{V}, value::V) where V
        counts[value] -= 1
        if counts[value] == 0
            delete!(counts, value)
            idx = searchsortedfirst(sorted_keys, value)
            if idx <= length(sorted_keys) && sorted_keys[idx] == value
                deleteat!(sorted_keys, idx)
            end
        end
    end

    mutable struct MinCollector{T, V} <: AbstractCollector
        mapping_function::Function
        counts::Accumulator{V, Int}
        sorted_keys::Vector{V}
        MinCollector{T,V}(f) where {T,V} = new{T,V}(f, Accumulator{V, Int}(), Vector{V}())
    end

    Greynet.is_empty(c::MinCollector) = isempty(c.sorted_keys)
    Greynet.result(c::MinCollector) = isempty(c.sorted_keys) ? nothing : c.sorted_keys[1]
    function Greynet.insert!(c::MinCollector{T,V}, item::T) where {T,V}
        value = c.mapping_function(item)::V
        handle_sorted_insert!(c.counts, c.sorted_keys, value)
        return () -> handle_sorted_retract!(c.counts, c.sorted_keys, value)
    end
    
    min_collector(f::Function, ::Type{T}, ::Type{V}) where {T,V} = () -> MinCollector{T,V}(f)

    mutable struct MaxCollector{T, V} <: AbstractCollector
        mapping_function::Function
        counts::Accumulator{V, Int}
        sorted_keys::Vector{V}
        MaxCollector{T,V}(f) where {T,V} = new{T,V}(f, Accumulator{V, Int}(), Vector{V}())
    end

    Greynet.is_empty(c::MaxCollector) = isempty(c.sorted_keys)
    Greynet.result(c::MaxCollector) = isempty(c.sorted_keys) ? nothing : c.sorted_keys[end]
    function Greynet.insert!(c::MaxCollector{T,V}, item::T) where {T,V}
        value = c.mapping_function(item)::V
        handle_sorted_insert!(c.counts, c.sorted_keys, value)
        return () -> handle_sorted_retract!(c.counts, c.sorted_keys, value)
    end
    
    max_collector(f::Function, ::Type{T}, ::Type{V}) where {T,V} = () -> MaxCollector{T,V}(f)
end