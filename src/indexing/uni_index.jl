mutable struct UniIndex{K, T<:AbstractTuple}
    index_properties::IndexProperties
    index_map::Dict{K, Vector{T}}
    tuple_to_key::Dict{T, K}
    
    @inline function UniIndex{K,T}(props::IndexProperties) where {K,T<:AbstractTuple}
        new{K,T}(props, Dict{K, Vector{T}}(), Dict{T, K}())
    end
end

@inline function put!(index::UniIndex{K,T}, tuple::T) where {K,T<:AbstractTuple}
    key = get_property(index.index_properties, tuple)::K
    index.tuple_to_key[tuple] = key
    
    if haskey(index.index_map, key)
        push!(index.index_map[key], tuple)
    else
        index.index_map[key] = sizehint!(Vector{T}([tuple]), 4)
    end
end

@inline function get_matches(index::UniIndex{K,T}, key::K) where {K,T<:AbstractTuple}
    return get(index.index_map, key, Vector{T}())
end

@inline function get_index(index::UniIndex{K,T}, key::K) where {K,T<:AbstractTuple}
    return get(index.index_map, key, Vector{T}())
end

@inline function remove!(index::UniIndex{K,T}, tuple::T) where {K,T<:AbstractTuple}
    key = get(index.tuple_to_key, tuple, nothing)
    if key !== nothing
        delete!(index.tuple_to_key, tuple)
        if haskey(index.index_map, key)
            list = index.index_map[key]
            filter!(t -> t !== tuple, list)
            if isempty(list)
                delete!(index.index_map, key)
            end
        end
    end
end

@inline function foreach_match(f::Function, index::UniIndex{K,T}, key::K) where {K,T<:AbstractTuple}
    tuples = get(index.index_map, key, nothing)
    if tuples !== nothing
        @inbounds for tuple in tuples
            f(tuple)
        end
    end
end

const StringUniIndex{T} = UniIndex{String, T}
const IntUniIndex{T} = UniIndex{Int, T}
const FloatUniIndex{T} = UniIndex{Float64, T}