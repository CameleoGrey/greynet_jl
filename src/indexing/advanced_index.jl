using DataStructures: SortedDict

mutable struct AdvancedIndex{K, T<:AbstractTuple}
    index_properties::IndexProperties
    joiner_type::JoinerType
    sorted_data::SortedDict{K, Vector{T}}
    tuple_to_key::Dict{T, K}
    
    @inline function AdvancedIndex{K,T}(props::IndexProperties, joiner::JoinerType) where {K,T<:AbstractTuple}
        new{K,T}(props, joiner, SortedDict{K, Vector{T}}(), Dict{T, K}())
    end
end

@inline function put!(index::AdvancedIndex{K,T}, tuple::T) where {K,T<:AbstractTuple}
    key = get_property(index.index_properties, tuple)::K
    index.tuple_to_key[tuple] = key
    
    if haskey(index.sorted_data, key)
        push!(index.sorted_data[key], tuple)
    else
        index.sorted_data[key] = sizehint!(Vector{T}([tuple]), 4)
    end
end

@inline function remove!(index::AdvancedIndex{K,T}, tuple::T) where {K,T<:AbstractTuple}
    key = get(index.tuple_to_key, tuple, nothing)
    if key !== nothing
        delete!(index.tuple_to_key, tuple)
        if haskey(index.sorted_data, key)
            tuples = index.sorted_data[key]
            filter!(t -> t !== tuple, tuples)
            if isempty(tuples)
                delete!(index.sorted_data, key)
            end
        end
    end
end

function foreach_match(f::Function, index::AdvancedIndex, query_key)
    jt = index.joiner_type
    
    if jt == EQUAL
        tuples = get(index.sorted_data, query_key, nothing)
        if tuples !== nothing
            for tuple in tuples
                f(tuple)
            end
        end
    elseif jt == LESS_THAN
        for (k, tuples) in index.sorted_data
            if k >= query_key break end
            for tuple in tuples
                f(tuple)
            end
        end
    elseif jt == LESS_THAN_OR_EQUAL
        for (k, tuples) in index.sorted_data
            if k > query_key break end
            for tuple in tuples
                f(tuple)
            end
        end
    elseif jt == GREATER_THAN
        found_start = false
        for (k, tuples) in index.sorted_data
            if !found_start
                if k > query_key
                    found_start = true
                else
                    continue
                end
            end
            for tuple in tuples
                f(tuple)
            end
        end
    elseif jt == GREATER_THAN_OR_EQUAL
        found_start = false
        for (k, tuples) in index.sorted_data
            if !found_start
                if k >= query_key
                    found_start = true
                else
                    continue
                end
            end
            for tuple in tuples
                f(tuple)
            end
        end
    elseif jt == NOT_EQUAL
        for (k, tuples) in index.sorted_data
            if k != query_key
                for tuple in tuples
                    f(tuple)
                end
            end
        end
    end
end

function get_matches(index::AdvancedIndex{K,T}, query_key::K) where {K,T}
    results = Vector{T}()
    
    if index.joiner_type in [EQUAL]
        sizehint!(results, 4)
    elseif index.joiner_type in [LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL]
        sizehint!(results, min(length(index.sorted_data) * 2, 100))
    else
        total_tuples = sum(length(tuples) for tuples in values(index.sorted_data))
        sizehint!(results, total_tuples)
    end
    
    foreach_match(t -> push!(results, t), index, query_key)
    return results
end

@inline function create_index(props::IndexProperties, joiner::JoinerType, ::Type{K}, ::Type{V}) where {K, V<:AbstractTuple}
    return joiner == EQUAL ? UniIndex{K, V}(props) : AdvancedIndex{K, V}(props, joiner)
end

function get_index_stats(index::AdvancedIndex)
    total_entries = length(index.sorted_data)
    total_tuples = sum(length(tuples) for tuples in values(index.sorted_data))
    avg_tuples_per_key = total_entries > 0 ? total_tuples / total_entries : 0.0
    
    return (
        entries = total_entries,
        tuples = total_tuples, 
        avg_per_key = avg_tuples_per_key,
        memory_efficiency = total_tuples > 0 ? total_entries / total_tuples : 1.0
    )
end

function put_batch!(index::AdvancedIndex{K,T}, tuples::Vector{T}) where {K,T<:AbstractTuple}
    key_groups = Dict{K, Vector{T}}()
    
    for tuple in tuples
        key = get_property(index.index_properties, tuple)::K
        index.tuple_to_key[tuple] = key
        if haskey(key_groups, key)
            push!(key_groups[key], tuple)
        else
            key_groups[key] = [tuple]
        end
    end
    
    for (key, group_tuples) in key_groups
        if haskey(index.sorted_data, key)
            append!(index.sorted_data[key], group_tuples)
        else
            index.sorted_data[key] = sizehint!(copy(group_tuples), length(group_tuples) + 4)
        end
    end
end

function remove_batch!(index::AdvancedIndex{K,T}, tuples::Vector{T}) where {K,T<:AbstractTuple}
    key_groups = Dict{K, Vector{T}}()
    
    for tuple in tuples
        key = get(index.tuple_to_key, tuple, nothing)
        if key !== nothing
            delete!(index.tuple_to_key, tuple)
            if haskey(key_groups, key)
                push!(key_groups[key], tuple)
            else
                key_groups[key] = [tuple]
            end
        end
    end
    
    for (key, group_tuples) in key_groups
        if haskey(index.sorted_data, key)
            current_tuples = index.sorted_data[key]
            for tuple_to_remove in group_tuples
                filter!(t -> t !== tuple_to_remove, current_tuples)
            end
            
            if isempty(current_tuples)
                delete!(index.sorted_data, key)
            end
        end
    end
end