@inline is_dirty(state::TupleState) = state == CREATING || state == UPDATING || state == DYING

function get_facts_generic(t::AbstractTuple)
    facts = Any[]
    for field_name in fieldnames(typeof(t))
        if startswith(string(field_name), "fact_")
            fact = getfield(t, field_name)
            if fact !== nothing
                push!(facts, fact)
            end
        end
    end
    return facts
end

get_facts(t::AbstractTuple) = get_facts_generic(t)

@inline function get_facts_batch(tuples::Vector{T}) where T<:AbstractTuple
    result = Vector{NTuple{fieldcount(T)-2, Any}}(undef, length(tuples))
    @inbounds for i in 1:length(tuples)
        result[i] = get_facts(tuples[i])
    end
    return result
end