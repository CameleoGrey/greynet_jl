mutable struct ScoringNode{S<:AbstractScore} <: AbstractNode
    node_id::Int
    constraint_id::Int
    impact_function::Function
    score_class::DataType
    matches::Dict{AbstractTuple, Tuple{S, AbstractTuple}}
    child_nodes::Vector{AbstractNode}
    
    function ScoringNode(id, cid::Int, impact, s_class::Type{S}) where S<:AbstractScore
        new{S}(id, cid, impact, s_class, Dict{AbstractTuple, Tuple{S, AbstractTuple}}(), [])
    end
end

function insert(node::ScoringNode{S}, tuple::AbstractTuple) where S<:AbstractScore
    if isnothing(tuple)
        return
    end
    facts = get_facts(tuple)
    score_object = node.impact_function(facts...)::S
    node.matches[tuple] = (score_object, tuple)
end

function retract(node::ScoringNode, tuple::AbstractTuple)
    if !isnothing(tuple)
        delete!(node.matches, tuple)
    end
end

function get_total_score(node::ScoringNode{S}) where S<:AbstractScore
    return sum(match_data -> match_data[1], values(node.matches); init=null_score(S))
end

function recalculate_scores!(node::ScoringNode{S}) where S<:AbstractScore
    for (tuple, (old_score, tuple_ref)) in node.matches
        facts = get_facts(tuple)
        new_score = node.impact_function(facts...)::S
        node.matches[tuple] = (new_score, tuple_ref)
    end
end