mutable struct ConstraintFactory
    name::String
    score_class::Type{<:AbstractScore}
    _constraint_defs::Vector{Function}
    tuple_pool::TuplePool
    node_sharer::NodeSharingManager
    constraint_id_cache::Dict{String, Int}
    
    function ConstraintFactory(name, score_class)
        new(name, score_class, [], TuplePool(), NodeSharingManager(), Dict{String, Int}())
    end
end

mutable struct Stream
    factory
    definition::StreamDefinition
    arity::Int
    next_streams::Vector{Stream}
end

mutable struct FromDefinition <: StreamDefinition
    factory::ConstraintFactory
    fact_class::DataType
    retrieval_id::Tuple{String, DataType}
    FromDefinition(fact, f_class) = new(fact, f_class, ("from", f_class))
end

mutable struct FilterDefinition <: StreamDefinition
    factory::ConstraintFactory
    source_stream::Stream
    predicate::Function
    retrieval_id::Tuple
end

mutable struct JoinDefinition <: StreamDefinition
    factory::ConstraintFactory
    left_stream::Stream
    right_stream::Stream
    joiner_type::JoinerType
    left_key_func::Function
    right_key_func::Function
    retrieval_id::Tuple
end

mutable struct ConditionalJoinDefinition <: StreamDefinition
    factory::ConstraintFactory
    left_stream::Stream
    right_stream::Stream
    should_exist::Bool
    left_key_func::Function
    right_key_func::Function
    retrieval_id::Tuple
end

mutable struct GroupByDefinition <: StreamDefinition
    factory::ConstraintFactory
    source_stream::Stream
    group_key_function::Function
    collector_supplier::Function
    retrieval_id::Tuple
end

mutable struct ScoringStreamDefinition <: StreamDefinition
    factory::ConstraintFactory
    source_stream::Stream
    constraint_id::Int
    impact_function::Function
    retrieval_id::Tuple
end

FilterDefinition(fact, src, pred) = FilterDefinition(fact, src, pred, ("filter", src.definition.retrieval_id, pred))
JoinDefinition(fact, l, r, j, lk, rk) = JoinDefinition(fact, l, r, j, lk, rk, ("join", l.definition.retrieval_id, r.definition.retrieval_id, j, lk, rk))
ConditionalJoinDefinition(fact, l, r, should, lk, rk) = ConditionalJoinDefinition(fact, l, r, should, lk, rk, ("cond_join", l.definition.retrieval_id, r.definition.retrieval_id, should, lk, rk))
GroupByDefinition(fact, src, kf, cs) = GroupByDefinition(fact, src, kf, cs, ("group_by", src.definition.retrieval_id, kf, cs))
ScoringStreamDefinition(fact, src, id::Int, impact) = ScoringStreamDefinition(fact, src, id, impact, ("score", id))

get_target_arity(::FromDefinition) = 1
get_target_arity(d::FilterDefinition) = d.source_stream.arity
get_target_arity(d::JoinDefinition) = d.left_stream.arity + d.right_stream.arity
get_target_arity(d::ConditionalJoinDefinition) = d.left_stream.arity
get_target_arity(::GroupByDefinition) = 2
get_target_arity(::ScoringStreamDefinition) = 0

function get_cached_constraint_id(factory::ConstraintFactory, id_str::String)
    return get!(factory.constraint_id_cache, id_str) do
        intern(id_str)
    end
end