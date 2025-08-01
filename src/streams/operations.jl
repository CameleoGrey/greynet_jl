struct Constraint
    stream::Stream
    score_type::Symbol
    penalty_function::Function
    constraint_id::Union{String, Nothing}
end

function Stream(factory, definition)
    Stream(factory, definition, get_target_arity(definition), [])
end

add_next_stream!(source::Stream, next::Stream) = push!(source.next_streams, next)

function from(factory::ConstraintFactory, fact_class::DataType)
    Stream(factory, FromDefinition(factory, fact_class))
end

function for_each_unique_pair(factory::ConstraintFactory, fact_class::DataType)
    base_stream = from(factory, fact_class)
    join(base_stream, base_stream, NOT_EQUAL, identity, identity)
end

function join(left::Stream, right::Stream, jt::JoinerType, lk::Function, rk::Function)
    new_stream = Stream(left.factory, JoinDefinition(left.factory, left, right, jt, lk, rk))
    add_next_stream!(left, new_stream)
    add_next_stream!(right, new_stream)
    return new_stream
end

function if_exists(left::Stream, right::Stream, lk::Function, rk::Function)
    new_stream = Stream(left.factory, ConditionalJoinDefinition(left.factory, left, right, true, lk, rk))
    add_next_stream!(left, new_stream)
    add_next_stream!(right, new_stream)
    return new_stream
end

function if_not_exists(left::Stream, right::Stream, lk::Function, rk::Function)
    new_stream = Stream(left.factory, ConditionalJoinDefinition(left.factory, left, right, false, lk, rk))
    add_next_stream!(left, new_stream)
    add_next_stream!(right, new_stream)
    return new_stream
end

function filter(source::Stream, predicate::Function)
    new_stream = Stream(source.factory, FilterDefinition(source.factory, source, predicate))
    add_next_stream!(source, new_stream)
    return new_stream
end

function group_by(source::Stream, key_func::Function, collector_supplier::Function)
    if source.arity != 1 
        error("group_by currently only supports streams with an arity of 1.") 
    end
    new_stream = Stream(source.factory, GroupByDefinition(source.factory, source, key_func, collector_supplier))
    add_next_stream!(source, new_stream)
    return new_stream
end

_create_penalty(s::Stream, st::Symbol, p) = Constraint(s, st, isa(p, Function) ? p : (facts...) -> p, nothing)
penalize_hard(s::Stream, p) = _create_penalty(s, :hard, p)
penalize_soft(s::Stream, p) = _create_penalty(s, :soft, p)
penalize_simple(s::Stream, p) = _create_penalty(s, :simple, p)

add_constraint!(factory::ConstraintFactory, def::Function) = push!(factory._constraint_defs, def)