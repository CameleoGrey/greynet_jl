# session/session.jl
# Session type and core session operations

mutable struct Session
    from_nodes::Dict{DataType, FromNode}
    scoring_nodes::Vector{ScoringNode}
    scheduler::BatchScheduler
    score_class::Type{<:AbstractScore}
    tuple_pool::TuplePool
    weights::ConstraintWeights
    fact_id_to_tuple::Dict{Int64, AbstractTuple}
    # MODIFIED: The map from constraint ID to node now uses an Int key.
    _scoring_node_map::Dict{Int, ScoringNode}
end

# MODIFIED: Session constructor updated for the Int-keyed map.
function Session(from_nodes, scoring_nodes, scheduler, score_class, tuple_pool, weights)
    Session(from_nodes, scoring_nodes, scheduler, score_class, tuple_pool, weights, Dict(), Dict(n.constraint_id => n for n in scoring_nodes))
end

function insert_batch!(s::Session, facts)
    for fact in facts
        if isnothing(fact)
            continue
        end
        fact_type = typeof(fact)
        if !haskey(s.from_nodes, fact_type) || haskey(s.fact_id_to_tuple, fact.id)
            continue
        end
        s.fact_id_to_tuple[fact.id] = insert(s.from_nodes[fact_type], fact)
    end
end

function retract_batch!(s::Session, facts)
    for fact in facts
        if isnothing(fact)
            continue
        end
        tuple_ = pop!(s.fact_id_to_tuple, fact.id, nothing)
        if !isnothing(tuple_)
            retract(tuple_.node, tuple_)
        end
    end
end

function clear!(s::Session)
    foreach(t -> retract(t.node, t), values(s.fact_id_to_tuple))
    empty!(s.fact_id_to_tuple)
    flush!(s)
end

# MODIFIED: The public API takes a string, interns it, and then operates on the ID.
function update_constraint_weight!(s::Session, id_str::String, weight::Float64)
    id = intern(id_str)
    if !haskey(s._scoring_node_map, id)
        error("No constraint found with ID: '$id_str'")
    end
    set_weight!(s.weights, id, weight)
    # Recalculate scores for the affected node.
    recalculate_scores!(s._scoring_node_map[id])
end

function dispose!(session::Session)
    clear!(session)
    empty!(session.from_nodes)
    empty!(session.scoring_nodes)
    empty!(session._scoring_node_map)
end
