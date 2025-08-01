# nodes/alpha_nodes.jl - Optimized alpha nodes

# FromNode with type stability improvements
mutable struct FromNode <: AbstractNode
    node_id::Int
    retrieval_id::DataType
    scheduler::Scheduler
    tuple_pool::TuplePool
    child_nodes::Vector{AbstractNode}
    
    @inline function FromNode(id::Int, rid::DataType, sched::Scheduler, pool::TuplePool)
        # Pre-allocate child_nodes with reasonable capacity
        children = sizehint!(Vector{AbstractNode}(), 4)
        new(id, rid, sched, pool, children)
    end
end

# FilterNode with optimized predicate evaluation
mutable struct FilterNode <: AbstractNode
    node_id::Int
    predicate::Function
    scheduler::Scheduler
    child_nodes::Vector{AbstractNode}
    
    @inline function FilterNode(id::Int, pred::Function, sched::Scheduler)
        children = sizehint!(Vector{AbstractNode}(), 4)
        new(id, pred, sched, children)
    end
end

# Highly optimized FromNode methods
@inline function insert(node::FromNode, fact::AbstractGreynetFact)
    if fact === nothing
        return nothing
    end
    
    # Direct tuple creation with concrete type
    tuple_ = acquire(node.tuple_pool, UniTuple{typeof(fact)}, fact)
    tuple_.node = node
    tuple_.state = CREATING
    schedule(node.scheduler, tuple_)
    return tuple_
end

@inline function retract(node::FromNode, tuple::AbstractTuple)
    if tuple === nothing
        return
    end
    
    if tuple.state == CREATING
        tuple.state = ABORTING
    elseif tuple.state == OK
        tuple.state = DYING
        schedule(node.scheduler, tuple)
    end
end

# Optimized FilterNode methods - avoid vector allocation in hot path
@inline function insert(node::FilterNode, tuple::AbstractTuple)
    if tuple === nothing
        return
    end
    
    # Evaluate predicate once and branch
    if node.predicate(tuple)
        # Direct iteration instead of function call overhead
        @inbounds for child in node.child_nodes
            insert(child, tuple)
        end
    end
end

@inline function retract(node::FilterNode, tuple::AbstractTuple)
    if tuple === nothing
        return
    end
    
    if node.predicate(tuple)
        @inbounds for child in node.child_nodes
            retract(child, tuple)
        end
    end
end