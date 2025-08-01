# nodes/group_nodes.jl
# Group nodes for aggregation operations

mutable struct GroupNode <: AbstractNode
    node_id::Int
    group_key_function::Function
    collector_supplier::Function
    scheduler::Scheduler
    tuple_pool::TuplePool
    group_map::Dict{Any, AbstractCollector}
    tuple_to_undo::Dict{AbstractTuple, Tuple{Any, Function}}
    group_key_to_tuple::Dict{Any, AbstractTuple}
    child_nodes::Vector{AbstractNode}
end

# Constructor with proper field initialization
function GroupNode(node_id::Int, group_key_function::Function, collector_supplier::Function, 
                   scheduler::Scheduler, tuple_pool::TuplePool)
    GroupNode(node_id, group_key_function, collector_supplier, scheduler, tuple_pool,
              Dict{Any, AbstractCollector}(), 
              Dict{AbstractTuple, Tuple{Any, Function}}(),
              Dict{Any, AbstractTuple}(),
              Vector{AbstractNode}())
end

function insert(node::GroupNode, tuple::AbstractTuple)
    if isnothing(tuple)
        return
    end
    
    facts = get_facts(tuple)
    if isempty(facts)
        error("GroupNode requires at least one fact in tuple")
    end
    # The first fact is assumed to be the one the collector operates on.
    # This is consistent with the group_by API only supporting arity-1 streams.
    fact_to_collect = facts[1] 
    
    group_key = node.group_key_function(facts...)
    
    # Get or create the collector for this group key
    collector = get!(node.collector_supplier, node.group_map, group_key)

    # Insert the item into the collector and store the undo function
    undo_function = insert!(collector, fact_to_collect)
    node.tuple_to_undo[tuple] = (group_key, undo_function)
    
    # Update or create the downstream tuple representing the aggregation result
    update_or_create_child(node, group_key, collector)
end

function retract(node::GroupNode, tuple::AbstractTuple)
    if isnothing(tuple) || !haskey(node.tuple_to_undo, tuple) 
        return 
    end
    
    # Retrieve and remove the undo information for this tuple
    group_key, undo_function = pop!(node.tuple_to_undo, tuple)
    undo_function() # Execute the retraction from the collector
    
    collector = get(node.group_map, group_key, nothing)
    if !isnothing(collector)
        if is_empty(collector)
            # If the collector is now empty, retract the result tuple and remove the group
            retract_child_by_key(node, group_key)
            delete!(node.group_map, group_key)
        else
            # Otherwise, update the result tuple with the new aggregation result
            update_or_create_child(node, group_key, collector)
        end
    end
end

# Helper methods for GroupNode
function update_or_create_child(node::GroupNode, group_key, collector)
    child_tuple = get(node.group_key_to_tuple, group_key, nothing)
    new_result = result(collector)
    
    if !isnothing(child_tuple)
        # If a result tuple already exists
        if child_tuple.fact_b == new_result 
            # If the result hasn't changed, do nothing.
            return 
        end
        # If the result has changed, retract the old result tuple first.
        retract_child_by_key(node, group_key)
    end
    
    # Create a new result tuple with the updated result.
    create_child(node, group_key, new_result)
end

function create_child(node::GroupNode, key, res)
    # Create a new BiTuple (key, result) and schedule it for insertion.
    tuple_ = acquire(node.tuple_pool, BiTuple{typeof(key), typeof(res)}, key, res)
    tuple_.node = node
    tuple_.state = CREATING
    node.group_key_to_tuple[key] = tuple_
    schedule(node.scheduler, tuple_)
end

function retract_child_by_key(node::GroupNode, key)
    if haskey(node.group_key_to_tuple, key)
        tuple_ = pop!(node.group_key_to_tuple, key)
        # Mark the tuple for retraction in the scheduler.
        if tuple_.state == CREATING 
            tuple_.state = ABORTING
        elseif !is_dirty(tuple_.state) 
            tuple_.state = DYING
            schedule(node.scheduler, tuple_)
        end
    end
end
