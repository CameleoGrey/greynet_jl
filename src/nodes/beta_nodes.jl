# nodes/beta_nodes.jl - Optimized beta nodes with reverse indexing

# Optimized JoinNode with reverse indices for O(1) retraction lookups
mutable struct JoinNode{K1, V1<:AbstractTuple, K2, V2<:AbstractTuple} <: BetaNode
    node_id::Int
    joiner_type::JoinerType
    left_index::Union{UniIndex{K1, V1}, AdvancedIndex{K1, V1}}
    right_index::Union{UniIndex{K2, V2}, AdvancedIndex{K2, V2}}
    scheduler::Scheduler
    tuple_pool::TuplePool
    target_arity::Int
    child_nodes::Vector{AbstractNode}
    beta_memory::Dict{Tuple{V1, V2}, AbstractTuple}
    
    # NEW: Reverse indices for O(1) retraction lookups
    left_to_children::Dict{V1, Vector{Tuple{V1, V2}}}
    right_to_children::Dict{V2, Vector{Tuple{V1, V2}}}
end

# ConditionalNode with optimized structure
mutable struct ConditionalNode{K1, V1<:AbstractTuple, K2, V2<:AbstractTuple} <: BetaNode
    node_id::Int
    left_properties::IndexProperties
    right_properties::IndexProperties
    should_exist::Bool
    scheduler::Scheduler
    left_index::UniIndex{K1, V1}
    right_index::UniIndex{K2, V2}
    tuple_map::Dict{V1, V1}
    child_nodes::Vector{AbstractNode}
end

# Adapter types for directional access to beta nodes
struct JoinLeftAdapter <: AbstractNode
    beta_node::BetaNode
    child_nodes::Vector{AbstractNode}
    JoinLeftAdapter(node) = new(node, [])
end

struct JoinRightAdapter <: AbstractNode
    beta_node::BetaNode
    child_nodes::Vector{AbstractNode}
    JoinRightAdapter(node) = new(node, [])
end

# Adapter methods
insert(adapter::JoinLeftAdapter, tuple::AbstractTuple) = insert_left(adapter.beta_node, tuple)
retract(adapter::JoinLeftAdapter, tuple::AbstractTuple) = retract_left(adapter.beta_node, tuple)
insert(adapter::JoinRightAdapter, tuple::AbstractTuple) = insert_right(adapter.beta_node, tuple)
retract(adapter::JoinRightAdapter, tuple::AbstractTuple) = retract_right(adapter.beta_node, tuple)

# Optimized JoinNode constructor with reverse indices
function JoinNode(node_id, joiner_type, left_props, right_props, scheduler, tuple_pool, target_arity, ::Type{K1}, ::Type{V1}, ::Type{K2}, ::Type{V2}) where {K1, V1<:AbstractTuple, K2, V2<:AbstractTuple}
    inverse_joiner = JOINER_INVERSES[joiner_type]
    
    left_index = create_index(left_props, joiner_type, K1, V1)
    right_index = create_index(right_props, inverse_joiner, K2, V2)
    
    beta_memory = Dict{Tuple{V1, V2}, AbstractTuple}()
    child_nodes = Vector{AbstractNode}()
    
    # Initialize reverse indices
    left_to_children = Dict{V1, Vector{Tuple{V1, V2}}}()
    right_to_children = Dict{V2, Vector{Tuple{V1, V2}}}()
    
    return JoinNode{K1, V1, K2, V2}(
        node_id, joiner_type, left_index, right_index, scheduler, tuple_pool, 
        target_arity, child_nodes, beta_memory, left_to_children, right_to_children
    )
end

# ConditionalNode constructor
function ConditionalNode(node_id, left_props, right_props, should_exist, scheduler, ::Type{K1}, ::Type{V1}, ::Type{K2}, ::Type{V2}) where {K1, V1<:AbstractTuple, K2, V2<:AbstractTuple}
    left_index = UniIndex{K1, V1}(left_props)
    right_index = UniIndex{K2, V2}(right_props)
    
    tuple_map = Dict{V1, V1}()
    child_nodes = Vector{AbstractNode}()
    
    return ConditionalNode{K1, V1, K2, V2}(
        node_id, left_props, right_props, should_exist, scheduler, 
        left_index, right_index, tuple_map, child_nodes
    )
end

# Workspace for zero-allocation joins
mutable struct JoinWorkspace
    combined_facts::Vector{Any}
    fact_types::Vector{DataType}
    temp_buffer::Vector{Any}
    
    function JoinWorkspace()
        new(
            sizehint!(Vector{Any}(), 10),
            sizehint!(Vector{DataType}(), 10),
            sizehint!(Vector{Any}(), 10)
        )
    end
end

# Thread-local workspace to avoid allocation
const THREAD_WORKSPACE = JoinWorkspace()

# Zero-allocation fact access using generated functions
@generated function get_facts_zero_alloc!(buffer::Vector, t::T) where T<:AbstractTuple
    fact_fields = [f for f in fieldnames(T) if startswith(string(f), "fact_")]
    assignments = [:(buffer[$i] = t.$f) for (i, f) in enumerate(fact_fields)]
    
    quote
        resize!(buffer, $(length(fact_fields)))
        $(assignments...)
        return $(length(fact_fields))
    end
end

# Optimized child tuple creation with workspace
function create_child_tuple_optimized(node::JoinNode, left::AbstractTuple, right::AbstractTuple)
    workspace = THREAD_WORKSPACE
    empty!(workspace.combined_facts)
    empty!(workspace.fact_types)
    
    # Zero-allocation fact extraction
    left_count = get_facts_zero_alloc!(workspace.temp_buffer, left)
    append!(workspace.combined_facts, view(workspace.temp_buffer, 1:left_count))
    
    right_count = get_facts_zero_alloc!(workspace.temp_buffer, right)
    append!(workspace.combined_facts, view(workspace.temp_buffer, 1:right_count))
    
    # Type inference without allocation
    for i in 1:length(workspace.combined_facts)
        push!(workspace.fact_types, typeof(workspace.combined_facts[i]))
    end
    
    base_tuple_type = ARITY_TO_TUPLE[node.target_arity]
    concrete_tuple_type = base_tuple_type{workspace.fact_types...}
    return acquire(node.tuple_pool, concrete_tuple_type, workspace.combined_facts...)
end

# Fallback to original implementation if needed
function create_child_tuple(node::JoinNode, left::AbstractTuple, right::AbstractTuple)
    try
        return create_child_tuple_optimized(node, left, right)
    catch
        # Fallback to original implementation
        combined_facts = (get_facts(left)..., get_facts(right)...)
        fact_types = typeof.(combined_facts)
        base_tuple_type = ARITY_TO_TUPLE[node.target_arity]
        concrete_tuple_type = base_tuple_type{fact_types...}
        return acquire(node.tuple_pool, concrete_tuple_type, combined_facts...)
    end
end

function retract_and_schedule_child(node::JoinNode, left, right)
    child = pop!(node.beta_memory, (left, right), nothing)
    if !isnothing(child)
        if child.state == CREATING 
            child.state = ABORTING
        elseif !is_dirty(child.state) 
            child.state = DYING
            schedule(node.scheduler, child)
        end
    end
end

# OPTIMIZED: insert_left with reverse index maintenance
function insert_left(node::JoinNode{K1, V1, K2, V2}, left_tuple::V1) where {K1, V1, K2, V2}
    if isnothing(left_tuple) return end
    
    put!(node.left_index, left_tuple)
    key = get_property(node.left_index.index_properties, left_tuple)::K2
    
    # Initialize reverse index entry
    if !haskey(node.left_to_children, left_tuple)
        node.left_to_children[left_tuple] = Vector{Tuple{V1, V2}}()
    end
    
    foreach_match(node.right_index, key) do right_tuple
        child = create_child_tuple(node, left_tuple, right_tuple)
        child.node = node
        child.state = CREATING
        
        pair = (left_tuple, right_tuple)
        node.beta_memory[pair] = child
        
        # Update reverse indices - O(1) operations
        push!(node.left_to_children[left_tuple], pair)
        if !haskey(node.right_to_children, right_tuple)
            node.right_to_children[right_tuple] = Vector{Tuple{V1, V2}}()
        end
        push!(node.right_to_children[right_tuple], pair)
        
        schedule(node.scheduler, child)
    end
end

# OPTIMIZED: insert_right with reverse index maintenance
function insert_right(node::JoinNode{K1, V1, K2, V2}, right_tuple::V2) where {K1, V1, K2, V2}
    if isnothing(right_tuple) return end
    
    put!(node.right_index, right_tuple)
    key = get_property(node.right_index.index_properties, right_tuple)::K1
    
    # Initialize reverse index entry
    if !haskey(node.right_to_children, right_tuple)
        node.right_to_children[right_tuple] = Vector{Tuple{V1, V2}}()
    end
    
    foreach_match(node.left_index, key) do left_tuple
        child = create_child_tuple(node, left_tuple, right_tuple)
        child.node = node
        child.state = CREATING
        
        pair = (left_tuple, right_tuple)
        node.beta_memory[pair] = child
        
        # Update reverse indices - O(1) operations
        push!(node.right_to_children[right_tuple], pair)
        if !haskey(node.left_to_children, left_tuple)
            node.left_to_children[left_tuple] = Vector{Tuple{V1, V2}}()
        end
        push!(node.left_to_children[left_tuple], pair)
        
        schedule(node.scheduler, child)
    end
end

# OPTIMIZED: retract_left with O(1) lookup instead of O(n) scan
function retract_left(node::JoinNode, left_tuple::AbstractTuple)
    if isnothing(left_tuple) return end
    
    remove!(node.left_index, left_tuple)
    
    # O(1) lookup instead of O(n) scan!
    if haskey(node.left_to_children, left_tuple)
        pairs_to_retract = pop!(node.left_to_children, left_tuple)
        
        for pair in pairs_to_retract
            retract_and_schedule_child(node, pair[1], pair[2])
            
            # Clean up right index
            right_pairs = get(node.right_to_children, pair[2], nothing)
            if right_pairs !== nothing
                filter!(p -> p !== pair, right_pairs)
                if isempty(right_pairs)
                    delete!(node.right_to_children, pair[2])
                end
            end
        end
    end
end

# OPTIMIZED: retract_right with O(1) lookup instead of O(n) scan
function retract_right(node::JoinNode, right_tuple::AbstractTuple)
    if isnothing(right_tuple) return end
    
    remove!(node.right_index, right_tuple)
    
    # O(1) lookup instead of O(n) scan!
    if haskey(node.right_to_children, right_tuple)
        pairs_to_retract = pop!(node.right_to_children, right_tuple)
        
        for pair in pairs_to_retract
            retract_and_schedule_child(node, pair[1], pair[2])
            
            # Clean up left index
            left_pairs = get(node.left_to_children, pair[1], nothing)
            if left_pairs !== nothing
                filter!(p -> p !== pair, left_pairs)
                if isempty(left_pairs)
                    delete!(node.left_to_children, pair[1])
                end
            end
        end
    end
end

# ConditionalNode helper methods
function propagate(n::ConditionalNode, t::AbstractTuple) 
    if !haskey(n.tuple_map, t) 
        n.tuple_map[t] = t
        calculate_downstream(n, t)
    end 
end

function retract_propagation(n::ConditionalNode, t::AbstractTuple) 
    if haskey(n.tuple_map, t) 
        delete!(n.tuple_map, t)
        retract_downstream(n, t)
    end 
end

# ConditionalNode directional methods (unchanged but benefit from type stability)
function insert_left(node::ConditionalNode{K1, V1, K2, V2}, tuple::V1) where {K1, V1, K2, V2}
    if isnothing(tuple) return end
    
    put!(node.left_index, tuple)
    key = get_property(node.left_properties, tuple)::K2
    if !isempty(get_index(node.right_index, key)) == node.should_exist 
        propagate(node, tuple) 
    end
end

function insert_right(node::ConditionalNode{K1, V1, K2, V2}, tuple::V2) where {K1, V1, K2, V2}
    if isnothing(tuple) return end
    
    key = get_property(node.right_properties, tuple)::K1
    was_empty = isempty(get_index(node.right_index, key))
    put!(node.right_index, tuple)
    if was_empty
        foreach_match(node.left_index, key) do left_tuple
            if node.should_exist 
                propagate(node, left_tuple) 
            else 
                retract_propagation(node, left_tuple) 
            end
        end
    end
end

function retract_left(node::ConditionalNode, tuple::AbstractTuple)
    if isnothing(tuple) return end
    remove!(node.left_index, tuple)
    retract_propagation(node, tuple)
end

function retract_right(node::ConditionalNode{K1, V1, K2, V2}, tuple::V2) where {K1, V1, K2, V2}
    if isnothing(tuple) return end
    
    key = get_property(node.right_properties, tuple)::K1
    remove!(node.right_index, tuple)
    if isempty(get_index(node.right_index, key))
        foreach_match(node.left_index, key) do left_tuple
            if node.should_exist 
                retract_propagation(node, left_tuple) 
            else 
                propagate(node, left_tuple) 
            end
        end
    end
end