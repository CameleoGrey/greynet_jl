function get_output_fact_types(d::FromDefinition)
    return (d.fact_class,)
end

function get_output_fact_types(d::FilterDefinition)
    return get_output_fact_types(d.source_stream.definition)
end

function get_output_fact_types(d::JoinDefinition)
    left_types = get_output_fact_types(d.left_stream.definition)
    right_types = get_output_fact_types(d.right_stream.definition)
    return (left_types..., right_types...)
end

function get_output_fact_types(d::ConditionalJoinDefinition)
    return get_output_fact_types(d.left_stream.definition)
end

function get_output_fact_types(d::GroupByDefinition)
    # The result is a BiTuple of (Key, CollectorResult).
    # Statically inferring the collector result type is complex.
    # Defaulting to Any is a safe and pragmatic compromise.
    return (Any, Any)
end

function get_output_tuple_type(d::StreamDefinition)
    fact_types = get_output_fact_types(d)
    arity = length(fact_types)
    if arity == 0 || !haskey(ARITY_TO_TUPLE, arity)
        # For terminal nodes like ScoringNode which don't output tuples.
        return AbstractTuple
    end
    base_tuple_type = ARITY_TO_TUPLE[arity]
    
    # FIX: The check that returned a non-parameterized type has been removed.
    # This function now ALWAYS returns a concrete, parameterized tuple type,
    # such as `BiTuple{Any, Any}` if the underlying fact types are not known.
    # This provides the necessary type stability for the compiler.
    return base_tuple_type{fact_types...}
end


# --- Node Building Logic ---

function build_node(d::FromDefinition, nc, nm, sched)
    get(nm, d.retrieval_id) do
        node_id = nc[]
        nc[] += 1
        node = FromNode(node_id, d.fact_class, sched, d.factory.tuple_pool)
        nm[d.retrieval_id] = node
        node
    end
end

function build_node(d::FilterDefinition, nc, nm, sched)
    node = get_or_create_node(d.factory.node_sharer, d.retrieval_id, :alpha_nodes, () -> begin
        node_id = nc[]
        nc[] += 1
        FilterNode(node_id, t -> d.predicate(get_facts(t)...), sched)
    end)
    if d.retrieval_id ∉ keys(nm)
        parent_node = build_node(d.source_stream.definition, nc, nm, sched)
        add_child_node!(parent_node, node)
        nm[d.retrieval_id] = node
    end
    return node
end

# --- MODIFIED: build_node for JoinDefinition ---
function build_node(d::JoinDefinition, nc, nm, sched)
    node = get_or_create_node(d.factory.node_sharer, d.retrieval_id, :beta_nodes, () -> begin
        target_arity = get_target_arity(d)
        if target_arity > 5 
            error("Joining would result in an arity greater than 5.") 
        end
        node_id = nc[]
        nc[] += 1
        left_props = IndexProperties(d.left_key_func)
        right_props = IndexProperties(d.right_key_func)
        
        # --- PERFORMANCE ENHANCEMENT: Infer Key and Value types ---
        V1 = get_output_tuple_type(d.left_stream.definition)
        V2 = get_output_tuple_type(d.right_stream.definition)
        
        left_fact_types = get_output_fact_types(d.left_stream.definition)
        right_fact_types = get_output_fact_types(d.right_stream.definition)

        # Use Base.return_types for inference. This is a huge performance win.
        K1 = first(Base.return_types(d.left_key_func, left_fact_types))
        K2 = first(Base.return_types(d.right_key_func, right_fact_types))
        
        # Call the new, fully type-stable JoinNode constructor
        JoinNode(node_id, d.joiner_type, left_props, right_props, sched, d.factory.tuple_pool, target_arity, K1, V1, K2, V2)
    end)
    if d.retrieval_id ∉ keys(nm)
        left_node = build_node(d.left_stream.definition, nc, nm, sched)
        right_node = build_node(d.right_stream.definition, nc, nm, sched)
        add_child_node!(left_node, JoinLeftAdapter(node))
        add_child_node!(right_node, JoinRightAdapter(node))
        nm[d.retrieval_id] = node
    end
    return node
end

# --- MODIFIED: build_node for ConditionalJoinDefinition ---
function build_node(d::ConditionalJoinDefinition, nc, nm, sched)
    node = get_or_create_node(d.factory.node_sharer, d.retrieval_id, :beta_nodes, () -> begin
        node_id = nc[]
        nc[] += 1
        left_props = IndexProperties(d.left_key_func)
        right_props = IndexProperties(d.right_key_func)
        
        # --- PERFORMANCE ENHANCEMENT: Infer Key and Value types ---
        V1 = get_output_tuple_type(d.left_stream.definition)
        V2 = get_output_tuple_type(d.right_stream.definition)
        
        left_fact_types = get_output_fact_types(d.left_stream.definition)
        right_fact_types = get_output_fact_types(d.right_stream.definition)

        K1 = first(Base.return_types(d.left_key_func, left_fact_types))
        K2 = first(Base.return_types(d.right_key_func, right_fact_types))

        # Call the new, fully type-stable ConditionalNode constructor
        ConditionalNode(node_id, left_props, right_props, d.should_exist, sched, K1, V1, K2, V2)
    end)
    if d.retrieval_id ∉ keys(nm)
        left_node = build_node(d.left_stream.definition, nc, nm, sched)
        right_node = build_node(d.right_stream.definition, nc, nm, sched)
        add_child_node!(left_node, JoinLeftAdapter(node))
        add_child_node!(right_node, JoinRightAdapter(node))
        nm[d.retrieval_id] = node
    end
    return node
end

function build_node(d::GroupByDefinition, nc, nm, sched)
    node = get_or_create_node(d.factory.node_sharer, d.retrieval_id, :group_nodes, () -> begin
        node_id = nc[]
        nc[] += 1
        GroupNode(node_id, d.group_key_function, d.collector_supplier, sched, d.factory.tuple_pool)
    end)
    if d.retrieval_id ∉ keys(nm)
        parent_node = build_node(d.source_stream.definition, nc, nm, sched)
        add_child_node!(parent_node, node)
        nm[d.retrieval_id] = node
    end
    return node
end

function build_node(d::ScoringStreamDefinition, nc, nm, sched)
    get(nm, d.retrieval_id) do
        source_node = build_node(d.source_stream.definition, nc, nm, sched)
        node_id = nc[]
        nc[] += 1
        node = ScoringNode(node_id, d.constraint_id, d.impact_function, d.factory.score_class)
        add_child_node!(source_node, node)
        nm[d.retrieval_id] = node
        node
    end
end

function build_session(factory::ConstraintFactory; weights::ConstraintWeights, batch_size=100)
    node_counter = Ref(0)
    session_node_map = Dict{Any, AbstractNode}()
    scheduler = BatchScheduler(session_node_map, factory.tuple_pool, batch_size)
    
    for def in factory._constraint_defs
        build_node(def(), node_counter, session_node_map, scheduler)
    end
    
    from_nodes = Dict{DataType, FromNode}(n.retrieval_id => n for n in values(session_node_map) if isa(n, FromNode))
    scoring_nodes = [n for n in values(session_node_map) if isa(n, ScoringNode)]
    
    return Session(from_nodes, scoring_nodes, scheduler, factory.score_class, factory.tuple_pool, weights)
end
