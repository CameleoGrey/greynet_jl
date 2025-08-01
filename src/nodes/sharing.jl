# Node sharing manager for reusing equivalent nodes

mutable struct NodeSharingManager
    alpha_nodes::Dict{Any, AbstractNode}
    beta_nodes::Dict{Any, AbstractNode}
    group_nodes::Dict{Any, AbstractNode}
    temporal_nodes::Dict{Any, AbstractNode}
    NodeSharingManager() = new(Dict(), Dict(), Dict(), Dict())
end

function get_or_create_node(manager::NodeSharingManager, retrieval_id::Any, node_map_symbol::Symbol, node_supplier::Function)
    node_map = getfield(manager, node_map_symbol)
    node = get(node_map, retrieval_id, nothing)
    if isnothing(node)
        node = node_supplier()
        node_map[retrieval_id] = node
    end
    return node
end