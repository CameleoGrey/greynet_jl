# nodes/base.jl - Optimized base node functionality

@inline function add_child_node!(parent::AbstractNode, child::AbstractNode)
    # Use findfirst for safety, but this should be rare in hot paths
    if findfirst(x -> x === child, parent.child_nodes) === nothing
        push!(parent.child_nodes, child)
    end
end

@inline function remove_child_node!(parent::AbstractNode, child::AbstractNode)
    # Use more efficient removal
    idx = findfirst(x -> x === child, parent.child_nodes)
    if idx !== nothing
        deleteat!(parent.child_nodes, idx)
    end
end

# Optimized downstream propagation - single tuple instead of vector
@inline function calculate_downstream(node::AbstractNode, tuple::AbstractTuple)
    @inbounds for child in node.child_nodes
        insert(child, tuple)
    end
end

@inline function retract_downstream(node::AbstractNode, tuple::AbstractTuple)
    @inbounds for child in node.child_nodes
        retract(child, tuple)
    end
end

# Keep the vector versions for compatibility but mark them as less efficient
function calculate_downstream(node::AbstractNode, tuples::Vector{<:AbstractTuple})
    @inbounds for tuple in tuples
        for child in node.child_nodes
            insert(child, tuple)
        end
    end
end

function retract_downstream(node::AbstractNode, tuples::Vector{<:AbstractTuple})
    @inbounds for tuple in tuples
        for child in node.child_nodes
            retract(child, tuple)
        end
    end
end

# Default implementations that error - nodes must implement their own
function insert(node::AbstractNode, tuple::AbstractTuple)
    error("insert not implemented for $(typeof(node))")
end

function retract(node::AbstractNode, tuple::AbstractTuple)
    error("retract not implemented for $(typeof(node))")
end

# BetaNode methods (kept as error-throwing to enforce adapter usage)
insert(::BetaNode, ::AbstractTuple) = error("BetaNode requires directional insert via an adapter.")
retract(::BetaNode, ::AbstractTuple) = error("BetaNode requires directional retract via an adapter.")