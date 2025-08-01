# session/weights.jl
# Constraint weights management

mutable struct ConstraintWeights
    # MODIFIED: The dictionary now maps an integer ID to the weight.
    _weights::Dict{Int, Float64}
    ConstraintWeights() = new(Dict{Int, Float64}())
end

# MODIFIED: id is now an Int.
function set_weight!(w::ConstraintWeights, id::Int, weight::Float64)
    if isnan(weight) || isinf(weight)
        error("Weight must be finite, got $weight")
    end
    w._weights[id] = weight
end

# MODIFIED: id is now an Int.
get_weight(w::ConstraintWeights, id::Int) = get(w._weights, id, 1.0)
