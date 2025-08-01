mutable struct ConstraintBuilder
    factory::ConstraintFactory
    weights::ConstraintWeights
    score_class::Type{<:AbstractScore}
    function ConstraintBuilder(name="default"; score_class=SimpleScore, weights=ConstraintWeights())
        new(ConstraintFactory(name, score_class), weights, score_class)
    end
end

function insert!(s::Session, fact::AbstractGreynetFact)
    insert_batch!(s, [fact])
    flush!(s)
end

function retract!(s::Session, fact::AbstractGreynetFact)
    retract_batch!(s, [fact])
    flush!(s)
end

flush!(s::Session) = fire_all(s.scheduler)

function get_score(s::Session)
    flush!(s)
    return sum(get_total_score, s.scoring_nodes; init=null_score(s.score_class))
end

function get_constraint_matches(s::Session)
    flush!(s)
    return Dict(lookup(node.constraint_id) => collect(values(node.matches)) for node in s.scoring_nodes if !isempty(node.matches))
end

from(b::ConstraintBuilder, fact_class) = from(b.factory, fact_class)
for_each_unique_pair(b::ConstraintBuilder, fact_class) = for_each_unique_pair(b.factory, fact_class)

function constraint(func::Function, builder::ConstraintBuilder, id_str::String, default_weight::Float64=1.0)
    id = get_cached_constraint_id(builder.factory, id_str)
    set_weight!(builder.weights, id, default_weight)

    constraint_def = () -> begin
        constraint_obj = func(builder)
        if !isa(constraint_obj, Constraint)
            error("Function for constraint '$id_str' must end with a penalize call.")
        end
        target_field = constraint_obj.score_type == :simple ? :value : constraint_obj.score_type
        if !hasfield(builder.score_class, target_field)
            error("Score type '$(constraint_obj.score_type)' invalid for score class '$(builder.score_class)'.")
        end

        impact_function = (facts...) -> begin
            base_penalty = constraint_obj.penalty_function(facts...)
            final_penalty = Float64(base_penalty) * get_weight(builder.weights, id)
            score_args = (f == target_field ? final_penalty : 0.0 for f in fieldnames(builder.score_class))
            return builder.score_class(score_args...)
        end
        return ScoringStreamDefinition(builder.factory, constraint_obj.stream, id, impact_function)
    end
    add_constraint!(builder.factory, constraint_def)
end

build(builder::ConstraintBuilder; batch_size=100) = build_session(builder.factory; weights=builder.weights, batch_size=batch_size)