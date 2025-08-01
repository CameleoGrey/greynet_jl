# Scoring system implementation

struct SimpleScore <: AbstractScore
    value::Float64
end

struct HardSoftScore <: AbstractScore
    hard::Float64
    soft::Float64
end

# Score arithmetic operations
Base.:+(a::S, b::S) where {S<:AbstractScore} = S((getfield(a, f) + getfield(b, f) for f in fieldnames(S))...)

# Null score constructor
null_score(::Type{S}) where {S<:AbstractScore} = S((zero(fieldtype(S, f)) for f in fieldnames(S))...)

# --- FIX: Add comparison methods to enable sorting ---

# Define how to compare two SimpleScore objects by their value.
Base.isless(a::SimpleScore, b::SimpleScore) = isless(a.value, b.value)

# Define how to compare two HardSoftScore objects, prioritizing the 'hard' score.
Base.isless(a::HardSoftScore, b::HardSoftScore) = isless((a.hard, a.soft), (b.hard, b.soft))
