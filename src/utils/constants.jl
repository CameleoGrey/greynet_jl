# Constants and mappings used throughout the system

# Mapping from arity to tuple type
const ARITY_TO_TUPLE = Dict(
    1 => UniTuple,
    2 => BiTuple, 
    3 => TriTuple,
    4 => QuadTuple,
    5 => PentaTuple
)

# Joiner type inverses for indexing
const JOINER_INVERSES = Dict(
    EQUAL => EQUAL, 
    NOT_EQUAL => NOT_EQUAL, 
    LESS_THAN => GREATER_THAN, 
    LESS_THAN_OR_EQUAL => GREATER_THAN_OR_EQUAL, 
    GREATER_THAN => LESS_THAN, 
    GREATER_THAN_OR_EQUAL => LESS_THAN_OR_EQUAL
)