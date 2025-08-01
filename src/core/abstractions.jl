# Core abstract types that everything else depends on

abstract type AbstractGreynetFact end
abstract type AbstractTuple end
abstract type AbstractScore end
abstract type AbstractNode end
abstract type AbstractCollector end
abstract type StreamDefinition end
abstract type Scheduler end

# Specialized node types
abstract type BetaNode <: AbstractNode end

# Enums
@enum TupleState CREATING OK UPDATING DYING ABORTING DEAD
@enum JoinerType EQUAL LESS_THAN LESS_THAN_OR_EQUAL GREATER_THAN GREATER_THAN_OR_EQUAL NOT_EQUAL

# Default implementations for AbstractCollector that throw errors - subclasses must override
function insert!(collector::AbstractCollector, item) 
    error("insert! not implemented for $(typeof(collector))")
end

function result(collector::AbstractCollector)
    error("result not implemented for $(typeof(collector))")  
end

function is_empty(collector::AbstractCollector)
    error("is_empty not implemented for $(typeof(collector))")
end