# src/Greynet.jl - Updated main module with optimizations

module Greynet

using MacroTools
using DataStructures  # Required for SortedDict in optimized advanced index

# --- EXPORTS ---
# Core types and macros
export AbstractGreynetFact, @greynet_fact, TupleState, AbstractTuple
export UniTuple, BiTuple, TriTuple, QuadTuple, PentaTuple
export TuplePool, TypedTuplePool, acquire, release!, acquire_typed
export insert_batch!, retract_batch!

# Scoring
export AbstractScore, SimpleScore, HardSoftScore, null_score

# Network Nodes
export AbstractNode, FromNode, FilterNode, ScoringNode, JoinNode, ConditionalNode, GroupNode
export NodeSharingManager, get_or_create_node
export Scheduler, BatchScheduler, CircularSchedulerQueue, schedule, fire_all
export JoinLeftAdapter, JoinRightAdapter, BetaNode

# Streams & Definitions
export Stream, StreamDefinition, FromDefinition, FilterDefinition, JoinDefinition, ConditionalJoinDefinition, GroupByDefinition
export from, filter, join, group_by, for_each_unique_pair, if_exists, if_not_exists
export JoinerType, IndexProperties, UniIndex, AdvancedIndex

# Collectors
export Collectors, AbstractCollector, insert!, result, is_empty

# Session & API
export ConstraintFactory, Constraint, ConstraintWeights, ScoringStreamDefinition
export Session, ConstraintBuilder, constraint, build, insert!, retract!, flush!, clear!, get_score, update_constraint_weight!, get_constraint_matches
export penalize_hard, penalize_soft, penalize_simple

# Utilities
export is_dirty, get_facts, dispose!
export intern, lookup

# NEW: Performance monitoring exports
export get_pool_stats, optimize_pool_sizes!, clear_pools!, get_queue_stats, get_index_stats
export fire_all_with_stats, fire_all_with_memory_monitoring, adaptive_batch_size

export Customer, Transaction, SecurityAlert

# --- CORRECTED INCLUDE ORDER ---

# 1. Core Abstractions & Enums (No dependencies)
include("core/abstractions.jl")

# 2. Core Data Structures (Depend only on abstractions)
include("core/scores.jl")
include("core/facts.jl")
include("core/tuples.jl")  # Now includes optimized TypedTuplePool

# 3. Utilities (Mostly self-contained)
include("utils/constants.jl")
include("utils/string_interner.jl")
include("utils/helpers.jl")
# Bring interner functions into Greynet's scope
using .StringInterner

# 4. Indexing (Depends on core tuples and utils)
include("indexing/properties.jl")
include("indexing/uni_index.jl")
include("indexing/advanced_index.jl")  # Now uses SortedDict for O(log n) operations

# 5. Collectors (Depends on abstractions)
include("collectors/base.jl")
include("collectors/implementations.jl")
using .Collectors

# 6. Scheduler (Depends on core tuples) - Now includes CircularSchedulerQueue
include("scheduling/scheduler.jl")

# 7. Node Definitions (Depend on everything above)
include("nodes/base.jl")
include("nodes/sharing.jl")
include("nodes/alpha_nodes.jl")
include("nodes/beta_nodes.jl")  # Now includes reverse indexing and workspace optimization
include("nodes/scoring_nodes.jl")
include("nodes/group_nodes.jl")

# 8. Stream Definitions (The "blueprints", depend on nodes and collectors)
include("streams/definitions.jl")
include("streams/operations.jl")

# 9. Session Weights (Crucial dependency for builders and session)
include("session/weights.jl")

# 10. Stream Builders (Depends on streams, nodes, and weights)
include("streams/builders.jl")

# 11. Scheduler Execution (The "engine", depends on scheduler and nodes) - Now optimized
include("scheduling/execution.jl")

# 12. Session (The high-level state, depends on almost everything)
include("session/session.jl")

# 13. API (The user-facing functions, depends on session and builders)
include("session/api.jl")


@greynet_fact mutable struct Customer
    customer_id::Int
    risk_level::String  # 'low', 'medium', 'high'
    status::String      # 'active', 'inactive'
end

@greynet_fact mutable struct Transaction
    transaction_id::Int
    customer_id::Int
    amount::Float64
    location::String
end

@greynet_fact mutable struct SecurityAlert
    location::String
    severity::Int    # 1 to 5
end

end # module Greynet