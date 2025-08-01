const ENABLE_POOL_STATS = false

mutable struct UniTuple{T<:AbstractGreynetFact} <: AbstractTuple
    fact_a::T
    node::Union{AbstractNode, Nothing}
    state::TupleState
    
    @inline function UniTuple{T}(fact::T) where T
        new(fact, nothing, CREATING)
    end
end

mutable struct BiTuple{T1, T2} <: AbstractTuple
    fact_a::T1
    fact_b::T2
    node::Union{AbstractNode, Nothing}
    state::TupleState
    
    @inline function BiTuple{T1,T2}(fact1::T1, fact2::T2) where {T1,T2}
        new(fact1, fact2, nothing, CREATING)
    end
end

mutable struct TriTuple{T1, T2, T3} <: AbstractTuple
    fact_a::T1
    fact_b::T2
    fact_c::T3
    node::Union{AbstractNode, Nothing}
    state::TupleState
    
    @inline function TriTuple{T1,T2,T3}(f1::T1, f2::T2, f3::T3) where {T1,T2,T3}
        new(f1, f2, f3, nothing, CREATING)
    end
end

mutable struct QuadTuple{T1, T2, T3, T4} <: AbstractTuple
    fact_a::T1
    fact_b::T2
    fact_c::T3
    fact_d::T4
    node::Union{AbstractNode, Nothing}
    state::TupleState
    
    @inline function QuadTuple{T1,T2,T3,T4}(f1::T1, f2::T2, f3::T3, f4::T4) where {T1,T2,T3,T4}
        new(f1, f2, f3, f4, nothing, CREATING)
    end
end

mutable struct PentaTuple{T1, T2, T3, T4, T5} <: AbstractTuple
    fact_a::T1
    fact_b::T2
    fact_c::T3
    fact_d::T4
    fact_e::T5
    node::Union{AbstractNode, Nothing}
    state::TupleState
    
    @inline function PentaTuple{T1,T2,T3,T4,T5}(f1::T1, f2::T2, f3::T3, f4::T4, f5::T5) where {T1,T2,T3,T4,T5}
        new(f1, f2, f3, f4, f5, nothing, CREATING)
    end
end

mutable struct TypedTuplePool
    uni_pools::Dict{DataType, Vector{Any}}
    bi_pools::Dict{DataType, Vector{Any}}
    tri_pools::Dict{DataType, Vector{Any}}
    quad_pools::Dict{DataType, Vector{Any}}
    penta_pools::Dict{DataType, Vector{Any}}
    
    acquisition_counts::Dict{DataType, Int}
    release_counts::Dict{DataType, Int}
    max_pool_sizes::Dict{DataType, Int}
    
    cache_hits::Int
    cache_misses::Int
    total_allocations::Int
    
    function TypedTuplePool()
        new(
            Dict{DataType, Vector{Any}}(),
            Dict{DataType, Vector{Any}}(),
            Dict{DataType, Vector{Any}}(),
            Dict{DataType, Vector{Any}}(),
            Dict{DataType, Vector{Any}}(),
            Dict{DataType, Int}(),
            Dict{DataType, Int}(),
            Dict{DataType, Int}(),
            0, 0, 0
        )
    end
end

mutable struct TuplePool
    typed_pool::TypedTuplePool
    
    TuplePool() = new(TypedTuplePool())
end

@inline function get_pool_for_arity(pool::TypedTuplePool, arity::Int)
    if arity == 1
        return pool.uni_pools
    elseif arity == 2
        return pool.bi_pools
    elseif arity == 3
        return pool.tri_pools
    elseif arity == 4
        return pool.quad_pools
    elseif arity == 5
        return pool.penta_pools
    else
        error("Unsupported tuple arity: $arity")
    end
end

@generated function acquire_typed(pool::TypedTuplePool, ::Type{TT}, facts...) where TT<:AbstractTuple
    arity = length(TT.parameters)
    
    field_assignments = [:(setfield!(instance, $(QuoteNode(Symbol(:fact_, 'a' + i - 1))), facts[$i])) 
                        for i in 1:arity]
    
    initial_size = arity <= 2 ? 100 : 50
    
    if ENABLE_POOL_STATS
        stats_update = quote
            pool.acquisition_counts[TT] = get(pool.acquisition_counts, TT, 0) + 1
        end
        cache_hit_update = :(pool.cache_hits += 1)
        cache_miss_update = quote
            pool.cache_misses += 1
            pool.total_allocations += 1
        end
    else
        stats_update = :()
        cache_hit_update = :()
        cache_miss_update = :()
    end
    
    quote
        pools = get_pool_for_arity(pool, $arity)
        if !haskey(pools, TT)
            pools[TT] = sizehint!(Vector{Any}(), $initial_size)
        end
        type_pool = pools[TT]
        
        $stats_update
        
        if !isempty(type_pool)
            instance = pop!(type_pool)::TT
            $(field_assignments...)
            instance.node = nothing
            instance.state = CREATING
            $cache_hit_update
            return instance
        else
            $cache_miss_update
            return TT(facts...)
        end
    end
end

@inline function acquire(pool::TuplePool, tuple_type::Type{UniTuple{T}}, fact::T) where T
    return acquire_typed(pool.typed_pool, tuple_type, fact)
end

@inline function acquire(pool::TuplePool, tuple_type::Type{BiTuple{T1,T2}}, fact1::T1, fact2::T2) where {T1,T2}
    return acquire_typed(pool.typed_pool, tuple_type, fact1, fact2)
end

@inline function acquire(pool::TuplePool, tuple_type::Type{TriTuple{T1,T2,T3}}, fact1::T1, fact2::T2, fact3::T3) where {T1,T2,T3}
    return acquire_typed(pool.typed_pool, tuple_type, fact1, fact2, fact3)
end

@inline function acquire(pool::TuplePool, tuple_type::Type{QuadTuple{T1,T2,T3,T4}}, fact1::T1, fact2::T2, fact3::T3, fact4::T4) where {T1,T2,T3,T4}
    return acquire_typed(pool.typed_pool, tuple_type, fact1, fact2, fact3, fact4)
end

@inline function acquire(pool::TuplePool, tuple_type::Type{PentaTuple{T1,T2,T3,T4,T5}}, fact1::T1, fact2::T2, fact3::T3, fact4::T4, fact5::T5) where {T1,T2,T3,T4,T5}
    return acquire_typed(pool.typed_pool, tuple_type, fact1, fact2, fact3, fact4, fact5)
end

@inline function release!(pool::TuplePool, tuple_instance::T) where T <: AbstractTuple
    typed_pool = pool.typed_pool
    tuple_type = typeof(tuple_instance)
    
    if ENABLE_POOL_STATS
        typed_pool.release_counts[tuple_type] = get(typed_pool.release_counts, tuple_type, 0) + 1
    end
    
    arity = length(tuple_type.parameters)
    if arity == 0
        return
    end
    pools = get_pool_for_arity(typed_pool, arity)
    
    type_pool = get!(pools, tuple_type) do
        sizehint!(Vector{Any}(), arity <= 2 ? 100 : 50)
    end
    
    max_size = get(typed_pool.max_pool_sizes, tuple_type, arity <= 2 ? 200 : 100)
    
    if length(type_pool) < max_size
        push!(type_pool, tuple_instance)
    end
end

function get_pool_stats(pool::Union{TuplePool, TypedTuplePool})
    typed_pool = isa(pool, TuplePool) ? pool.typed_pool : pool
    
    total_pooled = sum(sum(length, values(p)) for p in [
        typed_pool.uni_pools, typed_pool.bi_pools, typed_pool.tri_pools, 
        typed_pool.quad_pools, typed_pool.penta_pools
    ]; init=0)
    
    total_types = length(typed_pool.acquisition_counts)
    
    cache_hit_rate = typed_pool.cache_hits + typed_pool.cache_misses > 0 ? 
        typed_pool.cache_hits / (typed_pool.cache_hits + typed_pool.cache_misses) : 0.0
    
    return (
        total_pooled_instances = total_pooled,
        total_types_managed = total_types,
        cache_hits = typed_pool.cache_hits,
        cache_misses = typed_pool.cache_misses,
        cache_hit_rate = cache_hit_rate,
        total_allocations_on_miss = typed_pool.total_allocations
    )
end

function optimize_pool_sizes!(pool::Union{TuplePool, TypedTuplePool})
    typed_pool = isa(pool, TuplePool) ? pool.typed_pool : pool
    
    for (tuple_type, acquisitions) in typed_pool.acquisition_counts
        if acquisitions > 1000
            typed_pool.max_pool_sizes[tuple_type] = 500
        elseif acquisitions > 100
            typed_pool.max_pool_sizes[tuple_type] = 200
        else
            typed_pool.max_pool_sizes[tuple_type] = 50
        end
    end
end

function clear_pools!(pool::Union{TuplePool, TypedTuplePool})
    typed_pool = isa(pool, TuplePool) ? pool.typed_pool : pool
    
    for p in [typed_pool.uni_pools, typed_pool.bi_pools, typed_pool.tri_pools, typed_pool.quad_pools, typed_pool.penta_pools]
        empty!(p)
    end
    
    empty!(typed_pool.acquisition_counts)
    empty!(typed_pool.release_counts)
    empty!(typed_pool.max_pool_sizes)
    typed_pool.cache_hits = 0
    typed_pool.cache_misses = 0
    typed_pool.total_allocations = 0
end

@inline get_facts(t::UniTuple) = (t.fact_a,)
@inline get_facts(t::BiTuple) = (t.fact_a, t.fact_b)
@inline get_facts(t::TriTuple) = (t.fact_a, t.fact_b, t.fact_c)
@inline get_facts(t::QuadTuple) = (t.fact_a, t.fact_b, t.fact_c, t.fact_d)  
@inline get_facts(t::PentaTuple) = (t.fact_a, t.fact_b, t.fact_c, t.fact_d, t.fact_e)

function acquire_batch!(pool::TuplePool, requests::Vector{Tuple{DataType, Tuple}})
    results = Vector{AbstractTuple}(undef, length(requests))
    
    @inbounds for i in 1:length(requests)
        tuple_type, facts = requests[i]
        results[i] = acquire_typed(pool.typed_pool, tuple_type, facts...)
    end
    
    return results
end

function release_batch!(pool::TuplePool, tuples::Vector{<:AbstractTuple})
    @inbounds for tuple in tuples
        release!(pool, tuple)
    end
end