function fire_all(scheduler::BatchScheduler)
    while !is_empty(scheduler)
        batch_size = adaptive_batch_size(scheduler)
        processed = process_batch!(scheduler, batch_size)
        
        if processed == 0
            break
        end
    end
end

@inline function process_single_tuple!(scheduler::BatchScheduler, tuple::AbstractTuple)
    node = tuple.node
    state = tuple.state
    
    if state == CREATING
        calculate_downstream(node, tuple)
        tuple.state = OK
    elseif state == UPDATING
        retract_downstream(node, tuple)
        calculate_downstream(node, tuple)
        tuple.state = OK
    elseif state == DYING
        retract_downstream(node, tuple)
        tuple.state = DEAD
    elseif state == ABORTING
        tuple.state = DEAD
    end
    
    if tuple.state == DEAD
        release!(scheduler.tuple_pool, tuple)
    end
end

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

function fire_all_with_stats(scheduler::BatchScheduler)
    start_time = time()
    initial_queue_size = get_queue_stats(scheduler).size
    total_processed = 0
    
    while !is_empty(scheduler)
        batch_size = adaptive_batch_size(scheduler)
        processed = process_batch!(scheduler, batch_size)
        total_processed += processed
        
        if processed == 0
            break
        end
    end
    
    end_time = time()
    
    return (
        initial_queue_size = initial_queue_size,
        total_processed = total_processed,
        duration_seconds = end_time - start_time,
        tuples_per_second = total_processed / max(end_time - start_time, 0.001),
        final_queue_size = get_queue_stats(scheduler).size
    )
end

function fire_all_with_memory_monitoring(scheduler::BatchScheduler)
    initial_memory = Base.gc_live_bytes()
    
    fire_all(scheduler)
    
    GC.gc()
    
    final_memory = Base.gc_live_bytes()
    memory_change = final_memory - initial_memory
    
    return (
        initial_memory_mb = initial_memory / (1024^2),
        final_memory_mb = final_memory / (1024^2),
        memory_change_mb = memory_change / (1024^2),
        memory_freed = memory_change < 0
    )
end

function fire_progressive(scheduler::BatchScheduler, yield_interval::Int = 100)
    processed_since_yield = 0
    total_processed = 0
    
    while !is_empty(scheduler)
        tuple = dequeue!(scheduler)
        if tuple === nothing
            break
        end
        
        process_single_tuple!(scheduler, tuple)
        processed_since_yield += 1
        total_processed += 1
        
        if processed_since_yield >= yield_interval
            yield()
            processed_since_yield = 0
        end
    end
    
    return total_processed
end

function process_batch_optimized!(scheduler::BatchScheduler, max_items::Int = scheduler.batch_size)
    processed = 0
    dead_tuples = Vector{AbstractTuple}()
    sizehint!(dead_tuples, max_items ÷ 4)
    
    while processed < max_items && !is_empty(scheduler.pending_queue)
        tuple = dequeue!(scheduler.pending_queue)
        if tuple === nothing
            break
        end
        
        node = tuple.node
        state = tuple.state
        
        if state == CREATING
            calculate_downstream(node, tuple)
            tuple.state = OK
        elseif state == UPDATING
            retract_downstream(node, tuple)
            calculate_downstream(node, tuple)
            tuple.state = OK
        elseif state == DYING
            retract_downstream(node, tuple)
            tuple.state = DEAD
        elseif state == ABORTING
            tuple.state = DEAD
        end
        
        if tuple.state == DEAD
            push!(dead_tuples, tuple)
        end
        
        processed += 1
    end
    
    release_batch!(scheduler.tuple_pool, dead_tuples)
    return processed
end