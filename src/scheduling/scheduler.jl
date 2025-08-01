mutable struct CircularSchedulerQueue{T}
    buffer::Vector{T}
    head::Int
    tail::Int
    size::Int
    capacity::Int
    
    function CircularSchedulerQueue{T}(initial_capacity::Int = 1000) where T
        buffer = Vector{T}(undef, initial_capacity)
        new{T}(buffer, 1, 1, 0, initial_capacity)
    end
end

@inline function enqueue!(q::CircularSchedulerQueue{T}, item::T) where T
    if q.size == q.capacity
        resize_queue!(q)
    end
    
    q.buffer[q.tail] = item
    q.tail = q.tail == q.capacity ? 1 : q.tail + 1
    q.size += 1
end

@inline function dequeue!(q::CircularSchedulerQueue{T}) where T
    if q.size == 0
        return nothing
    end
    
    item = q.buffer[q.head]
    q.head = q.head == q.capacity ? 1 : q.head + 1
    q.size -= 1
    return item
end

@inline function is_empty(q::CircularSchedulerQueue)
    return q.size == 0
end

function resize_queue!(q::CircularSchedulerQueue)
    old_size = q.size
    new_capacity = q.capacity * 2
    new_buffer = Vector{eltype(q.buffer)}(undef, new_capacity)
    
    for i in 1:old_size
        idx = (q.head + i - 2) % q.capacity + 1
        new_buffer[i] = q.buffer[idx]
    end
    
    q.buffer = new_buffer
    q.head = 1
    q.tail = old_size + 1
    q.capacity = new_capacity
end

mutable struct BatchScheduler <: Scheduler
    node_map::Dict{Any, AbstractNode}
    tuple_pool::TuplePool
    batch_size::Int
    pending_queue::CircularSchedulerQueue{AbstractTuple}
    
    function BatchScheduler(map::Dict{Any, AbstractNode}, pool::TuplePool, size::Int=100)
        initial_capacity = max(size * 4, 1000)
        queue = CircularSchedulerQueue{AbstractTuple}(initial_capacity)
        new(map, pool, size, queue)
    end
end

@inline function schedule(scheduler::BatchScheduler, tuple::AbstractTuple)
    enqueue!(scheduler.pending_queue, tuple)
end

@inline is_empty(scheduler::BatchScheduler) = is_empty(scheduler.pending_queue)

@inline function dequeue!(scheduler::BatchScheduler)
    return dequeue!(scheduler.pending_queue)
end

function process_batch!(scheduler::BatchScheduler, max_items::Int = scheduler.batch_size)
    processed = 0
    
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
            release!(scheduler.tuple_pool, tuple)
        end
        
        processed += 1
    end
    
    return processed
end

function get_queue_stats(scheduler::BatchScheduler)
    q = scheduler.pending_queue
    return (
        size = q.size,
        capacity = q.capacity,
        utilization = q.size / q.capacity,
        head = q.head,
        tail = q.tail
    )
end

function adaptive_batch_size(scheduler::BatchScheduler)
    stats = get_queue_stats(scheduler)
    
    if stats.utilization > 0.8
        return scheduler.batch_size * 2
    elseif stats.utilization < 0.2
        return scheduler.batch_size
    else
        return Int(ceil(scheduler.batch_size * 1.2))
    end
end

function drain_all!(scheduler::BatchScheduler)
    processed = 0
    while !is_empty(scheduler.pending_queue)
        processed += process_batch!(scheduler, 1000)
    end
    return processed
end