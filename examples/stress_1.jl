using Random
using Printf
using Statistics

# Import the Greynet library
include("../src/Greynet.jl")
using .Greynet
import .Greynet: join
import .Greynet: EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL

# --- Data Definitions ---

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

# --- Constraint Definitions ---

function define_constraints!(builder::ConstraintBuilder)
    """
    Defines a set of rules to stress test various engine capabilities.
    """
    
    # Constraint 1: Simple filter for high-value transactions
    constraint(builder, "high_value_transaction") do b
        stream = from(b, Transaction) |> 
                 s -> Greynet.filter(s, tx -> tx.amount > 45000.0)
        return penalize_simple(stream, tx -> tx.amount / 1000.0)
    end

    # Constraint 2: Group transactions by customer and check for excessive activity
    constraint(builder, "excessive_transactions_per_customer") do b
        stream = from(b, Transaction) |>
                 s -> group_by(s, tx -> tx.customer_id, Collectors.count_collector()) |>
                 s -> Greynet.filter(s, (cid, count) -> count > 25)
        return penalize_simple(stream, (cid, count) -> (count - 25) * 10.0)
    end

    # Constraint 3: Join transactions with security alerts on location
    constraint(builder, "transaction_in_alerted_location") do b
        stream = join(from(b, Transaction), 
                     from(b, SecurityAlert),
                     EQUAL,
                     tx -> tx.location,
                     alert -> alert.location)
        return penalize_simple(stream, (tx, alert) -> 100.0 * alert.severity)
    end

    # Constraint 4: Join to find transactions from inactive customers
    constraint(builder, "inactive_customer_transaction") do b
        customers_stream = from(b, Customer) |>
                          s -> Greynet.filter(s, c -> c.status == "inactive")
        stream = join(customers_stream,
                     from(b, Transaction),
                     EQUAL,
                     c -> c.customer_id,
                     tx -> tx.customer_id)
        return penalize_simple(stream, (c, tx) -> 500.0)
    end

    # Constraint 5: Complex rule using if_not_exists
    # Penalize if a high-risk customer has a transaction in a location
    # that does NOT have a security alert
    constraint(builder, "high_risk_transaction_without_alert") do b
        high_risk_customers = from(b, Customer) |>
                             s -> Greynet.filter(s, c -> c.risk_level == "high")
        customer_transactions = join(high_risk_customers,
                                   from(b, Transaction),
                                   EQUAL,
                                   c -> c.customer_id,
                                   tx -> tx.customer_id)
        stream = if_not_exists(customer_transactions,
                              from(b, SecurityAlert),
                              (c, tx) -> tx.location,
                              alert -> alert.location)
        return penalize_simple(stream, (c, tx) -> 1000.0)
    end
end

# --- Optimized Data Generation ---

function generate_data(num_customers::Int, num_transactions::Int, num_locations::Int)
    """Generates optimized dataset with pre-allocation and efficient random generation."""
    println("Generating test data...")
    
    # Pre-allocate location strings once
    locations = Vector{String}(undef, num_locations)
    @inbounds for i in 1:num_locations
        locations[i] = string("location_", i)
    end
    
    # Pre-defined risk levels and statuses as tuples for better performance
    risk_levels = ("low", "medium", "high")
    statuses = ("active", "inactive")
    
    # Pre-allocate and fill customers vector
    customers = Vector{Customer}(undef, num_customers)
    @inbounds for i in 1:num_customers
        customers[i] = Customer(
            i,
            risk_levels[rand(1:3)],
            rand() < 0.95 ? "active" : "inactive"  # 95% active, 5% inactive
        )
    end
    
    # Pre-allocate and fill transactions vector
    transactions = Vector{Transaction}(undef, num_transactions)
    @inbounds for i in 1:num_transactions
        transactions[i] = Transaction(
            i,
            rand(1:num_customers),
            rand() * 49999.0 + 1.0,  # 1.0 to 50000.0
            locations[rand(1:num_locations)]
        )
    end
    
    # Optimized alert generation - avoid shuffle, use direct indexing
    num_alerted = max(1, num_locations ÷ 4)
    alert_indices = Set{Int}()
    while length(alert_indices) < num_alerted
        push!(alert_indices, rand(1:num_locations))
    end
    
    alerts = Vector{SecurityAlert}(undef, num_alerted)
    @inbounds for (i, idx) in enumerate(alert_indices)
        alerts[i] = SecurityAlert(locations[idx], rand(1:5))
    end
    
    return (customers=customers, transactions=transactions, alerts=alerts)
end

# --- JIT Warmup ---

function warmup_jit()
    """Runs a smaller version of the test to warm up the JIT compiler."""
    println("Warming up JIT compiler...")
    
    # Create a small builder and session for warmup
    builder = ConstraintBuilder("warmup", score_class=SimpleScore)
    define_constraints!(builder)
    session = build(builder, batch_size=50)  # Smaller batch for warmup
    
    # Generate small dataset
    small_data = generate_data(50, 100, 10)
    all_facts = vcat(small_data.customers, small_data.transactions, small_data.alerts)
    
    # Process the data
    insert_batch!(session, all_facts)
    score = get_score(session)
    matches = get_constraint_matches(session)
    
    # Clean up
    dispose!(session)
    
    println("JIT warmup completed. Score: $score, Total matches: $(sum(length(v) for v in values(matches)))")
end

# --- Performance Measurement Utilities ---

@inline function measure_memory()
    """Optimized memory measurement."""
    GC.gc()  # Force garbage collection
    return Base.gc_live_bytes()
end

# --- Main Test Runner ---

function main()
    """Main function to run the optimized stress test."""
    
    # --- Optimized Configuration ---
    NUM_CUSTOMERS = 10_000
    NUM_TRANSACTIONS = 10_000_000
    NUM_LOCATIONS = 1_000
    
    println("### Starting Optimized Rule Engine Stress Test (Julia) ###")
    
    # 0. JIT Warmup Phase
    warmup_jit()
    println()
    
    # 1. Setup Phase & Initial State
    mem_start = measure_memory()
    
    time_start_setup = time()
    builder = ConstraintBuilder("stress-test-session", score_class=SimpleScore)
    define_constraints!(builder)
    # Optimized batch size for better performance/memory balance
    session = build(builder, batch_size=200)
    time_end_setup = time()
    
    # 2. Data Generation Phase
    time_start_data = time()
    data = generate_data(NUM_CUSTOMERS, NUM_TRANSACTIONS, NUM_LOCATIONS)
    all_facts = vcat(data.customers, data.transactions, data.alerts)
    time_end_data = time()

    # 3. Processing Phase
    println("Inserting facts and processing rules...")
    mem_before_processing = measure_memory()
    
    # Force one GC before timing to get clean measurement
    GC.gc()
    
    time_start_processing = time()
    insert_batch!(session, all_facts)
    final_score = get_score(session)
    matches = get_constraint_matches(session)
    time_end_processing = time()
    
    # 4. Get Memory Snapshot
    mem_end = measure_memory()
    
    # 5. Reporting
    println("\n--- Optimized Stress Test Results ---")
    
    # Time Metrics
    setup_duration = time_end_setup - time_start_setup
    data_gen_duration = time_end_data - time_start_data
    processing_duration = time_end_processing - time_start_processing
    total_duration = time_end_processing - time_start_setup

    # Performance Metrics
    total_facts = length(all_facts)
    facts_per_second = processing_duration > 0 ? total_facts / processing_duration : Inf

    # Memory Metrics (convert from bytes to MB)
    memory_used = (mem_end - mem_start) / (1024^2)
    processing_memory = (mem_end - mem_before_processing) / (1024^2)

    # Display Report
    println("\n#### Performance Summary")
    println("| Metric                         | Value               |")
    println("|--------------------------------|---------------------|")
    @printf("| Total Facts Processed          | %s         |\n", format_number(total_facts))
    @printf("| Setup Time (Build Network)     | %.4f s      |\n", setup_duration)
    @printf("| Data Generation Time           | %.4f s      |\n", data_gen_duration)
    @printf("| **Processing Time (Insert+Flush)** | **%.4f s** |\n", processing_duration)
    @printf("| Total Time                     | %.4f s      |\n", total_duration)
    @printf("| **Throughput** | **%s facts/sec** |\n", format_number(facts_per_second, 2))

    println("\n#### Memory Usage Summary")
    println("| Metric                         | Value               |")
    println("|--------------------------------|---------------------|")
    @printf("| Total Memory Used              | %.2f MB        |\n", memory_used)
    @printf("| **Processing Memory** | **%.2f MB** |\n", processing_memory)
    @printf("| Memory per Fact                | %.2f bytes     |\n", (processing_memory * 1024 * 1024) / total_facts)

    println("\n#### Engine Output")
    println("- **Final Score:** $final_score")
    total_matches = sum(length(v) for v in values(matches))
    println("- **Total Constraint Matches:** $total_matches")
    
    for (constraint_id, match_list) in sort(collect(matches))
        println("  - `$constraint_id`: $(length(match_list)) matches")
    end
    
    # Cleanup
    #dispose!(session)
    
    println("\n### Optimized Stress Test Completed ###")
end

# --- Utility Functions ---

@inline function format_number(num::Real, decimals::Int=0)
    """Optimized number formatting with commas."""
    if decimals == 0
        # Fast path for integers
        str = string(round(Int, num))
    else
        # Handle decimals
        str = string(round(num, digits=decimals))
        if decimals > 0 && !occursin('.', str)
            str = str * "." * "0"^decimals
        elseif decimals > 0
            decimal_part = split(str, '.')[2]
            if length(decimal_part) < decimals
                str = str * "0"^(decimals - length(decimal_part))
            end
        end
    end
    
    # Add commas to integer part
    parts = split(str, '.')
    # FIX: Explicitly call Base.join to avoid conflict with Greynet.join
    integer_part = reverse(Base.join([reverse(parts[1])[i:min(i+2, end)] for i in 1:3:length(parts[1])], ","))
    
    return length(parts) > 1 ? integer_part * "." * parts[2] : integer_part
end

# --- Entry Point ---

if abspath(PROGRAM_FILE) == @__FILE__
    # Run the optimized stress test
    main()
end
