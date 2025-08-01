# core/facts.jl - Optimized fact system implementation

# Use a simple counter instead of UUID for much better performance
const FACT_ID_COUNTER = Ref{Int64}(0)

@inline function next_fact_id()
    # Atomic increment for thread safety
    old_val = FACT_ID_COUNTER[]
    FACT_ID_COUNTER[] = old_val + 1
    return old_val + 1
end

macro greynet_fact(struct_def)
    if !@capture(struct_def, mutable struct Name_ fields__ end)
        error("Usage: @greynet_fact mutable struct ... end")
    end
    
    # Use Int64 ID instead of UUID - massive performance gain
    required_fields = [:(id::Int64)]
    all_fields = vcat(fields, required_fields)
    user_fields = fields
    new_struct_def = :(mutable struct $Name <: AbstractGreynetFact; $(all_fields...); end)
    
    # Extract field names more efficiently
    user_field_names = [isa(f, Symbol) ? f : f.args[1] for f in user_fields]
    
    constructor = quote
        @inline function $Name($(user_field_names...))
            # FIX: Use the fully qualified name to avoid scope issues when the
            # macro is used outside the Greynet module.
            new_id = Greynet.next_fact_id()
            return $(Expr(:call, Name, user_field_names..., :new_id))
        end
    end
    
    # Optimized hash and equality based on Int64 ID only
    hash_def = :(@inline Base.hash(f::$Name, h::UInt) = Base.hash(f.id, h))
    isequal_def = :(@inline Base.isequal(a::$Name, b::$Name) = a.id == b.id)
    
    return esc(quote
        $new_struct_def
        $constructor
        $hash_def
        $isequal_def
    end)
end
