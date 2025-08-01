# utils/string_interner.jl
# A global singleton for interning strings to integer IDs for performance.
# Optimized for a single-threaded environment.

module StringInterner

export intern, lookup

# In a single-threaded context, we don't need a lock.
# This struct holds the mappings.
struct Interner
    str_to_id::Dict{String, Int}
    id_to_str::Vector{String}
end

# The single global instance of the interner.
const GLOBAL_INTERNER = Interner(Dict{String, Int}(), Vector{String}())

"""
    intern(s::String)::Int

Returns a unique integer ID for the given string. If the string is new,
it's added to the interner; otherwise, the existing ID is returned.
"""
function intern(s::String)::Int
    # get! is an efficient way to get an existing key or create it if absent.
    # No lock is needed in a single-threaded model.
    get!(GLOBAL_INTERNER.str_to_id, s) do
        push!(GLOBAL_INTERNER.id_to_str, s)
        # The new ID is the index of the string in the vector.
        return length(GLOBAL_INTERNER.id_to_str)
    end
end

"""
    lookup(id::Int)::String

Returns the string corresponding to the given integer ID.
"""
function lookup(id::Int)::String
    # Direct array lookup is very fast and doesn't require a lock.
    return GLOBAL_INTERNER.id_to_str[id]
end

end # module StringInterner
