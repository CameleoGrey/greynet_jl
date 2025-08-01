# Abstract collector interface - implementation already in core/abstractions.jl
# This file is included for organizational completeness but functionality 
# is defined in the abstractions file to avoid circular dependencies

# The AbstractCollector type and its default methods are defined in core/abstractions.jl:
# - insert!(collector::AbstractCollector, item) 
# - result(collector::AbstractCollector)
# - is_empty(collector::AbstractCollector)