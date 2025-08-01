# indexing/properties.jl
# Index properties for tuple indexing

struct IndexProperties
    property_retriever::Function
end

get_property(props::IndexProperties, obj::AbstractTuple) = props.property_retriever(get_facts(obj)...)