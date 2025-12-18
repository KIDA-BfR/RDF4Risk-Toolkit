import logging
from rdflib import ConjunctiveGraph, URIRef

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Serialization Function ---
def serialize_rdf(graph: ConjunctiveGraph, rdf_format: str, named_graph_uri: str = None) -> str:
    """
    Serializes the graph to the specified format string.
    Handles named graphs explicitly for Turtle and RDF/XML serialization.
    N-Quads and JSON-LD handle ConjunctiveGraphs naturally.

    Args:
        graph: The rdflib.ConjunctiveGraph to serialize.
        rdf_format: The target serialization format (e.g., "turtle", "nquads", "json-ld", "pretty-xml").
        named_graph_uri: The URI of the specific named graph used during generation, if any.

    Returns:
        A string containing the serialized RDF data.

    Raises:
        ValueError: If an unsupported rdf_format is provided or serialization fails.
    """
    supported_formats = ["turtle", "nquads", "json-ld", "xml", "pretty-xml", "trig"]
    if rdf_format not in supported_formats:
        raise ValueError(f"Invalid RDF format for serialization: '{rdf_format}'. Supported: {supported_formats}")

    try:
        # --- Handling logic based on format and named graph presence ---
        if rdf_format == "trig":
            logging.info("Serializing conjunctive graph to TriG format.")
            return graph.serialize(format="trig", encoding='utf-8').decode('utf-8')

        if named_graph_uri:
            # A specific named graph was used during generation
            context = URIRef(named_graph_uri)
            named_graph_context = graph.get_context(context) # Get the specific graph context

            if len(named_graph_context) == 0:
                 logging.warning(f"Named graph '{named_graph_uri}' was specified but is empty or could not be found within the ConjunctiveGraph.")
                 # Decide behavior for empty graph: return empty string or serialize the empty context (which includes prefixes)
                 # return "" # Option 1: Return empty string
                 # We choose to serialize the (potentially empty) context graph to include prefixes if defined.

            if rdf_format == "turtle":
                # Serialize ONLY the named graph context for Turtle output.
                # The Turtle syntax itself won't list the graph URI per triple,
                # but the output contains *only* the triples from that graph.
                logging.info(f"Serializing named graph '{named_graph_uri}' to Turtle format.")
                serialized_data = named_graph_context.serialize(format="turtle", encoding='utf-8')
                return serialized_data.decode('utf-8') if isinstance(serialized_data, bytes) else serialized_data

            elif rdf_format == "nquads":
                # N-Quads handles conjunctive graphs correctly by default.
                # Serializing the whole ConjunctiveGraph is the correct approach.
                logging.info(f"Serializing conjunctive graph (including named graph '{named_graph_uri}') to N-Quads format.")
                return graph.serialize(format="nquads", encoding='utf-8').decode('utf-8')

            elif rdf_format == "json-ld":
                # JSON-LD serialization of ConjunctiveGraph can represent named graphs.
                logging.info(f"Serializing conjunctive graph (including named graph '{named_graph_uri}') to JSON-LD format.")
                serialized_data = graph.serialize(format="json-ld", indent=2, encoding='utf-8')
                return serialized_data.decode('utf-8') if isinstance(serialized_data, bytes) else serialized_data

            elif rdf_format in ["xml", "pretty-xml"]:
                # RDF/XML usually serializes the default graph. We adapt by
                # serializing ONLY the named graph context, similar to Turtle.
                logging.warning(f"RDF/XML format requested for named graph '{named_graph_uri}'. Serializing only the content of this specific graph. Standard RDF/XML has limited support for named graphs.")
                return named_graph_context.serialize(format="pretty-xml", encoding='utf-8').decode('utf-8')

            else: # Should not happen due to format check, but include for safety
                 logging.warning(f"Unhandled format '{rdf_format}' when a named graph is specified. Falling back to default serialization.")
                 return graph.serialize(format=rdf_format) # Fallback

        else:
            # No named graph was specified, serialize the whole graph (usually the default graph)
            logging.info(f"Serializing graph (default context) to {rdf_format} format.")
            
            # All formats should be explicitly encoded and decoded if they return bytes
            serialize_kwargs = {"format": rdf_format, "encoding": "utf-8"}
            if rdf_format == "json-ld":
                serialize_kwargs["indent"] = 2
            elif rdf_format == "pretty-xml":
                 serialize_kwargs["format"] = "pretty-xml" # Ensure pretty-xml is used
            
            serialized_data = graph.serialize(**serialize_kwargs)
            
            # Decode if the result is bytes, otherwise return as is
            return serialized_data.decode('utf-8') if isinstance(serialized_data, bytes) else serialized_data

    except Exception as e:
        # Keep existing robust error handling
        logging.error(f"Error during RDF serialization to format '{rdf_format}': {e}", exc_info=True)
        raise ValueError(f"Error during serialization to {rdf_format}: {e}") from e
