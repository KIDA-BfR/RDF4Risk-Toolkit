# template_processor.py
import logging
from rdflib import BNode, URIRef, Literal
from rdflib.namespace import RDF, XSD, RDFS

# Import helpers from data_preprocessor
from .data_preprocessor import (
    is_valid_uri_simple, 
    strip_angle_brackets, 
    safe_value, 
    extract_label
)

def apply_template_properties(
    graph_target, 
    current_subject_node, 
    template_properties, 
    row_data, 
    df_columns, 
    all_templates, 
    pred_mappings, 
    obj_mappings, 
    defined_predicates, 
    graph_ns, 
    ex_ns, 
    config, 
    _create_object_node_func,
    iri_column: str,
    string_column: str
):
    """
    Recursively applies properties from a schema template to a subject node.
    Handles nested templates.
    """
    for prop_template in template_properties:
        pred_uri_str = prop_template.get("predicate")
        map_type = prop_template.get("map_type")
        map_value_config = prop_template.get("value")

        if not pred_uri_str or not is_valid_uri_simple(pred_uri_str):
            logging.warning(f"Invalid or missing predicate URI '{pred_uri_str}' in template. Skipping property.")
            continue
        
        pred_uri = URIRef(pred_uri_str)
        
        is_defined = any(pred_uri == p_uri for p_uri in defined_predicates.values())
        if not is_defined:
            graph_target.add((pred_uri, RDF.type, RDF.Property))
            label_text = extract_label(pred_uri_str)
            
            if iri_column in pred_mappings.columns and string_column in pred_mappings.columns:
                match_row = pred_mappings[pred_mappings[iri_column].astype(str) == pred_uri_str]
                if not match_row.empty:
                    label_text = safe_value(match_row.iloc[0][string_column])

            graph_target.add((pred_uri, RDFS.label, Literal(label_text, datatype=XSD.string)))
            defined_predicates[pred_uri_str] = pred_uri

        obj_node = None
        if map_type == "Column Value (Literal)" or map_type == "Column Value (URI)":
            if map_value_config not in df_columns:
                logging.warning(f"Column '{map_value_config}' for predicate '{pred_uri_str}' not in data. Skipping.")
                continue
            raw_cell_value = safe_value(row_data.get(map_value_config))
            if raw_cell_value is None:
                continue

            if map_type == "Column Value (Literal)":
                obj_node = _create_object_node_func(raw_cell_value, map_value_config, obj_mappings, ex_ns, graph_target, config)
            else:
                cleaned_uri_val = strip_angle_brackets(raw_cell_value)
                if is_valid_uri_simple(cleaned_uri_val):
                    obj_node = URIRef(cleaned_uri_val)
                else:
                    logging.warning(f"Value '{raw_cell_value}' from column '{map_value_config}' for predicate '{pred_uri_str}' is not a valid URI. Skipping.")
                    continue
        
        elif map_type == "Fixed URI":
            if not is_valid_uri_simple(map_value_config):
                logging.warning(f"Fixed URI value '{map_value_config}' for predicate '{pred_uri_str}' is invalid. Skipping.")
                continue
            obj_node = URIRef(map_value_config)

        elif map_type == "Nested Template":
            nested_template_name = map_value_config
            nested_template_def = next((t_def for t_def in all_templates if t_def.get("template_name") == nested_template_name), None)
            
            if not nested_template_def:
                logging.warning(f"Nested template '{nested_template_name}' for predicate '{pred_uri_str}' not found. Skipping.")
                continue

            b_node_nested = BNode()
            graph_target.add((current_subject_node, pred_uri, b_node_nested))
            
            nested_rdf_type = nested_template_def.get("rdf_type")
            if nested_rdf_type and is_valid_uri_simple(nested_rdf_type):
                graph_target.add((b_node_nested, RDF.type, URIRef(nested_rdf_type)))
            
            apply_template_properties(
                graph_target=graph_target,
                current_subject_node=b_node_nested,
                template_properties=nested_template_def.get("properties", []),
                row_data=row_data,
                df_columns=df_columns,
                all_templates=all_templates,
                pred_mappings=pred_mappings,
                obj_mappings=obj_mappings,
                defined_predicates=defined_predicates,
                graph_ns=graph_ns,
                ex_ns=ex_ns,
                config=config,
                _create_object_node_func=_create_object_node_func,
                iri_column=iri_column,
                string_column=string_column
            )
            continue

        else:
            logging.warning(f"Unknown map_type '{map_type}' for predicate '{pred_uri_str}'. Skipping.")
            continue

        if obj_node is not None:
            graph_target.add((current_subject_node, pred_uri, obj_node))
