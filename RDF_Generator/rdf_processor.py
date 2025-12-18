# rdf_processor.py
import pandas as pd
import re
import os
from rdflib import ConjunctiveGraph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, XSD, RDFS, SKOS
from urllib.parse import urlparse
from dateutil.parser import parse as date_parse
import logging
from .data_preprocessor import (
    clean_string_for_uri, safe_value, guess_xsd_datatype,
    is_valid_uri, is_valid_uri_simple, strip_angle_brackets,
    extract_label, _extract_full_uris_from_mappings, _extract_all_uris_from_data
)
from .template_processor import apply_template_properties
from .uri_utils import map_custom_uri_to_standard_uri, extract_prefixes, find_relevant_prefixes

# --- Configuration ---
# Keep existing logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Main RDF Generation Function ---

def _initialize_graph_with_namespaces(mapping_df, df, iri_column, instance_class_uri, subject_uri_base, named_graph_uri, config):
    g = ConjunctiveGraph()

    # 1. Extract all unique full URIs from the mapping table AND the data
    mapping_uris = _extract_full_uris_from_mappings(mapping_df, iri_column)
    data_uris = _extract_all_uris_from_data(df)
    all_uris = mapping_uris.union(data_uris)

    # Add other important URIs that might not be in the mapping table
    if instance_class_uri and is_valid_uri(instance_class_uri):
        all_uris.add(instance_class_uri)
    if subject_uri_base and is_valid_uri(subject_uri_base):
        all_uris.add(subject_uri_base)

    # 2. Load all available prefixes
    prefix_file_path = os.path.join(os.path.dirname(__file__), 'utils', 'Prefixes', 'prefixes.txt')
    all_prefixes = extract_prefixes(prefix_file_path)

    # --- Manual Prefix Injection ---
    manual_prefixes = config.get('manual_prefixes', {})
    for ns, prefixes in manual_prefixes.items():
        if ns not in all_prefixes:
            all_prefixes[ns] = []
        for prefix in prefixes:
            if prefix not in all_prefixes[ns]:
                all_prefixes[ns].append(prefix)

    # 3. Find prefixes relevant to the current mapping URIs
    relevant_prefixes = {}
    if all_prefixes:
        relevant_prefixes = find_relevant_prefixes(all_prefixes, all_uris)

    # 4. Bind the relevant prefixes to the graph
    for prefix, namespace in relevant_prefixes.items():
        g.bind(prefix, Namespace(namespace))

    # --- Namespace Setup (including fallbacks and defaults) ---
    default_ex_ns_str =  'https://fskx-graphdb.risk-ai-cloud.com/'
    EX = Namespace(default_ex_ns_str)
    g.bind("ex", EX) # Default example namespace

    if named_graph_uri:
        try:
            parsed_ng = urlparse(named_graph_uri)
            # Extract base URL and hash from named_graph_uri
            base_url = f"{parsed_ng.scheme}://{parsed_ng.netloc}"
            # Extract hash from path (assuming it's the last part)
            path_parts = [p for p in parsed_ng.path.split('/') if p]
            hash_part = path_parts[-1] if path_parts else "default"
            # Construct: https://base/dataset/hash/
            base_path = f"{base_url}/{hash_part}/"
            DATASET_Default = Namespace(base_path)
        except Exception:
            DATASET_Default = Namespace(f"{named_graph_uri}")
    else:
        DATASET_Default = Namespace(str(EX) + "")
    g.bind("dataset", DATASET_Default) # Default graph namespace

    # Bind standard, commonly used prefixes if they weren't found by the dynamic loader
    if "rdfs" not in g.namespace_manager: g.bind("rdfs", RDFS)
    if "rdf" not in g.namespace_manager: g.bind("rdf", RDF)
    if "xsd" not in g.namespace_manager: g.bind("xsd", XSD)
    if "skos" not in g.namespace_manager: g.bind("skos", SKOS)

    return g, EX, DATASET_Default

def _prepare_mappings(mapping_df, string_column, iri_column, rdf_role_column):
    logging.info(f"Mapping table columns: {mapping_df.columns.tolist()}")
    mapping_df.columns = [col.strip() for col in mapping_df.columns]

    # Fallback for renamed 'URI' column and update the caller's variable
    if iri_column not in mapping_df.columns and 'Mapped ID' in mapping_df.columns:
        logging.info("Found 'Mapped ID' instead of 'URI'. Adjusting iri_column.")
        iri_column = 'Mapped ID'

    if not all(col in mapping_df.columns for col in [string_column, iri_column, rdf_role_column]):
        raise ValueError(f"Mapping table is missing required columns. Needed: {string_column}, {iri_column}, {rdf_role_column}.")

    # Convert relevant columns to string type to ensure '.str' accessor works
    mapping_df[rdf_role_column] = mapping_df[rdf_role_column].astype(str)
    mapping_df[string_column] = mapping_df[string_column].astype(str)

    pred_mappings = mapping_df[mapping_df[rdf_role_column].str.strip().str.lower() == "predicate"].copy()
    obj_mappings = mapping_df[mapping_df[rdf_role_column].str.strip().str.lower() == "object"].copy()

    pred_mappings['_norm_term'] = pred_mappings[string_column].str.strip().str.lower()
    obj_mappings['_norm_term'] = obj_mappings[string_column].str.strip().str.lower()

    # Return the potentially updated iri_column name
    return pred_mappings, obj_mappings, iri_column

def _define_predicates(df, pred_mappings, id_column, subject_column, column_to_group_info, iri_column, graph_target, config, term_to_concept_uri_map=None):
    """
    Define predicates for data columns.
    CRITICAL FIX: Now uses term_to_concept_uri_map to ensure internal concept URIs are used
    instead of external matched URIs, enabling proper round-trip conversion.
    """
    defined_predicates = {}
    default_namespace = config.get('default_namespace', 'https://fskx-graphdb.risk-ai-cloud.com/')

    for col in df.columns:
        if col == id_column or (subject_column and col == subject_column) or col in column_to_group_info:
            continue

        pred_uri = None

        # CRITICAL FIX: Check term_to_concept_uri_map FIRST (Problem 3 fix)
        # This ensures we use internal concept URIs instead of external matched URIs
        if term_to_concept_uri_map and col in term_to_concept_uri_map:
            pred_uri = term_to_concept_uri_map[col]
            logging.debug(f"Column '{col}': Using internal concept URI from map: {pred_uri}")
        else:
            # Fallback: If not in concept map, try mapping table or generate new URI
            col_norm = str(col).strip().lower()
            match_row = pred_mappings[pred_mappings['_norm_term'] == col_norm]

            if not match_row.empty:
                raw_mapping_uri = strip_angle_brackets(safe_value(match_row.iloc[0][iri_column]))
                if raw_mapping_uri and is_valid_uri(raw_mapping_uri):
                    # Note: If term_to_concept_uri_map exists but column not in it,
                    # we still use external URI as fallback (backwards compatibility)
                    pred_uri = URIRef(map_custom_uri_to_standard_uri(raw_mapping_uri, default_namespace))
                    logging.debug(f"Column '{col}': Using URI from mapping table: {pred_uri}")

            if not pred_uri:
                # Generate internal URI if no mapping found
                col_cleaned = clean_string_for_uri(col, config.get('uri_character_replacements'))
                if col_cleaned:
                    pred_uri = URIRef(default_namespace + "concepts/" + col_cleaned)
                    logging.debug(f"Column '{col}': Generated new internal URI: {pred_uri}")
                else:
                    logging.warning(f"Cannot generate predicate URI for column '{col}'. Skipping.")
                    continue

        # Add predicate metadata (type and label) only once per unique URI
        if pred_uri not in defined_predicates.values():
            graph_target.add((pred_uri, RDF.type, RDF.Property))
            graph_target.add((pred_uri, RDFS.label, Literal(str(col).strip(), datatype=XSD.string)))

        defined_predicates[col] = pred_uri

    return defined_predicates

def _process_rows(df, id_column, subject_column, subject_uri_base, config, class_uri_ref, active_template_name, schema_templates, valid_group_config, column_to_group_info, graph_target, defined_predicates, pred_mappings, obj_mappings, iri_column, DATASET_Default, EX, term_to_concept_uri_map, input_data_path):
    def _get_active_template(schema_templates, active_template_name):
        active_template = None
        if active_template_name and schema_templates:
            for t in schema_templates:
                if t.get("template_name") == active_template_name:
                    active_template = t
                    logging.info(f"Using active schema template: {active_template_name}")
                    break
            if not active_template:
                logging.warning(f"Active template '{active_template_name}' not found in provided templates.")
        return active_template

    def _initialize_group_bnodes(valid_group_config, subject_uri, graph_target):
        group_bnodes = {}
        for group_key, config_data in valid_group_config.items():
            bnode = BNode()
            group_bnodes[group_key] = bnode
            connecting_pred_uri = URIRef(config_data['connecting_predicate'])
            graph_target.add((subject_uri, connecting_pred_uri, bnode))
            if config_data.get('group_type'):
                graph_target.add((bnode, RDF.type, URIRef(config_data['group_type'])))
        return group_bnodes

    def _apply_schema_template(active_template, subject_uri, graph_target, pred_mappings, obj_mappings, defined_predicates, row, df_columns, DATASET_Default, EX, config, term_to_concept_uri_map, iri_column):
        template_rdf_type_uri = active_template.get("rdf_type")
        if template_rdf_type_uri and is_valid_uri_simple(template_rdf_type_uri):
            graph_target.add((subject_uri, RDF.type, URIRef(template_rdf_type_uri)))

        apply_template_properties(
            graph_target=graph_target,
            current_subject_node=subject_uri,
            template_properties=active_template.get("properties", []),
            row_data=row,
            df_columns=df_columns,
            all_templates=schema_templates,
            pred_mappings=pred_mappings,
            obj_mappings=obj_mappings,
            defined_predicates=defined_predicates,
            graph_ns=DATASET_Default,
            ex_ns=EX,
            config=config,
            _create_object_node_func=_create_object_node,
            iri_column=iri_column,
            string_column=pred_mappings.columns[0]  # Pass string column name
        )

    def _process_grouped_columns(row, df_columns, column_to_group_info, group_bnodes, graph_target, defined_predicates, pred_mappings, obj_mappings, DATASET_Default, EX, config, term_to_concept_uri_map, iri_column):
        for col_name, group_info_detail in column_to_group_info.items():
            if col_name in df_columns:  # Ensure column exists in df
                obj_value_raw = safe_value(row.get(col_name))
                if obj_value_raw is None:
                    continue

                target_bnode_for_group = group_bnodes.get(group_info_detail['group_key'])
                if not target_bnode_for_group:
                    continue  # Should not happen if group_bnodes initialized correctly

                # Determine predicate for the grouped column's data
                pred_uri_for_group_prop = defined_predicates.get(col_name)
                if not pred_uri_for_group_prop:  # Define if not already
                    # CRITICAL FIX: Check term_to_concept_uri_map FIRST for grouped columns too
                    if term_to_concept_uri_map and col_name in term_to_concept_uri_map:
                        pred_uri_for_group_prop = term_to_concept_uri_map[col_name]
                        logging.debug(f"Grouped column '{col_name}': Using internal concept URI from map")
                    else:
                        # Fallback to mapping table or generate new URI
                        col_norm = str(col_name).strip().lower()
                        raw_mapping_uri_grp = None
                        match_row_grp = pred_mappings[pred_mappings['_norm_term'] == col_norm]
                        if not match_row_grp.empty:
                            raw_mapping_uri_grp = strip_angle_brackets(safe_value(match_row_grp.iloc[0][iri_column]))
                            if raw_mapping_uri_grp and is_valid_uri(raw_mapping_uri_grp):
                                pred_uri_for_group_prop = URIRef(raw_mapping_uri_grp)
                        if not pred_uri_for_group_prop:
                            col_cleaned_grp = clean_string_for_uri(col_name, config.get('uri_character_replacements'))
                            if col_cleaned_grp:
                                pred_uri_for_group_prop = URIRef(str(DATASET_Default) + col_cleaned_grp)

                    if pred_uri_for_group_prop and pred_uri_for_group_prop not in defined_predicates.values():
                        graph_target.add((pred_uri_for_group_prop, RDF.type, RDF.Property))
                        graph_target.add((pred_uri_for_group_prop, RDFS.label, Literal(str(col_name).strip(), datatype=XSD.string)))
                        defined_predicates[col_name] = pred_uri_for_group_prop

                if not pred_uri_for_group_prop:
                    logging.warning(f"Row {idx+1}, Grouped Col '{col_name}': Predicate URI missing for group property.")
                    continue

                # Determine object for the grouped column's data (similar to non-template, non-grouped logic)
                obj_for_group_prop = _create_object_node(obj_value_raw, col_name, obj_mappings, EX, graph_target, config, term_to_concept_uri_map, iri_column=iri_column)
                if obj_for_group_prop:
                    graph_target.add((target_bnode_for_group, pred_uri_for_group_prop, obj_for_group_prop))

    def _process_non_grouped_columns(row, df_columns, column_to_group_info, group_bnodes, subject_uri, graph_target, defined_predicates, pred_mappings, obj_mappings, EX, config, term_to_concept_uri_map, iri_column):
        for col in df_columns:
            if col == id_column or (subject_column and col == subject_column):
                continue

            group_info = column_to_group_info.get(col)
            # If column is part of a group, its direct properties are handled by group processing,
            # not as direct properties of the main subject unless explicitly mapped outside grouping.
            # The original logic correctly assigns to target_subject_node (which is bnode if grouped).

            target_subject_node = group_bnodes.get(group_info['group_key']) if group_info else subject_uri

            obj_value_raw = safe_value(row.get(col))  # Use .get for safety
            if obj_value_raw is None:
                continue

            pred_uri = defined_predicates.get(col)  # Predicates should be pre-defined
            if not pred_uri:
                # This case should ideally be covered by pre-definition or skipped if col is only for grouping
                if group_info:  # If it's a grouped column, its predicate is for the BNode
                    # Predicate definition for grouped columns happens inside group processing loop or needs to be ensured
                    pass  # Already handled if defined_predicates is populated correctly for grouped columns
                else:  # Not grouped, not pre-defined: error or dynamic creation
                    logging.warning(f"Row {idx+1}, Col '{col}': Predicate URI not pre-defined and not in a group. Skipping.")
                    continue

            if not pred_uri:
                continue  # Safety skip

            obj = _create_object_node(obj_value_raw, col, obj_mappings, EX, graph_target, config, term_to_concept_uri_map, iri_column=iri_column)
            if obj is not None:
                graph_target.add((target_subject_node, pred_uri, obj))
            else:
                logging.warning(f"Row {idx+1}, Col '{col}': Could not determine object for '{obj_value_raw}'.")

    active_template = _get_active_template(schema_templates, active_template_name)

    for idx, row in df.iterrows():
        group_bnodes = {}
        subject_uri = None

        if subject_uri_base and subject_column:
            if subject_column not in row:
                logging.warning(f"Row {idx+1}: Shared identifier column '{subject_column}' not found. Skipping row.")
                continue
            subject_id_val = safe_value(row[subject_column])
            if subject_id_val:
                cleaned_id = clean_string_for_uri(subject_id_val, config.get('uri_character_replacements'))
                if cleaned_id:
                    subject_uri = URIRef(subject_uri_base.rstrip('/') + '/' + cleaned_id)
                else:
                    logging.warning(f"Row {idx+1}: Shared ID '{subject_id_val}' cleaned to empty string. Skipping.")
                    continue
            else:
                logging.warning(f"Row {idx+1}: Missing shared ID in '{subject_column}'. Skipping.")
                continue
        else:
            if id_column not in row:
                if id_column == "_generated_id_internal_" or id_column == "_generated_id_":
                    logging.error(f"Row {idx+1}: Expected generated ID column '{id_column}' not found in DataFrame. Skipping.")
                    continue
                else:
                    logging.warning(f"Row {idx+1}: Default ID column '{id_column}' not found. Skipping.")
                    continue
            default_id_val = safe_value(row[id_column])
            if default_id_val:
                    cleaned_id = clean_string_for_uri(default_id_val, config.get('uri_character_replacements'))
                    if cleaned_id:
                        # Use the default_namespace for subjects when not using shared_id_column
                        default_namespace = DATASET_Default 
                        subject_uri = URIRef(default_namespace.rstrip('/') + "/subjects/" + cleaned_id)
                    else:
                        logging.warning(f"Row {idx+1}: Default ID '{default_id_val}' cleaned to empty string. Skipping.")
                        continue
            else:
                logging.warning(f"Row {idx+1}: Missing default ID in '{id_column}'. Skipping.")
                continue
        if subject_uri is None:
            logging.error(f"Row {idx+1}: Failed to determine subject URI unexpectedly. Skipping.")
            continue

        if class_uri_ref and not active_template:  # Only add if not using a template that defines its own type
            graph_target.add((subject_uri, RDF.type, class_uri_ref))


        group_bnodes = _initialize_group_bnodes(valid_group_config, subject_uri, graph_target)

        if active_template:
            _apply_schema_template(active_template, subject_uri, graph_target, pred_mappings, obj_mappings, defined_predicates, row, df.columns, DATASET_Default, EX, config, term_to_concept_uri_map, iri_column)
            _process_grouped_columns(row, df.columns, column_to_group_info, group_bnodes, graph_target, defined_predicates, pred_mappings, obj_mappings, DATASET_Default, EX, config, term_to_concept_uri_map, iri_column)
        else:
            _process_non_grouped_columns(row, df.columns, column_to_group_info, group_bnodes, subject_uri, graph_target, defined_predicates, pred_mappings, obj_mappings, EX, config, term_to_concept_uri_map, iri_column)

def _create_dataset_column_order_sequence(original_column_order: list, graph_target, DATASET_Default, named_graph_uri: str = None, id_column: str = None, subject_column: str = None):
    """Creates column metadata with schema:position to preserve original column order information."""
    if not original_column_order:
        return None

    # Filter out ID and subject columns from the sequence as they're structural, not data columns
    data_columns = [col for col in original_column_order
                   if col != id_column and col != subject_column]

    if not data_columns:
        return None

    # Create a dataset URI to attach the column metadata
    dataset_uri = URIRef(str(DATASET_Default) + "dataset")
    graph_target.add((dataset_uri, RDF.type, URIRef(str(DATASET_Default) + "Dataset")))

    # Add each column with schema:position
    for i, column_name in enumerate(data_columns, 1):
        column_uri = URIRef(str(DATASET_Default) + "columns/" + clean_string_for_uri(column_name, {}))

        # Add column metadata
        graph_target.add((column_uri, RDF.type, URIRef(str(DATASET_Default) + "Column")))
        graph_target.add((column_uri, RDFS.label, Literal(column_name, datatype=XSD.string)))
        graph_target.add((column_uri, URIRef("https://schema.org/position"), Literal(i, datatype=XSD.integer)))
        graph_target.add((column_uri, URIRef(str(DATASET_Default) + "originalColumnName"), Literal(column_name, datatype=XSD.string)))

        # Link column to dataset
        graph_target.add((dataset_uri, URIRef(str(DATASET_Default) + "hasColumn"), column_uri))

    return dataset_uri




def _create_object_node(value_raw: str, column_name: str, obj_mappings_df: pd.DataFrame, ex_ns: Namespace, graph_target, config: dict = None, term_to_concept_uri_map: dict = None, iri_column: str = 'URI'):
    """Helper to create an RDF object node (Literal or URIRef) from a raw value."""
    obj = None
    value_str = str(value_raw).strip()
    value_norm = value_str.lower()

    # 0. Check if the value maps to a SKOS concept
    if term_to_concept_uri_map and value_str in term_to_concept_uri_map:
        return term_to_concept_uri_map[value_str]

    # 1. Check mapping table for explicit object URI
    obj_match_row = obj_mappings_df[obj_mappings_df['_norm_term'] == value_norm]
    if not obj_match_row.empty:
        # Use the dynamically passed iri_column name
        if iri_column in obj_mappings_df.columns:
             raw_obj_uri = strip_angle_brackets(safe_value(obj_match_row.iloc[0][iri_column]))
             if raw_obj_uri and is_valid_uri(raw_obj_uri):
                 obj = URIRef(raw_obj_uri)

    # 2. Check for comparator pattern (e.g., <=5)
    if obj is None:
        comparator_match = re.match(r"^(<=|>=|<|>)(\s*)(\d+[\.,]?\d*)$", str(value_raw))
        if comparator_match:
            comp, _, number_str = comparator_match.groups()
            number_str_norm = number_str.replace(",", ".")
            number_datatype = guess_xsd_datatype(number_str_norm, column_name=None, config=config)
            bnode_qv = BNode()
            graph_target.add((bnode_qv, RDF.type, ex_ns.QuantitativeValue))
            graph_target.add((bnode_qv, ex_ns.comparator, Literal(comp)))
            graph_target.add((bnode_qv, ex_ns.hasValue, Literal(number_str_norm, datatype=number_datatype)))
            obj = bnode_qv

    # 3. Create Literal with guessed datatype
    if obj is None:
        xsd_type = guess_xsd_datatype(value_raw, column_name=column_name, config=config)
        val_str = str(value_raw)
        if xsd_type == XSD.boolean:
            is_true = val_str.lower() in ['true', 'ja', 'yes']
            obj = Literal(is_true, datatype=XSD.boolean)
        elif xsd_type == XSD.gYear:
            year_match_val = re.match(r"^(?P<year>\d{4})(?:\.0+)?$", val_str)
            if year_match_val:
                obj = Literal(year_match_val.group("year"), datatype=XSD.gYear)
            else:
                obj = Literal(val_str, datatype=XSD.string)
        elif xsd_type == XSD.dateTime:
            try:
                dt_obj = date_parse(val_str)
                obj = Literal(dt_obj.isoformat(), datatype=XSD.dateTime)
            except Exception:
                obj = Literal(val_str, datatype=XSD.string)
        elif xsd_type == XSD.decimal:
            obj = Literal(val_str.replace(",", "."), datatype=XSD.decimal)
        else:
            obj = Literal(val_str, datatype=xsd_type)
    return obj

def create_rdf_with_mappings(
    df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    id_column: str,                 # Default subject ID column (_generated_id_ or selected)
    string_column: str,             # Mapping table: Original term column
    iri_column: str,                # Mapping table: Target URI column
    rdf_role_column: str,           # Mapping table: 'predicate' or 'object'
    instance_class_uri: str = None, # Optional: Full URI for rdf:type of subjects
    named_graph_uri: str = None,    # Optional: URI for the named graph context
    subject_uri_base: str = None,   # Optional: Base URI for subjects (enables cross-file linking)
    subject_column: str = None,     # Optional: Column for cross-file linking identifier
    group_config: dict = None,      # Optional: Config for grouping columns from UI
    schema_templates: list = None,  # Optional: List of schema mapping templates from UI
    active_template_name: str = None, # Optional: Name of the template to apply to main subjects
    config: dict = None,
    term_to_concept_uri_map: dict = None,
    input_data_path: str = None,
    original_column_order: list = None  # Optional: Original column order from Excel/CSV
) -> ConjunctiveGraph:
    """
    Generates RDF graph. Prioritizes subject_uri_base/subject_column for subject URIs.
    Handles grouping and schema mapping templates.
    """
    schema_templates = schema_templates or []
    g, EX, DATASET_Default = _initialize_graph_with_namespaces(mapping_df, df, iri_column, instance_class_uri, subject_uri_base, named_graph_uri, config)

    group_config = group_config or {}
    context = URIRef(named_graph_uri) if named_graph_uri else None
    graph_target = g.get_context(context) if context else g

    pred_mappings, obj_mappings, iri_column = _prepare_mappings(mapping_df, string_column, iri_column, rdf_role_column)

    # --- Define Class ---
    class_uri_ref = None
    if instance_class_uri and is_valid_uri_simple(instance_class_uri):
        class_uri_ref = URIRef(instance_class_uri)
        class_label = extract_label(instance_class_uri)
        graph_target.add((class_uri_ref, RDF.type, RDFS.Class))
        graph_target.add((class_uri_ref, RDFS.label, Literal(class_label, datatype=XSD.string)))
    elif instance_class_uri: logging.warning(f"Provided Class URI '{instance_class_uri}' invalid.")

    # --- Pre-process Grouping Configuration ---
    column_to_group_info = {}; valid_group_config = {}
    for group_key, config in group_config.items():
        conn_pred = config.get('connecting_predicate'); cols = config.get('columns', [])
        if conn_pred and cols:
            valid_group_config[group_key] = config
            for col_name in cols: column_to_group_info[col_name] = {'group_key': group_key, 'config': config}
        else: logging.warning(f"Skipping invalid group config for key '{group_key}'.")

    defined_predicates = _define_predicates(df, pred_mappings, id_column, subject_column, column_to_group_info, iri_column, graph_target, config, term_to_concept_uri_map)

    _process_rows(df, id_column, subject_column, subject_uri_base, config, class_uri_ref, active_template_name, schema_templates, valid_group_config, column_to_group_info, graph_target, defined_predicates, pred_mappings, obj_mappings, iri_column, DATASET_Default, EX, term_to_concept_uri_map, input_data_path)

    # Create dataset-level column order sequence to preserve original Excel/CSV column order
    # Place this at the end to separate it from the main data
    if original_column_order:
        _create_dataset_column_order_sequence(original_column_order, graph_target, DATASET_Default, named_graph_uri, id_column, subject_column)

    return g

# --- End of Script ---
