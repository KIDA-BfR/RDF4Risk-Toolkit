import yaml
import pandas as pd
import logging
import os
import hashlib
import re
from datetime import datetime
import asyncio
import argparse
from .rdf_processor import create_rdf_with_mappings
from .rdf_serializer import serialize_rdf
from .dcat_generator import create_dcat_catalog
from .skos_generator import create_skos_graph_and_lookup_map
from .uri_utils import generate_hashed_graph_uri, process_iris_async
from rdflib import URIRef

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(config_path='config.yaml'):
    """Loads the YAML configuration file."""
    try:
        # When running from mcp_server, the current working directory is the root.
        # We need to construct the correct path to the config file.
        if not os.path.isabs(config_path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(script_dir, config_path)

        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at: {config_path}")
        return None
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return None

def load_data(file_path):
    """Loads data from a CSV or Excel file."""
    try:
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_path.endswith('.xlsx'):
            return pd.read_excel(file_path)
        else:
            logging.error(f"Unsupported file format: {file_path}")
            return None
    except FileNotFoundError:
        logging.error(f"Data file not found at: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        return None

def save_graph_to_ttl(graph, output_path, graph_name):
    """Saves a graph to a TTL file."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        graph.serialize(destination=output_path, format='turtle')
        logging.info(f"{graph_name} graph successfully saved to: {output_path}")
    except Exception as e:
        logging.error(f"Failed to save {graph_name} graph to {output_path}: {e}", exc_info=True)

def main(
    input_csv_path: str = None, 
    input_data_path: str = None, 
    api_keys: dict = None, 
    ignore_services: list = None
):
    """Main function to run the RDF generation process."""
    config = load_config('config.yaml')
    if not config:
        return

    # --- API Specs ---
    api_specs = config.get('api_specs', {})

    # --- Determine Data Paths ---
    data_path = input_data_path if input_data_path else config.get('input_data_path')
    mapping_data_path = input_csv_path if input_csv_path else config.get('mapping_data_path')

    if not data_path or not mapping_data_path:
        logging.error("Input data path or mapping data path is not defined. Aborting.")
        return

    # --- IRI Resolution Step (Conditional) ---
    if config.get('iri_resolution', {}).get('enabled', False):
        logging.info("IRI resolution is enabled. Starting process...")
        
        unresolved_mapping_df = load_data(mapping_data_path)
        if unresolved_mapping_df is None:
            logging.error("Failed to load mapping data for IRI resolution. Aborting.")
            return

    if "URI" in unresolved_mapping_df.columns:
        unresolved_mapping_df.rename(columns={"URI": "Mapped ID"}, inplace=True)

        # Pass the dynamic keys and ignored services to the processing function
        results_df = asyncio.run(process_iris_async(
            df=unresolved_mapping_df,
            specs=api_specs,
            concurrency_limit=5,
            api_keys=api_keys,
            ignore_services=ignore_services
        ))

        mapping_df = pd.merge(
            unresolved_mapping_df, results_df,
            how='left', left_on='Mapped ID', right_on='iri'
        )

        if 'label_y' in mapping_df.columns and 'label_x' in mapping_df.columns:
            mapping_df['label'] = mapping_df['label_y'].fillna(mapping_df['label_x'])
            mapping_df.drop(columns=['label_x', 'label_y'], inplace=True)

        if 'iri' in mapping_df.columns:
            mapping_df.drop(columns=['iri'], inplace=True)

        logging.info("--- Resolved IRIs ---")
        logging.info(results_df.to_string())
        
        resolved_output_path = config.get('iri_resolution', {}).get('output_path')
        if resolved_output_path:
            try:
                # Ensure the output directory exists
                output_dir = os.path.dirname(resolved_output_path)
                if not os.path.isabs(output_dir):
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    output_dir = os.path.join(script_dir, output_dir)
                os.makedirs(output_dir, exist_ok=True)
                
                final_output_path = os.path.join(output_dir, os.path.basename(resolved_output_path))
                mapping_df.to_csv(final_output_path, index=False)
                logging.info(f"Resolved mapping table saved to: {final_output_path}")
            except Exception as e:
                logging.warning(f"Could not save resolved mapping table: {e}")
        
        logging.info("IRI resolution complete.")
    else:
        logging.info("IRI resolution not enabled. Loading mapping data directly.")
        mapping_df = load_data(mapping_data_path)
        if mapping_df is not None and "URI" in mapping_df.columns:
            # Ensure column name is consistent with the resolution path
            mapping_df.rename(columns={"URI": "Mapped ID"}, inplace=True)

    # --- Load Main Data ---
    df = load_data(data_path)


    if df is None or mapping_df is None:
        logging.error("Failed to load data, or mapping data could not be created. Aborting.")
        return

    # --- Ensure ID column exists if using the default ---
    id_column_name = config.get('id_column', '_generated_id_')
    if id_column_name == '_generated_id_' and id_column_name not in df.columns:
        logging.info(f"Default ID column '{id_column_name}' not found. Generating it from DataFrame index.")
        # Calculate number of digits needed for zero-padding
        total_rows = len(df)
        num_digits = len(str(total_rows))
        # Generate IDs with zero-padding for proper alphabetical sorting
        df[id_column_name] = [f"row_{i:0{num_digits}d}" for i in df.index]

    # --- Generate Dynamic Graph URI ---
    # This will override any URI from the config, ensuring a unique graph per run.
    data_graph_uri = generate_hashed_graph_uri(config['input_data_path'], config=config)
    if not data_graph_uri:
        logging.error("Could not generate a dynamic graph URI. Aborting.")
        return

    # --- Generate SKOS Graph and Term Map ---
    logging.info("Generating SKOS graph and term map...")
    skos_graph, term_to_uri_lookup = create_skos_graph_and_lookup_map(
        mapping_df=mapping_df,
        config=config,
        data_graph_uri=data_graph_uri  # Pass the dynamic URI
    )

    # --- Generate Data Graph ---
    logging.info("Starting data graph generation...")
    try:
        rdf_graph = create_rdf_with_mappings(
            df=df,
            mapping_df=mapping_df,
            id_column=config.get('id_column', '_generated_id_'),
            string_column=config.get('string_column', 'Term'),
            iri_column='Mapped ID',
            rdf_role_column=config.get('rdf_role_column', 'RDF Role'),
            match_type_column=config.get('match_type_column'),
            instance_class_uri=config.get('instance_class_uri'),
            named_graph_uri=data_graph_uri, # Use the dynamically generated URI
            subject_uri_base=config.get('subject_uri_base'),
            subject_column=config.get('subject_column'),
            group_config=config.get('group_config'),
            schema_templates=config.get('schema_templates'),
            active_template_name=config.get('active_template_name'),
            config=config,
            term_to_concept_uri_map=term_to_uri_lookup
        )
        logging.info("Data graph created successfully.")
    except Exception as e:
        logging.error(f"An error occurred during data graph creation: {e}", exc_info=True)
        return

    # --- Save individual graphs before merging ---
    output_dir = os.path.dirname(config.get('output_rdf_path', 'output.ttl'))
    save_graph_to_ttl(rdf_graph, os.path.join(output_dir, 'data_graph.ttl'), 'Data')
    save_graph_to_ttl(skos_graph, os.path.join(output_dir, 'skos_graph.ttl'), 'SKOS')

    # --- Merge Graphs ---
    logging.info("Merging SKOS and data graphs...")
    for quad in skos_graph.quads((None, None, None, None)):
        rdf_graph.add(quad)

    # --- Serialize Final Graph ---
    logging.info(f"Serializing main RDF graph to {config.get('rdf_format', 'turtle')} format...")
    try:
        serialized_rdf = serialize_rdf(
            graph=rdf_graph,
            rdf_format=config.get('rdf_format', 'turtle'),
            named_graph_uri=data_graph_uri # Use the dynamically generated URI
        )
    except Exception as e:
        logging.error(f"An error occurred during main RDF serialization: {e}", exc_info=True)
        return

    # --- DCAT Catalog Generation (Optional) ---
    if 'dcat_metadata' in config and config['dcat_metadata'].get('enabled', False):
        logging.info("DCAT metadata generation is enabled. Creating full catalog...")
        try:
            # The output of DCAT is always a TriG string, so we overwrite serialized_rdf
            serialized_rdf = create_dcat_catalog(
                rdf_data_string=serialized_rdf,
                rdf_format=config.get('rdf_format', 'turtle'),
                data_graph_uri_str=data_graph_uri, # Use the dynamically generated URI
                metadata_config=config['dcat_metadata'],
                default_namespace=config.get('default_namespace')
            )
            
            # The format is now 'trig' for file extension purposes.
            config['rdf_format'] = 'trig'
        except Exception as e:
            logging.error(f"An error occurred during DCAT catalog generation: {e}", exc_info=True)
            return

    # --- Determine Output Path and Save ---
    output_path = None
    rdf_format = config.get('rdf_format', 'turtle')

    # If DCAT was enabled, it dictates the output path.
    if 'dcat_metadata' in config and config['dcat_metadata'].get('enabled', False):
        output_path = config.get('output_rdf_path', 'dcat_catalog.trig')
    else:
        # Otherwise, use the standard output path.
        output_path = config.get('output_rdf_path', 'output.ttl')
        # If the format is trig (but not from DCAT), ensure the extension is correct.
        if rdf_format == 'trig':
            base, _ = os.path.splitext(output_path)
            output_path = base + '.trig'

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(serialized_rdf)
        logging.info(f"RDF output successfully saved to: {output_path}")
    except Exception as e:
        logging.error(f"Failed to save RDF output to {output_path}: {e}", exc_info=True)

if __name__ == '__main__':
    import json
    parser = argparse.ArgumentParser(description="Automated RDF Generation from CSV/Excel data.")
    parser.add_argument('--input_csv_path', help='Path to the input mapping CSV file.')
    parser.add_argument('--input_data_path', help='Path to the input data file (CSV or Excel).')
    parser.add_argument(
        '--api_keys', 
        type=str, 
        help='JSON string of API keys, e.g., \'{"bioportal": "your_key"}\''
    )
    parser.add_argument(
        '--ignore_services', 
        nargs='*', 
        help='List of services to ignore, e.g., --ignore_services ols geonames'
    )
    args = parser.parse_args()

    # Parse JSON string for API keys if provided
    parsed_api_keys = None
    if args.api_keys:
        try:
            parsed_api_keys = json.loads(args.api_keys)
        except json.JSONDecodeError:
            logging.error("Invalid JSON format for --api_keys argument.")
            exit(1)

    main(
        input_csv_path=args.input_csv_path, 
        input_data_path=args.input_data_path,
        api_keys=parsed_api_keys,
        ignore_services=args.ignore_services
    )
