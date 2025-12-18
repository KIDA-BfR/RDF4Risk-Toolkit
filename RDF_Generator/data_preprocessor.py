import pandas as pd
import re
from urllib.parse import urlparse
from dateutil.parser import parse as date_parse
from rdflib.namespace import XSD

def clean_string_for_uri(value: str, replacements: dict = None) -> str:
    """Cleans a string to be suitable for use in the path part of a URI,
       preferring ASCII representations by transliterating common characters."""
    if not value or pd.isna(value): return None
    value_str = str(value).strip()

    # Transliteration for common German characters and others
    if replacements:
        for char, replacement in replacements.items():
            value_str = value_str.replace(char, replacement)

    # Replace any remaining non-ASCII characters (not covered by transliteration)
    # and other unwanted symbols with underscore.
    # This regex targets anything not an ASCII letter, digit, or hyphen.
    value_str = re.sub(r'[^a-zA-Z0-9-]', '_', value_str)

    value_str = value_str.strip('_') # Remove leading/trailing underscores
    value_str = re.sub(r'_+', '_', value_str) # Consolidate multiple underscores

    # If the string becomes empty after cleaning, return None
    if not value_str:
        return None

    return value_str # Removed quote() call, as transliteration and regex should suffice


def safe_value(value) -> str:
    """Safely convert value to string, return None for NA/empty."""
    if pd.isna(value): return None
    value_str = str(value).strip()
    return value_str if value_str else None


def guess_xsd_datatype(value, column_name: str = None, config: dict = None): # Added column_name parameter
    """Guesses the XSD datatype for a given literal value."""
    config = config or {}
    datatype_guessing_config = config.get('datatype_guessing', {})
    boolean_keywords = datatype_guessing_config.get('boolean_keywords', ['true', 'false', 'ja', 'nein', 'yes', 'no'])
    year_keywords = datatype_guessing_config.get('year_keywords', ['jahr', 'year'])

    try:
        value_str = str(value).strip()
        if not value_str: return XSD.string
        value_lower = value_str.lower()

        # 1. Boolean
        if value_lower in boolean_keywords:
            return XSD.boolean

        # 2. gYear (Gregorian Year)
        if column_name:
            col_name_lower = str(column_name).lower()
            if any(keyword in col_name_lower for keyword in year_keywords):
                year_match = re.fullmatch(r"^(?P<year>\d{4})(?:\.0+)?$", value_str)
                if year_match:
                    return XSD.gYear
        
        # 3. Integer
        if re.fullmatch(r"^-?\d+$", value_str):
            return XSD.integer
        
        # 4. Decimal (replaces Float for general decimal numbers)
        # Normalize comma to dot for decimal check
        value_norm_for_decimal = value_str.replace(",", ".")
        if re.fullmatch(r"^-?\d+\.\d+$", value_norm_for_decimal):
            return XSD.decimal # Changed from XSD.float
        
        # 5. DateTime (use original value_str for date parsing as it's more robust)
        try:
            _ = date_parse(value_str, fuzzy=False)
            return XSD.dateTime
        except:
            pass
        
    except Exception:
        pass
    return XSD.string


def is_valid_uri(uri: str) -> bool:
    """Checks if a string is a valid absolute URI."""
    if not uri or not isinstance(uri, str): return False
    try: result = urlparse(uri); return all([result.scheme, result.netloc])
    except ValueError: return False


def is_valid_uri_simple(uri: str) -> bool:
    """Less strict check if a string *looks like* it could be a URI."""
    if not uri or not isinstance(uri, str): return False
    try: result = urlparse(uri); return bool(result.scheme or result.path or ':' in uri)
    except ValueError: return False


def strip_angle_brackets(iri: str) -> str:
    """Removes leading/trailing angle brackets."""
    if iri and isinstance(iri, str): return iri.strip().lstrip('<').rstrip('>')
    return iri


def extract_label(uri_or_text: str) -> str:
    """Extracts a potential label from a URI or returns the text."""
    uri_or_text = str(uri_or_text).strip()
    if is_valid_uri(uri_or_text):
        try:
            parsed = urlparse(uri_or_text)
            if parsed.fragment: return parsed.fragment
            else: path_part = parsed.path.rstrip('/'); return path_part.split('/')[-1] if '/' in path_part else path_part if path_part else parsed.netloc if parsed.netloc else uri_or_text
        except Exception: return uri_or_text
    return uri_or_text


def _extract_full_uris_from_mappings(mapping_df: pd.DataFrame, iri_column: str) -> set:
    """Extracts all unique, valid, full URIs from the mapping table's IRI column."""
    if iri_column not in mapping_df.columns:
        return set()
    
    uris = set()
    for uri in mapping_df[iri_column].dropna().unique():
        # Added .strip() to remove potential leading/trailing whitespace from the cell
        cleaned_uri = strip_angle_brackets(str(uri).strip())
        if is_valid_uri(cleaned_uri):
            uris.add(cleaned_uri)
    return uris


def _extract_all_uris_from_data(df: pd.DataFrame) -> set:
    """Scans the entire DataFrame and extracts all unique, valid, full URIs from the data itself."""
    uris = set()
    for col in df.columns:
        for item in df[col].dropna().unique():
            item_str = str(item)
            # Quick check for http/https to avoid running regex on everything
            if 'http' not in item_str:
                continue

            # This regex is a bit more robust for finding URIs within other text
            potential_uris = re.findall(r'https?://[^\s<>"\']+', item_str)
            for uri in potential_uris:
                cleaned_uri = strip_angle_brackets(uri.strip())
                if is_valid_uri(cleaned_uri):
                    uris.add(cleaned_uri)
    return uris


def capture_original_column_order(df: pd.DataFrame) -> list:
    """Captures and returns the original column order from a DataFrame."""
    return list(df.columns)


def load_excel_with_column_order(file_path_or_buffer, **kwargs):
    """
    Loads Excel file while preserving original column order.
    Returns tuple of (DataFrame, original_column_order).
    """
    df = pd.read_excel(file_path_or_buffer, **kwargs)
    original_order = capture_original_column_order(df)
    return df, original_order


def load_csv_with_column_order(file_path_or_buffer, **kwargs):
    """
    Loads CSV file while preserving original column order.
    Returns tuple of (DataFrame, original_column_order).
    """
    df = pd.read_csv(file_path_or_buffer, **kwargs)
    original_order = capture_original_column_order(df)
    return df, original_order
