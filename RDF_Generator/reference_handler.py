#!/usr/bin/env python3
"""
DOI to SemOpenAlex RDF Converter
Retrieves publication metadata from DOI and converts to SemOpenAlex RDF format
"""

import requests
import json
from datetime import datetime
from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, RDFS, XSD, DCTERMS, FOAF
import hashlib
import argparse
import sys
from urllib.parse import quote
import time

# Define namespaces
SOA = Namespace("https://semopenalex.org/ontology/")
PRISM = Namespace("http://prismstandard.org/namespaces/basic/2.0/")
FABIO = Namespace("http://purl.org/spar/fabio/")
CITO = Namespace("http://purl.org/spar/cito/")
DBPEDIA_OWL = Namespace("https://dbpedia.org/ontology/")
DBPROP = Namespace("https://dbpedia.org/property/")
ORG = Namespace("http://www.w3.org/ns/org#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
BIDO = Namespace("http://purl.org/spar/bido/")

# Base URIs for SemOpenAlex entities
SOA_WORK = Namespace("https://semopenalex.org/work/")
SOA_AUTHOR = Namespace("https://semopenalex.org/author/")
SOA_AUTHORSHIP = Namespace("https://semopenalex.org/authorship/")
SOA_LOCATION = Namespace("https://semopenalex.org/location/")
SOA_SOURCE = Namespace("https://semopenalex.org/source/")
SOA_INSTITUTION = Namespace("https://semopenalex.org/institution/")
SOA_OPENACCESS = Namespace("https://semopenalex.org/openaccess/")
SOA_FUNDER = Namespace("https://semopenalex.org/funder/")
SOA_CONCEPT = Namespace("https://semopenalex.org/concept/")

class DOIToSemOpenAlexConverter:
    def __init__(self, fetch_citations=True, max_workers=5):
        self.graph = Graph()
        self.bind_namespaces()
        self.fetch_citations = fetch_citations  # Make citation fetching optional
        self.max_workers = max_workers  # Control parallel processing
        self.metadata_cache = {}  # Simple in-memory cache
        
    def bind_namespaces(self):
        """Bind all namespaces to the graph"""
        self.graph.bind("soa", SOA)
        self.graph.bind("prism", PRISM)
        self.graph.bind("fabio", FABIO)
        self.graph.bind("cito", CITO)
        self.graph.bind("dct", DCTERMS)
        self.graph.bind("foaf", FOAF)
        self.graph.bind("dbpedia-owl", DBPEDIA_OWL)
        self.graph.bind("dbprop", DBPROP)
        self.graph.bind("org", ORG)
        self.graph.bind("skos", SKOS)
        self.graph.bind("bido", BIDO)
        
    def generate_id(self, text):
        """Generate a consistent ID from text"""
        return hashlib.md5(text.encode()).hexdigest()[:10].upper()
    
    def validate_uri(self, uri_string):
        """Validate that a URI string is properly formatted"""
        try:
            # Test by creating a URIRef - this will fail if invalid
            test_uri = URIRef(uri_string)
            # Additional check: ensure no problematic characters
            invalid_chars = ['(', ')', '<', '>', ' ', '{', '}']
            return not any(char in str(test_uri) for char in invalid_chars)
        except Exception:
            return False
    
    def fetch_crossref_metadata(self, doi):
        """Fetch metadata from CrossRef API with caching"""
        # Check cache first
        if doi in self.metadata_cache:
            return self.metadata_cache[doi]
            
        url = f"https://api.crossref.org/works/{doi}"
        headers = {
            'User-Agent': 'DOI-to-SemOpenAlex-Converter/1.0 (mailto:your-email@example.com)'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=5)  # Reduced timeout
            response.raise_for_status()
            metadata = response.json()['message']
            self.metadata_cache[doi] = metadata  # Cache the result
            return metadata
        except requests.exceptions.RequestException as e:
            print(f"Error fetching metadata from CrossRef: {e}")
            self.metadata_cache[doi] = None  # Cache failures too
            return None
    
    def fetch_openalex_metadata(self, doi):
        """Fetch metadata from OpenAlex API"""
        url = f"https://api.openalex.org/works/doi:{doi}"
        headers = {
            'User-Agent': 'DOI-to-SemOpenAlex-Converter/1.0'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            return None
        except requests.exceptions.RequestException:
            return None
    
    def create_work_uri(self, doi):
        """Create a URI for the work"""
        work_id = f"W{self.generate_id(doi)}"
        return URIRef(SOA_WORK[work_id])
    
    def add_work_metadata(self, work_uri, metadata, openalex_data=None):
        """Add work metadata to the graph"""
        # Add type
        self.graph.add((work_uri, RDF.type, SOA.Work))
        
        # Title
        if 'title' in metadata and metadata['title']:
            if isinstance(metadata['title'], list) and len(metadata['title']) > 0:
                title = metadata['title'][0]
            elif isinstance(metadata['title'], str):
                title = metadata['title']
            else:
                title = None
            if title:
                self.graph.add((work_uri, DCTERMS.title, Literal(title, lang="en")))
        
        # Abstract
        if 'abstract' in metadata:
            self.graph.add((work_uri, DCTERMS.abstract, Literal(metadata['abstract'], lang="en")))
        elif openalex_data and 'abstract_inverted_index' in openalex_data:
            # Reconstruct abstract from inverted index if available
            abstract = self.reconstruct_abstract(openalex_data['abstract_inverted_index'])
            if abstract:
                self.graph.add((work_uri, DCTERMS.abstract, Literal(abstract, lang="en")))
        
        # DOI
        if 'DOI' in metadata:
            doi_uri = URIRef(f"https://doi.org/{metadata['DOI']}")
            self.graph.add((work_uri, PRISM.doi, doi_uri))
        
        # Publication date
        if 'published-print' in metadata and 'date-parts' in metadata['published-print']:
            date_parts_list = metadata['published-print']['date-parts']
            if len(date_parts_list) > 0:
                date_parts = date_parts_list[0]
                if len(date_parts) >= 3:
                    date_str = f"{date_parts[0]:04d}-{date_parts[1]:02d}-{date_parts[2]:02d}"
                    self.graph.add((work_uri, PRISM.publicationDate, Literal(date_str, datatype=XSD.date)))
                elif len(date_parts) >= 1:
                    self.graph.add((work_uri, FABIO.hasPublicationYear, Literal(date_parts[0], datatype=XSD.integer)))
                if len(date_parts) >= 1:
                    self.graph.add((work_uri, FABIO.hasPublicationYear, Literal(date_parts[0], datatype=XSD.integer)))
        elif 'published-online' in metadata and 'date-parts' in metadata['published-online']:
            date_parts_list = metadata['published-online']['date-parts']
            if len(date_parts_list) > 0:
                date_parts = date_parts_list[0]
                if len(date_parts) >= 2:
                    date_str = f"{date_parts[0]:04d}-{date_parts[1]:02d}-01"
                    self.graph.add((work_uri, PRISM.publicationDate, Literal(date_str, datatype=XSD.date)))
                elif len(date_parts) >= 1:
                    date_str = f"{date_parts[0]:04d}-01-01"
                    self.graph.add((work_uri, PRISM.publicationDate, Literal(date_str, datatype=XSD.date)))
                if len(date_parts) >= 1:
                    self.graph.add((work_uri, FABIO.hasPublicationYear, Literal(date_parts[0], datatype=XSD.integer)))
        
        # Volume and issue
        if 'volume' in metadata:
            self.graph.add((work_uri, SOA.hasVolume, Literal(metadata['volume'])))
        if 'issue' in metadata:
            self.graph.add((work_uri, SOA.hasIssue, Literal(metadata['issue'])))
        
        # Pages
        if 'page' in metadata:
            pages = metadata['page'].split('-')
            if len(pages) >= 1:
                self.graph.add((work_uri, PRISM.startingPage, Literal(pages[0])))
            if len(pages) >= 2:
                self.graph.add((work_uri, PRISM.endingPage, Literal(pages[1])))
        
        # Type
        if 'type' in metadata:
            self.graph.add((work_uri, SOA.crossrefType, Literal(metadata['type'])))
            self.graph.add((work_uri, SOA.workType, Literal(metadata['type'])))
        
        # Additional metadata
        self.graph.add((work_uri, SOA.isParatext, Literal(False, datatype=XSD.boolean)))
        self.graph.add((work_uri, SOA.isRetracted, Literal(False, datatype=XSD.boolean)))
        
        # Created and modified dates
        now = datetime.now().strftime("%Y-%m-%d")
        self.graph.add((work_uri, DCTERMS.created, Literal(now, datatype=XSD.date)))
        self.graph.add((work_uri, DCTERMS.modified, Literal(now, datatype=XSD.date)))
        
        # Citation count from OpenAlex if available
        if openalex_data and 'cited_by_count' in openalex_data:
            self.graph.add((work_uri, SOA.citedByCount, Literal(openalex_data['cited_by_count'], datatype=XSD.integer)))
        else:
            self.graph.add((work_uri, SOA.citedByCount, Literal(0, datatype=XSD.integer)))
        
        return work_uri
    
    def add_authors(self, work_uri, metadata, openalex_data=None):
        """Add author information to the graph"""
        if 'author' not in metadata:
            return
        
        for idx, author in enumerate(metadata['author']):
            # Create author URI
            author_name = f"{author.get('given', '')} {author.get('family', '')}".strip()
            if not author_name:
                author_name = author.get('name', f"Author{idx+1}")
            
            author_id = f"A{self.generate_id(author_name)}"
            author_uri = SOA_AUTHOR[author_id]
            
            # Create authorship URI
            authorship_id = f"AS{self.generate_id(f'{work_uri}{author_name}')}"
            authorship_uri = SOA_AUTHORSHIP[authorship_id]
            
            # Add author
            self.graph.add((author_uri, RDF.type, SOA.Author))
            self.graph.add((author_uri, FOAF.name, Literal(author_name)))
            
            # Add ORCID if available
            if 'ORCID' in author:
                orcid = author['ORCID'].replace('http://orcid.org/', '').replace('https://orcid.org/', '')
                self.graph.add((author_uri, DBPEDIA_OWL.orcidId, Literal(orcid)))
            
            # Add authorship
            self.graph.add((authorship_uri, RDF.type, SOA.Authorship))
            self.graph.add((authorship_uri, SOA.hasAuthor, author_uri))
            self.graph.add((authorship_uri, SOA.position, Literal(idx + 1, datatype=XSD.integer)))
            self.graph.add((authorship_uri, SOA.isCorresponding, Literal(idx == 0, datatype=XSD.boolean)))
            
            # Add affiliation if available
            if 'affiliation' in author and author['affiliation'] and len(author['affiliation']) > 0:
                affiliation = author['affiliation'][0]
                if 'name' in affiliation:
                    inst_id = f"I{self.generate_id(affiliation['name'])}"
                    inst_uri = SOA_INSTITUTION[inst_id]

                    self.graph.add((inst_uri, RDF.type, SOA.Institution))
                    self.graph.add((inst_uri, FOAF.name, Literal(affiliation['name'])))
                    self.graph.add((authorship_uri, SOA.hasOrganization, inst_uri))
                    self.graph.add((authorship_uri, SOA.rawAffiliation, Literal(affiliation['name'])))
            
            # Link work to authorship and creator
            self.graph.add((work_uri, SOA.hasAuthorship, authorship_uri))
            self.graph.add((work_uri, DCTERMS.creator, author_uri))
    
    def add_source(self, work_uri, metadata):
        """Add source/journal information"""
        if 'container-title' not in metadata or not metadata['container-title']:
            return
        
        if isinstance(metadata['container-title'], list) and len(metadata['container-title']) > 0:
            journal_name = metadata['container-title'][0]
        elif isinstance(metadata['container-title'], str):
            journal_name = metadata['container-title']
        else:
            return
        
        # Create source URI
        source_id = f"S{self.generate_id(journal_name)}"
        source_uri = SOA_SOURCE[source_id]
        
        # Create location URI
        location_id = f"L{self.generate_id(f'{work_uri}{journal_name}')}"
        location_uri = SOA_LOCATION[location_id]
        
        # Add source
        self.graph.add((source_uri, RDF.type, SOA.Source))
        self.graph.add((source_uri, FOAF.name, Literal(journal_name)))
        self.graph.add((source_uri, SOA.sourceType, Literal("journal")))
        
        # Add ISSN if available
        if 'ISSN' in metadata and metadata['ISSN']:
            if isinstance(metadata['ISSN'], list) and len(metadata['ISSN']) > 0:
                issn = metadata['ISSN'][0]
            elif isinstance(metadata['ISSN'], str):
                issn = metadata['ISSN']
            else:
                issn = None
            if issn:
                self.graph.add((source_uri, PRISM.issn, Literal(issn)))
                self.graph.add((source_uri, FABIO.hasIssnL, Literal(issn)))
        
        # Add publisher if available
        if 'publisher' in metadata:
            self.graph.add((source_uri, DCTERMS.publisher, Literal(metadata['publisher'])))
        
        # Add location
        self.graph.add((location_uri, RDF.type, SOA.Location))
        self.graph.add((location_uri, SOA.hasSource, source_uri))
        
        # Add URL if available
        if 'URL' in metadata:
            self.graph.add((location_uri, FABIO.hasURL, Literal(metadata['URL'])))
        
        # Link work to location
        self.graph.add((work_uri, SOA.hasPrimaryLocation, location_uri))
        self.graph.add((work_uri, SOA.hasLocation, location_uri))
    
    def add_open_access(self, work_uri, metadata, openalex_data=None):
        """Add open access information"""
        oa_id = f"OA{self.generate_id(str(work_uri))}"
        oa_uri = SOA_OPENACCESS[oa_id]
        
        self.graph.add((oa_uri, RDF.type, SOA.OpenAccess))
        
        # Check if it's open access
        is_oa = False
        oa_status = "closed"
        
        if openalex_data and 'open_access' in openalex_data:
            is_oa = openalex_data['open_access'].get('is_oa', False)
            oa_status = openalex_data['open_access'].get('oa_status', 'closed')
        elif 'license' in metadata and metadata['license']:
            is_oa = True
            oa_status = "hybrid"
        
        self.graph.add((oa_uri, SOA.isOa, Literal(is_oa, datatype=XSD.boolean)))
        self.graph.add((oa_uri, SOA.oaStatus, Literal(oa_status)))
        
        # Add OA URL if available
        if openalex_data and 'open_access' in openalex_data and 'oa_url' in openalex_data['open_access']:
            self.graph.add((oa_uri, SOA.oaUrl, Literal(openalex_data['open_access']['oa_url'])))
        
        self.graph.add((work_uri, SOA.hasOpenAccess, oa_uri))
    
    def add_funders(self, work_uri, metadata):
        """Add funder information"""
        if 'funder' not in metadata:
            return
        
        for funder in metadata['funder']:
            if 'name' not in funder:
                continue
            
            funder_id = f"F{self.generate_id(funder['name'])}"
            funder_uri = SOA_FUNDER[funder_id]
            
            self.graph.add((funder_uri, RDF.type, SOA.Funder))
            self.graph.add((funder_uri, FOAF.name, Literal(funder['name'])))
            
            if 'DOI' in funder:
                doi_uri = URIRef(f"https://doi.org/{funder['DOI']}")
                self.graph.add((funder_uri, PRISM.doi, doi_uri))
            
            self.graph.add((work_uri, SOA.hasFunder, funder_uri))
    
    def add_references(self, work_uri, metadata):
        """Add citation references - fast, lightweight approach"""
        if 'reference' not in metadata:
            return
        
        references = metadata.get('reference', [])
        doi_references = [ref for ref in references if 'DOI' in ref]
        
        if not doi_references:
            return
            
        print(f"Processing {len(doi_references)} DOI citations (fast mode)...")
        
        for ref in doi_references:
            try:
                # Create clean hash-based URI for RDF graph
                cited_work_id = f"W{self.generate_id(ref['DOI'])}"
                cited_work_uri = URIRef(SOA_WORK[cited_work_id])
                
                # Add the citation relationship
                self.graph.add((work_uri, CITO.cites, cited_work_uri))
                
                # Add basic work metadata
                self.graph.add((cited_work_uri, RDF.type, SOA.Work))
                
                # Store original DOI as resolvable literal
                self.graph.add((cited_work_uri, PRISM.doi, Literal(ref['DOI'])))
                
                # Store DOI as resolvable URI (URL-encoded for valid URI)
                encoded_doi = quote(ref['DOI'], safe='')
                doi_uri = URIRef(f"https://doi.org/{encoded_doi}")
                self.graph.add((cited_work_uri, DCTERMS.identifier, doi_uri))
                
                # Add any available basic info from CrossRef reference data
                if 'article-title' in ref:
                    self.graph.add((cited_work_uri, DCTERMS.title, Literal(ref['article-title'])))
                
                if 'journal-title' in ref:
                    self.graph.add((cited_work_uri, DCTERMS.source, Literal(ref['journal-title'])))
                
                if 'author' in ref:
                    # Simple author string (no complex authorship modeling for citations)
                    author_str = ref['author'] if isinstance(ref['author'], str) else str(ref.get('author', ''))
                    if author_str:
                        self.graph.add((cited_work_uri, DCTERMS.creator, Literal(author_str)))
                
                if 'year' in ref:
                    self.graph.add((cited_work_uri, DCTERMS.date, Literal(ref['year'])))
                    
            except Exception as e:
                print(f"Error processing citation DOI {ref['DOI']}: {e}")
                continue
    
    def add_concepts_from_openalex(self, work_uri, openalex_data):
        """Add concepts from OpenAlex data if available"""
        if not openalex_data or 'concepts' not in openalex_data:
            return
        
        for concept in openalex_data['concepts']:
            concept_id = f"C{self.generate_id(concept.get('display_name', ''))}"
            concept_uri = SOA_CONCEPT[concept_id]
            
            self.graph.add((concept_uri, RDF.type, SOA.Concept))
            self.graph.add((concept_uri, SKOS.prefLabel, Literal(concept.get('display_name', ''))))
            
            if 'level' in concept:
                self.graph.add((concept_uri, SOA.level, Literal(concept['level'], datatype=XSD.integer)))
            
            if 'score' in concept:
                self.graph.add((concept_uri, SOA.score, Literal(concept['score'], datatype=XSD.float)))
            
            self.graph.add((work_uri, SOA.hasConcept, concept_uri))
    
    def reconstruct_abstract(self, inverted_index):
        """Reconstruct abstract from OpenAlex inverted index"""
        if not inverted_index:
            return None
        
        # Create position-word pairs
        word_positions = []
        for word, positions in inverted_index.items():
            for pos in positions:
                word_positions.append((pos, word))
        
        # Sort by position
        word_positions.sort(key=lambda x: x[0])
        
        # Reconstruct text
        abstract = ' '.join([word for _, word in word_positions])
        return abstract
    
    def convert(self, doi):
        """Main conversion method"""
        print(f"Fetching metadata for DOI: {doi}")
        
        # Fetch metadata from CrossRef
        crossref_data = self.fetch_crossref_metadata(doi)
        if not crossref_data:
            print("Failed to fetch metadata from CrossRef")
            return None
        
        # Try to fetch additional metadata from OpenAlex
        print("Attempting to fetch additional metadata from OpenAlex...")
        openalex_data = self.fetch_openalex_metadata(doi)
        if openalex_data:
            print("Successfully retrieved OpenAlex data")
        else:
            print("OpenAlex data not available, using CrossRef data only")
        
        # Create work URI and add metadata
        work_uri = self.create_work_uri(doi)
        
        # Add all metadata components
        self.add_work_metadata(work_uri, crossref_data, openalex_data)
        self.add_authors(work_uri, crossref_data, openalex_data)
        self.add_source(work_uri, crossref_data)
        self.add_open_access(work_uri, crossref_data, openalex_data)
        self.add_funders(work_uri, crossref_data)
        self.add_references(work_uri, crossref_data)
        
        if openalex_data:
            self.add_concepts_from_openalex(work_uri, openalex_data)
        
        return self.graph
    
    def serialize(self, format='turtle'):
        """Serialize the graph to a string"""
        return self.graph.serialize(format=format)
    
    def save_to_file(self, filename, format='turtle'):
        """Save the graph to a file"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(self.serialize(format=format))
        print(f"RDF saved to {filename}")

def main():
    parser = argparse.ArgumentParser(description='Convert DOI metadata to SemOpenAlex RDF format')
    parser.add_argument('doi', help='DOI of the publication (e.g., 10.1038/nature12373)')
    parser.add_argument('-o', '--output', help='Output file (default: output.ttl)', default='output.ttl')
    parser.add_argument('-f', '--format', help='Output format (turtle, xml, json-ld, n3)', default='turtle')
    
    args = parser.parse_args()
    
    # Create converter
    converter = DOIToSemOpenAlexConverter()
    
    # Convert DOI to RDF
    graph = converter.convert(args.doi)
    
    if graph:
        # Save to file
        converter.save_to_file(args.output, format=args.format)
        
        # Also print to console
        print("\n--- Generated RDF (first 100 lines) ---")
        lines = converter.serialize(format=args.format).split('\n')
        for line in lines[:100]:
            print(line)
        if len(lines) > 100:
            print(f"\n... ({len(lines) - 100} more lines)")
    else:
        print("Conversion failed")
        sys.exit(1)

if __name__ == "__main__":
    main()