import json
from typing import Dict, Any
import json
import yaml
import os
from langchain.schema import Document
from langchain.tools import Tool

def load_dbt_manifest(manifest_path):

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    return manifest

def load_schema_yml(schema_yml_path):
    try:
        with open(schema_yml_path, 'r') as f:
            schema_data = yaml.safe_load(f)
        return schema_data
    except Exception as e:
        print(f"Warning: Could not load schema.yml from {schema_yml_path}: {e}")
        return {}

def extract_relationships_from_schema(schema_data):
    relationships = {}
    
    if not schema_data or 'models' not in schema_data:
        return relationships
    
    for model in schema_data['models']:
        model_name = model.get('name', '')
        if not model_name.startswith('mart_'):
            continue
            
        model_relationships = {
            'foreign_keys': [],
            'unique_keys': [],
            'not_null_columns': []
        }
        
        columns = model.get('columns', [])
        for column in columns:
            column_name = column.get('name', '')
            tests = column.get('tests', [])
            
            for test in tests:
                if isinstance(test, dict):
                    if 'relationships' in test:
                        rel_info = test['relationships']
                        # Extract referenced table from ref() function
                        to_ref = rel_info.get('to', '')
                        if 'ref(' in to_ref:
                            # Extract table name from ref('table_name')
                            ref_table = to_ref.split("'")[1] if "'" in to_ref else to_ref
                            model_relationships['foreign_keys'].append({
                                'column': column_name,
                                'references_table': ref_table,
                                'references_column': rel_info.get('field', '')
                            })
                elif test == 'unique':
                    model_relationships['unique_keys'].append(column_name)
                elif test == 'not_null':
                    model_relationships['not_null_columns'].append(column_name)
        
        relationships[model_name] = model_relationships
    
    return relationships

def load_schema_document(manifest_path):
    
    manifest = load_dbt_manifest(manifest_path)
    
    # Try to find and load the schema.yml file
    manifest_dir = os.path.dirname(manifest_path)
    project_root = os.path.dirname(manifest_dir)  # Go up from target/ to project root
    schema_yml_path = os.path.join(project_root, 'models', 'marts', 'schema.yml')
    
    schema_data = load_schema_yml(schema_yml_path)
    relationships = extract_relationships_from_schema(schema_data)
    
    # Extract relevant schema information for mart_ tables only
    schema_info = {
        "database_type": "duckdb",
        "database": "db",
        "schema": "main",
        "tables": {},
        "relationships": relationships,
        "summary": "DuckDB database containing healthcare claims data with dimension and fact tables"
    }
    
    # Extract model information for tables with mart_ prefix only
    for node_id, node in manifest.get("nodes", {}).items():
        if (node.get("resource_type") == "model" and 
            node.get("name", "").startswith("mart_")):
            
            table_name = node.get("name")
            
            # Extract column information
            columns = {}
            for col_name, col_info in node.get("columns", {}).items():
                columns[col_name] = {
                    "name": col_name,
                    "description": col_info.get("description", ""),
                    "data_type": col_info.get("data_type", "unknown"),
                    "constraints": col_info.get("constraints", [])
                }
            
            # Determine table type based on naming convention
            table_type = "dimension" if "dim_" in table_name else "fact"
            
            # Add table information
            schema_info["tables"][table_name] = {
                "name": table_name,
                "type": table_type,
                "description": node.get("description", ""),
                "columns": columns,
                "database": node.get("database"),
                "schema": node.get("schema"),
                "alias": node.get("alias"),
                "full_name": f'"{node.get("database")}"."{node.get("schema")}"."{table_name}"',
                "relationships": relationships.get(table_name, {})
            }
    
    # Create a comprehensive description for the AI
    description_parts = [
        f"DuckDB Database Schema - Healthcare Claims Data",
        f"Database: {schema_info['database']} | Schema: {schema_info['schema']}",
        "",
        "TABLES OVERVIEW:",
        f"Total mart tables: {len(schema_info['tables'])}",
        ""
    ]
    
    # Add table descriptions
    for table_name, table_info in schema_info["tables"].items():
        table_type = table_info["type"].upper()
        col_count = len(table_info["columns"])
        relationships_info = table_info["relationships"]
        
        description_parts.extend([
            f"• {table_name} ({table_type} TABLE)",
            f"  - Columns: {col_count}",
            f"  - Full name: {table_info['full_name']}",
        ])
        
        # Add relationship information
        if relationships_info.get('foreign_keys'):
            description_parts.append("  - Foreign Keys:")
            for fk in relationships_info['foreign_keys']:
                description_parts.append(f"    • {fk['column']} → {fk['references_table']}.{fk['references_column']}")
        
        if relationships_info.get('unique_keys'):
            description_parts.append(f"  - Unique Keys: {', '.join(relationships_info['unique_keys'])}")
        
        description_parts.append("")
    
    # Add detailed schema as JSON 
    description_parts.extend([
        "DETAILED SCHEMA (JSON):",
        json.dumps(schema_info, indent=2)
    ])
    
    schema_content = "\n".join(description_parts)
    
    return Document(
        page_content=schema_content,
        metadata={
            "source": "dbt_manifest", 
            "type": "duckdb_schema_information",
            "database": "db",
            "schema": "main",
            "table_count": len(schema_info["tables"]),
            "tables": list(schema_info["tables"].keys())
        }
    )

def get_schema_info_tool(manifest_path):
    """Create a tool for retrieving schema information."""
    schema_doc = load_schema_document(manifest_path)
    
    def get_schema_info(query = ""):

        return f"DuckDB Healthcare Claims Database Schema:\n\n{schema_doc.page_content}"
    
    return Tool(
        name="get_schema_info",
        description="Get DuckDB database schema information including mart_ table structures, column definitions, relationships from schema.yml, and model descriptions from the dbt manifest. Specifically designed for healthcare claims data analysis.",
        func=get_schema_info
    )