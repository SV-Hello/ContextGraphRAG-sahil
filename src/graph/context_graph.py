"""
Neo4j Graph Connection & Schema Management (Sahil)
"""
from neo4j import GraphDatabase
import yaml
from pathlib import Path


"""Connection manager for the Neo4j Context Graph."""
class ContextGraph:
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 user: str = "neo4j", 
                 password: str = "password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.df_tables = None
        self.df_metrics = None

    """Load metrics and tables"""
    def load_data_dictionary(self, excel_path: str):
        import pandas as pd

        df_tables = pd.read_excel(excel_path, sheet_name = 0)
        df_metrics = pd.read_excel(excel_path, sheet_name = 1)

        print("Loading metrics...")
        self._load_metrics(df_metrics)

        print("Loading tables...")
        self._load_tables(df_tables)

        print("Creating hazard concepts...")
        self._create_hazard_concepts(df_metrics, df_tables)

        print("Linking concepts to features...")
        self._link_hazards_to_features()

        print("Data dictionary loaded!")

    """Load (realtime) table rows as Feature nodes"""
    def _load_tables(self, df):

        for _, row in df,iterrows():
            #Value Extration
            table_name = row['Cube Data Model Name']
            name = row['name']
            hazard_type = row['Hazard Type']
            data_type = row['Data Type']
            risk_component = row['Risk component']
            schema_name = row['schema name']
            view = row.get('View', '')
            description_text = row.get('Description', '')

            desc = f"{description_text} | Hazard: {hazard_type} | Data: {data_type} | Risk: {risk_component} | Schema: {schema_name} | View: {view}"

            self.create_feature(
                feature_id=table_name,
                name=name,
                source='database',
                aggregation='real_time',
                description=description
            )

    """Load (static) metric rows as Feature nodes"""
    def _load_metrics(self, df):
        
        for _, row in df.iterrows():
            #Value Extraction
            feature_id = row['Feature ID']
            name = row['Name']
            variable_name = row['Variable Name']
            hazard_type = row['Hazard Type']
            system_type = row['System Type']
            risk_component = row['Risk Component']
            
            #Desc String
            desc = f"Hazard: {hazard_type} |  System: {system_type} | Risk: {risk_component} | Variables: {variable_name}"

            #Build feature node
            self.create_feature(
                feature_id = feature_id,
                name = name,
                source = 'parquet',
                aggregation = 'h3_l8',
                description = desc
            )

    """Generate a synyonym list for known hazard types to improve search"""
    def _get_hazard_synonyms(self, hazard_type: str) -> list:
        synonym_map = {
            'Flood': ['flood', 'flooding', 'deluge'],
            'Fire': ['fire', 'wildfire', 'blaze', 'forest fire', 'brush fire'],
            'Hurricane' : ['hurricane', 'tropical storm', 'cyclone', 'typhoon'],
            'Flood & Rainfall': ['flood', 'flooding', 'rain', 'rainfall', 'precipitation', 'deluge'],
            'Earthquake & Other': ['earthquake', 'seismic', 'tremor', 'quake'],
            'Severe Weather': ['severe weather', 'storm', 'thunderstorm', 'weather'],
            'Multi-hazard': ['multi-hazard', 'multiple hazards', 'compound'],
            'Not hazard-specific': ['general', 'infrastructure', 'baseline']
        }

    """Making unique concept nods for each distinct hazard type"""
    def __create_hazard_concepts(self, df_metrics, df_tables):
        hazard_metrics = df_metrics['Hazard Type'].dropna().unique()
        hazards_tables = df_tables['Hazard Type'].dropna().unique()

        #Combine and deduplicate
        all_hazards = set(list(hazard_metrics) + list(hazards_tables))

        print(f"Creating {len(all_hazards)} hazard concepts...")

        #Creating concept nodes for hazards
        for hazard in all_hazards:
            concept_id = f"hazard_{hazard.lower().replace(' ', '_').replace('%', 'and')}"
            synonyms = self._get_hazard_synonyms(hazard)

            self.create_concept(
                concept_id = concept_id,
                name = hazard,
                synonyms = synonyms
            )

            print("Created: {hazard}")

    """Building the MAPS_TO edges between concepts and features"""
    def _link_hazards_to_features(self):
        query = """
        MATCH (f:Feature)
        WHERE f.description CONTAINS 'HAZARD:'
        WITH f,
            split(f.description, '|')[0] as hazard_part
        WITH f,
            trim(replace(hazard_part, 'Hazard:', '')) as hazard_name
        MATCH (c:Concept)
        WHERE c.name = hazard_name
        MERGE (c)-[:MAPS_TO] -> (f)
        """

        with self.driver.session() as session:
            result = session.run(query)
            summary = result.consume()
            print(f"Created {summary.counters.relationships_created} MAPS_TO relationships")



    def close(self):
        self.driver.close()

    def clear_graph(self):
        """Remove all nodes and edges. Use with caution."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def create_feature(self, feature_id: str, name: str, source: str, 
                       aggregation: str, description: str = ""):
        """Create a Feature node in the Schema Graph."""
        query = """
        MERGE (f:Feature {id: $id})
        SET f.name = $name, f.source = $source, 
            f.aggregation = $agg, f.description = $desc
        """
        with self.driver.session() as session:
            session.run(query, id=feature_id, name=name, 
                       source=source, agg=aggregation, desc=description)

    def create_concept(self, concept_id: str, name: str, synonyms: list):
        """Create a Concept node and link synonyms."""
        query = """
        MERGE (c:Concept {id: $id})
        SET c.name = $name, c.synonyms = $synonyms
        """
        with self.driver.session() as session:
            session.run(query, id=concept_id, name=name, synonyms=synonyms)

    def link_concept_to_feature(self, concept_id: str, feature_id: str):
        """Create MAPS_TO edge between Concept and Feature."""
        query = """
        MATCH (c:Concept {id: $c_id}), (f:Feature {id: $f_id})
        MERGE (c)-[:MAPS_TO]->(f)
        """
        with self.driver.session() as session:
            session.run(query, c_id=concept_id, f_id=feature_id)

    def resolve_concept(self, concept_name: str) -> list:
        """Given a concept name, return linked Feature IDs."""
        query = """
        MATCH (c:Concept)-[:MAPS_TO]->(f:Feature)
        WHERE toLower(c.name) = toLower($name) 
           OR $name IN [s IN c.synonyms | toLower(s)]
        RETURN f.id as feature_id, f.source as source, f.aggregation as agg
        """
        with self.driver.session() as session:
            result = session.run(query, name=concept_name)
            return [dict(record) for record in result]


def load_schema_from_yaml(yaml_path: str = "config/schema.yaml") -> dict:
    """Load schema definition from YAML file."""
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    # Quick test
    print("Testing ContextGraph connection...")
    # graph = ContextGraph()
    # graph.close()
    print("Module loaded successfully. Neo4j connection not tested (requires running instance).")
