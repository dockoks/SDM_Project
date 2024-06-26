from neo4j import GraphDatabase
uri = "neo4j://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

def load_constraints(query):
    query.run("""
        CREATE CONSTRAINT Paper_ID IF NOT EXISTS
        FOR (p:Paper) REQUIRE p.ID IS UNIQUE
    """)

    query.run("""
        CREATE CONSTRAINT Author_ID IF NOT EXISTS
        FOR (a:Author) REQUIRE a.ID IS UNIQUE
    """)

    query.run("""
        CREATE CONSTRAINT Volume_ID IF NOT EXISTS
        FOR (v:Volume) REQUIRE v.ID IS UNIQUE
    """)

    query.run("""
        CREATE CONSTRAINT Journal_ID IF NOT EXISTS
        FOR (j:Journal) REQUIRE j.ID IS UNIQUE
    """)

    query.run("""
        CREATE CONSTRAINT Proceeding_ID IF NOT EXISTS
        FOR (pr:Proceeding) REQUIRE pr.ID IS UNIQUE
    """)

    query.run("""
        CREATE CONSTRAINT Edition_ID IF NOT EXISTS
        FOR (e:Edition) REQUIRE e.ID IS UNIQUE
    """)

    query.run("""
        CREATE CONSTRAINT Person_ID IF NOT EXISTS
        FOR (per:Person) REQUIRE per.ID IS UNIQUE
    """)

    print("Loaded all constraints")

def load_nodes(query):
    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Author {ID: row.id, FullName: row.full_name, Birthdate: row.birth_date})
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///chairman_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Person {ID: row.person_id, Name: row.person_name})
    """)
    #
    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///editor_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Person {ID: row.editor_id, Name: row.editor_name})
    """)
    #
    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///journal_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Journal {ID: row.id, Name: row.name})
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///papers_from_journal_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Paper {ID: row.paper_id, Title: row.title, Keywords: row.keywords, Abstract: row.abstract})
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///papers_from_proceeding_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Paper {ID: row.paper_id, Title: row.title, Keywords: row.keywords, Abstract: row.abstract})
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///proceeding_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Proceeding {ID: row.proceeding_id, Name: row.name})
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///edition_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Edition {ID: row.edition_id, Date: row.edition_date, City: row.edition_city})
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///volume_nodes.csv' AS row FIELDTERMINATOR ','
        CREATE (:Volume {ID: row.volume_id, Year: row.year})
    """)

    print("Created all nodes")


def load_relations(query):
    #:Is_corresponding and :Writes relations
    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_IS_CORE_AUTHOR_paper_journal.csv' AS row FIELDTERMINATOR ','
        MERGE (author:Author {ID: row.core_author})
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (author)-[:IS_CORRESPONDING]->(paper)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_IS_CO_AUTHOR_paper_journal.csv' AS row FIELDTERMINATOR ','
        MERGE (author:Author {ID: row.co_author})
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (author)-[:WRITES]->(paper)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_IS_CORE_AUTHOR_paper_proceeding.csv' AS row FIELDTERMINATOR ','
        MERGE (author:Author {ID: row.core_author})
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (author)-[:IS_CORRESPONDING]->(paper)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_IS_CO_AUTHOR_paper_proceeding.csv' AS row FIELDTERMINATOR ','
        MERGE (author:Author {ID: row.co_author})
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (author)-[:WRITES]->(paper)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_REVIEWS_paper_journal.csv' AS row FIELDTERMINATOR ','
        MERGE (author:Author {ID: row.reviewer})
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (author)-[:REVIEWS]->(paper)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_REVIEWS_paper_proceeding.csv' AS row FIELDTERMINATOR ','
        MERGE (author:Author {ID: row.reviewer})
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (author)-[:REVIEWS]->(paper)
    """)

    query.run("""
         LOAD CSV WITH HEADERS FROM 'file:///journal_CONTAINS_VOLUME_volume.csv' AS row FIELDTERMINATOR ','
         MERGE (journal:Journal {ID: row.journal_id})
         MERGE (volume:Volume {ID: row.volume_id})
         MERGE (journal)-[:CONTAINS_VOLUME]->(volume)
     """)

    query.run("""
         LOAD CSV WITH HEADERS FROM 'file:///proceeding_CONTAINS_CONFERENCE_edition.csv' AS row FIELDTERMINATOR ','
         MERGE (proceeding:Proceeding {ID: row.proceeding_id})
         MERGE (conference:Edition {ID: row.edition_id})
         MERGE (proceeding)-[:CONTAINS_CONFERENCE]->(conference)
     """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///proceeding_CONTAINS_WORKSHOP_edition.csv' AS row FIELDTERMINATOR ','
         MERGE (proceeding:Proceeding {ID: row.proceeding_id})
         MERGE (workshop:Edition {ID: row.edition_id})
        MERGE (proceeding)-[:CONTAINS_WORKSHOP]->(workshop)
    """)

    query.run("""
         LOAD CSV WITH HEADERS FROM 'file:///journal_HAS_EDITOR_person.csv' AS row FIELDTERMINATOR ','
         MERGE (journal:Proceeding {ID: row.id})
         MERGE (editor:Person {ID: row.editor_id})
         MERGE (journal)-[:HAS_EDITOR]->(editor)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///proceeding_HAS_CONFERENCE_CHAIRMAN_person.csv' AS row FIELDTERMINATOR ','
        MERGE (proceeding:Proceeding {ID: row.proceeding_id})
        MERGE (chairman:Person {ID: row.chairman})
        MERGE (proceeding)-[:HAS_CHAIRMAN_CONFERENCE]->(chairman)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///proceeding_HAS_WORKSHOP_CHAIRMAN_person.csv' AS row FIELDTERMINATOR ','
        MERGE (proceeding:Proceeding {ID: row.proceeding_id})
        MERGE (chairman:Person {ID: row.chairman})
        MERGE (proceeding)-[:HAS_CHAIRMAN_WORKSHOP]->(chairman)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_CITES_paper_journal.csv' AS row FIELDTERMINATOR ','
        MERGE (paper1:Paper {ID: row.paper_id})
        MERGE (paper2:Paper {ID: row.citation_id})
        MERGE (paper1)-[:CITES]->(paper2)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_CITES_paper_proceeding.csv' AS row FIELDTERMINATOR ','
        MERGE (paper1:Paper {ID: row.paper_id})
        MERGE (paper2:Paper {ID: row.citation_id})
        MERGE (paper1)-[:CITES]->(paper2)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_PUBLISHED_IN_CONFERENCE_edition_proceeding.csv' AS row FIELDTERMINATOR ','
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (edition:Edition {ID: row.edition_id})
        MERGE (paper)-[:PUBLISHED_CONFERENCE]->(edition)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_PUBLISHED_IN_WORKSHOP_edition_proceeding.csv' AS row FIELDTERMINATOR ','
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (edition:Edition {ID: row.edition_id})
        MERGE (paper)-[:PUBLISHED_WORKSHOP]->(edition)
    """)

    query.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_PUBLISHED_IN_VOLUME_journal.csv' AS row FIELDTERMINATOR ','
        MERGE (paper:Paper {ID: row.paper_id})
        MERGE (volume:Volume {ID: row.volume_id})
        MERGE (paper)-[:PUBLISHED_JOURNAL]->(volume)
    """)

    print("Created all relations")

def delete_everything(query):
    query.run("MATCH (n) DETACH DELETE n")
    print("Deleted everything")

def main():
    # Connect to neo4j on local
    with driver.session() as session:
        # Remove all previous nodes and relations, if not at some point it collapses
        session.execute_write(delete_everything)

        # Load first all the nodes, and then all the realtions between nodes
        session.execute_write(load_constraints)
        session.execute_write(load_nodes)
        session.execute_write(load_relations)
    driver.close()

if __name__ == "__main__":
    main()
