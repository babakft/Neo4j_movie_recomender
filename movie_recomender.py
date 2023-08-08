import time
from neo4j import GraphDatabase
from config import URI, CSV_URL, AUTH
import pandas as pd
import ast
from concurrent.futures import ThreadPoolExecutor


class MoveRecommender:
    def __init__(self):
        """Create the whole graph and relation via the csv file"""
        self.driver = GraphDatabase.driver(URI, auth=AUTH, encrypted=True, trust='TRUST_ALL_CERTIFICATES',
                                           max_connection_lifetime=3600 * 24 * 30, keep_alive=True,
                                           max_connection_pool_size=500)
        time.sleep(75)  # waiting to get connected to neo4j azura
        self.driver.verify_connectivity()
        self.df = pd.read_csv(CSV_URL)  # the CSV file that contains all the data
        self.__initials_nodes(csv_dataframe=self.df)
        # check if the initialization already happened in server to don't recreate indexes again
        if self.__is_initialized() is False:
            self.__index_nodes()

        self.__initials_relations(csv_dataframe=self.df)

    def __is_initialized(self):
        query = """
        MATCH (n) RETURN count(n) AS nodeCount;
        """
        if int(self.__execute_query(query)[0]['nodeCount']) == 0:
            return False
        return True

    def __create_movie_node(self, csv_row):
        query = """  
        MERGE (:Movie {
            Title: $Title,
            USER_SCORE: $USER_SCORE,
            Runtime: $Runtime,
            Languages: $Languages,
            METASCORE: $METASCORE,
            Genrs: $Genrs,
            Countries: $Countries,
            url: $url
            })
            """
        self.__execute_query(query, csv_row.to_dict())
        print(f"movie node created successfully")

    def __create_simple_node(self, label, name):
        # create a new node with name property
        query = f"""
            MERGE (:{label} {{Name: $Name}})
            """
        self.__execute_query(query, {'Name': name})
        print(f"{label.lower()} node created")

    def __create_relation(self, movie_title, relation_name, tail_node_label, tail_node_name):
        query = f"""
            MATCH (movie:Movie),(tail:{tail_node_label})
            WHERE movie.Title ="{movie_title.replace('"', '""')}" AND
            tail.Name ="{tail_node_name.replace('"', '""')}"
            MERGE (tail)-[:{relation_name}]->(movie)
                """
        self.__execute_query(query)
        print(f"{tail_node_name}--{relation_name}--->{movie_title}")

    def __initials_nodes(self, csv_dataframe):
        # creating set for director, actor, writer, Genres to avoid using merge in cypher language
        # we use thread to increase the process speed

        def fill_set(input_set, column_name):
            for row in self.df[column_name]:
                row = ast.literal_eval(row)
                for data in row:
                    input_set.add(data)

        # using ThreadPool to run the above def
        director_set = set()
        actor_set = set()
        writer_set = set()
        # fill the set
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(fill_set, director_set, "Director")
            executor.submit(fill_set, actor_set, "Cast")
            executor.submit(fill_set, actor_set, "PrincipleCast")
            executor.submit(fill_set, writer_set, "Writers")

        # creating node base on dict and movie via dataframe

        # getting movie from csv
        movie_list = [movie for index, movie in csv_dataframe.iterrows()]
        # insert the data(nodes) in neo4j database
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(self.__create_movie_node, movie_list)
            executor.map(self.__create_simple_node, ["Actor"] * len(actor_set), actor_set)
            executor.map(self.__create_simple_node, ["Writer"] * len(writer_set), writer_set)
            executor.map(self.__create_simple_node, ["Director"] * len(director_set), director_set)

        # at the end creating the Genre
        Genre = ['Genre', 'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family',
                 'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music', 'Musical', 'Mystery', 'News', 'Reality-TV',
                 'Romance', 'Sci-Fi', 'Short', 'Sport', 'Thriller', 'War', 'Western']

        for genres_name in Genre:
            self.__create_simple_node("Genre", genres_name)

    def __index_nodes(self):
        # keys are the label name and values are the nodes property
        details = {"Movie": "Title",
                   "Director": "Name",
                   "Actor": "Name",
                   "Writer": "Name",
                   }

        for label in details:
            indexing_query = f"""
                    CREATE INDEX {label}_{details[label]} FOR (n:{label}) ON (n.{details[label]})
                    """
            self.__execute_query(indexing_query)
            print(f"INDEX {label}_{details[label]} created")

    def __initials_relations(self, csv_dataframe):
        # relation are from nodes to movie node
        def __create_relations(row):
            # relation for actor nodes
            for actor in ast.literal_eval(row.Cast):
                self.__create_relation(movie_title=row.Title, relation_name="ACTED_IN",
                                       tail_node_label="Actor", tail_node_name=actor)
            for actor in ast.literal_eval(row.PrincipleCast):
                self.__create_relation(movie_title=row.Title, relation_name="ACTED_IN",
                                       tail_node_label="Actor", tail_node_name=actor)

            # create relation for Director
            for director in ast.literal_eval(row.Director):
                self.__create_relation(movie_title=row.Title, relation_name="DIRECTED",
                                       tail_node_label="Director", tail_node_name=director)
            # create relation for Writer
            for writer in ast.literal_eval(row.Writers):
                self.__create_relation(movie_title=row.Title, relation_name="WRITEN",
                                       tail_node_label="Writer", tail_node_name=writer)

            # create relation for Genre
            for genre in ast.literal_eval(row.Genrs):
                self.__create_relation(movie_title=row.Title, relation_name="GENRE",
                                       tail_node_label="Genre", tail_node_name=genre)

        # run a thread for each node in data frame
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(__create_relations, row) for _, row in self.df.iterrows()]

    def __execute_query(self, query, params=None):
        with self.driver.session() as session:
            result = session.run(query, **params) if params else session.run(query)
            return result.data()

    def __recommend_movie(self, movie_title):
        # this query find if any other movie by this actor have been played and return the movies that have more than 4
        # relation
        movie_title = movie_title.replace('"', '""')
        query = (
            f"MATCH (movie:Movie {{Title: '{movie_title}'}})<-[r:WRITEN|DIRECTED|ACTED_IN]-(person),"
            f"(person)-[r2]->(recommended_movie),"
            f"(:Movie {{Title: '{movie_title}'}})<-[r3:GENRE]-(movie_genre),"
            f"(movie_genre)-[r4:GENRE]->(recommended_movie) "
            "WITH recommended_movie, COUNT(DISTINCT r2) + COUNT(DISTINCT r4) AS totalRelations "
            "WHERE totalRelations > 4 "
            "RETURN recommended_movie"
        )
        return self.__execute_query(query)

    def stream_or_download(self, movie_title):
        """When a movie is started or downloaded, this method gives a recommendation"""
        return self.__recommend_movie(movie_title=movie_title)


if __name__ == "__main__":
    recommender = MoveRecommender()