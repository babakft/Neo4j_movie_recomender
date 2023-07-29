import concurrent
import threading

from neo4j import GraphDatabase
from config import URI, CSV_URL, AUTH
import pandas as pd
import ast
import queue
from concurrent.futures import ThreadPoolExecutor


class MoveRecommender:
    def __init__(self):
        """Create the whole graph and relation via the csv file"""
        self.df = pd.read_csv('crawled_movie.csv')
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        # self.__initials_nodes(csv_dataframe=self.df)
        # self.__initials_actors_relations()
        self.__initial_director_relation()

    def __initials_nodes(self, csv_dataframe):
        query = "CREATE (n:Movie{Title:$Title, USER_SCORE:$USER_SCORE, Runtime:$Runtime, Languages:$Languages, " \
                "METASCORE:$METASCORE, Genrs:$Genrs, Countries:$Countries, Writers:$Writers, Cast:$Cast, " \
                "Director:$Director, url:$url})"

        """in this part i'm extracting the movie detail from dataframe and turn to
         a dictionary that contain parameters to create a new node"""
        for index, row in csv_dataframe.iterrows():
            params = dict()
            for i in range(len(row)):
                try:
                    params[row.keys()[i]] = ast.literal_eval(row[i])
                except (ValueError, SyntaxError):
                    params[row.keys()[i]] = row[i]
            print(params)
            self.__execute_query(query, params)

    def __initials_actors_relations(self):
        """getting all the node title and actor"""
        query = "MATCH (n:Movie) RETURN n.Title as title, n.Cast as actor"

        """create a list to get the result"""
        result_list = list()
        for i in self.__execute_query(query):
            result_list.append(i)

        actors_list = list()  # list of created actor

        def __create_actor_relation(movie_dict):
            """we see this actor for the first time ,so we create a node and also a relation for it"""
            for actor in movie_dict['actor']:
                if actor not in actors_list:
                    """we see this actor for the first time"""
                    query = "CREATE (n:Actor{name:$name})"
                    self.__execute_query(query, {"name": actor})

                    actors_list.append(actor)
                    print(f"{actor}: actor node created")

                """ create the relation between actor and the movie"""
                relation_query = "Match (a:Movie),(b:Actor) WHERE a.Title = $Title AND b.name =$Actor " \
                                 "CREATE (b)-[:Acted_in]->(a) "
                self.__execute_query(relation_query, {"Actor": actor, "Title": movie_dict['title']})

        """multi thread the process to increase performance"""
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(__create_actor_relation, result_list)

    def __initial_director_relation(self):
        """getting all the node title and director"""
        query = "MATCH (n:Movie) RETURN n.Title as title, n.Director as director"

        """create a list to get the result"""
        result_list = list()
        for i in self.__execute_query(query):
            result_list.append(i)

        director_list = list()  # list of created Director

        def __create_director_relation(movie_dict):
            """we see this actor for the first time ,so we create a node and also a relation for it"""
            for director in movie_dict['director']:
                if director not in director_list:
                    """we see this director for the first time"""
                    query = "CREATE (n:Director{name:$name})"
                    self.__execute_query(query, {"name": director})

                    director_list.append(director)
                    print(f"{director}: director node created")

                """ create the relation between director and the movie"""
                relation_query = "Match (a:Movie),(b:Director) WHERE a.Title = $Title AND b.name =$Director " \
                                 "CREATE (b)-[:Directed]->(a) "
                self.__execute_query(relation_query, {"Director": director, "Title": movie_dict['title']})

        """multi thread the process to increase performance"""
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(__create_director_relation, result_list)

    def __execute_query(self, query, params=None):
        with self.driver.session() as session:
            result = session.run(query, **params) if params else session.run(query)
            return result.data()

    def stream_or_download(self, title):
        """When a movie is started or downloaded, this method gives a recommendation"""
        pass


MoveRecommender()
