# Neo4j Movie Recommender System

A graph-based movie recommendation system built with Neo4j and Python that analyzes relationships between movies, actors, directors, writers, and genres to provide intelligent movie recommendations.

## Overview

This project creates a knowledge graph from movie data and uses Neo4j's graph database capabilities to recommend movies based on shared cast members, crew, and genres. When a user watches or downloads a movie, the system suggests similar movies by analyzing graph patterns and relationship strengths.

## Features

- **Graph Database Architecture**: Utilizes Neo4j to store and query complex movie relationships
- **Multi-threaded Data Loading**: Efficient parallel processing for creating nodes and relationships
- **Smart Recommendations**: Recommends movies based on:
  - Shared actors
  - Same directors
  - Common writers
  - Similar genres
- **Relationship Scoring**: Filters recommendations based on relationship strength (minimum 4 connections)
- **Automatic Indexing**: Creates indexes on key properties for optimized queries

## Graph Structure

The system creates the following node types:
- **Movie**: Contains title, user score, runtime, languages, metascore, genres, countries, and URL
- **Actor**: Represents cast members
- **Director**: Represents film directors
- **Writer**: Represents screenwriters
- **Genre**: Movie genres

## Relationships

- `ACTED_IN`: Actor → Movie
- `DIRECTED`: Director → Movie
- `WRITEN`: Writer → Movie
- `GENRE`: Genre → Movie

## How It Works

1. **Data Loading**: Fetches movie data from CSV source
2. **Node Creation**: Creates movie, actor, director, writer, and genre nodes using multi-threading
3. **Index Creation**: Builds indexes on Title and Name properties for fast lookups
4. **Relationship Building**: Establishes connections between entities
5. **Recommendation Logic**: 
   - Finds movies sharing cast, crew, or genres with the input movie
   - Counts total shared relationships
   - Returns movies with more than 4 common connections

### Recommendation Query

The recommendation algorithm uses Cypher to find movies that share:
- Same actors, directors, or writers
- Similar genres

Movies are ranked by the total number of shared relationships and filtered to return only those with significant overlap (>4 relationships).
