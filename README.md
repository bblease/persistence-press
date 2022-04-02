<img src="/media/embedded_logo.svg" height=150/>

Robo 🤖 news aggregator powered by NLP topic clustering. Headlines without the bloat. A clean, news-driven website designed to provide users without bloated design.

## 📰 News Sources

- News API (https://newsapi.org/)
- Mediastack (https://mediastack.com/)
- Yahoo Finance API (https://www.yahoofinanceapi.com/)

## ▶️ Usage

To start the Docker stack run 

`make up`

To stop the stack and bring down the containers

`make down` 
or 
`docker-compose down`

To install the frontend for development without running in Docker, run

`yarn`

in the `/frontend` directory, and start the app with

`yarn start`

see the frontend directory readme for further guides

## 🛠️ Architecture

The goal of the Persistence Press architecture is to allow swappable news ingest and modeling with Kafka as a central message broker. Clustering will also be integrated as a swappable service down the line

![Architecture](https://github.com/bblease/persistence-press/blob/master/keyscreens/Architecture.png)
