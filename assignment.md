# Mini-project II

#### [----> Task List <----](#tasks)

In this mini-project, we will combine and practice topics that we have learned in previous two weeks:
- APIs
- Databases (SQL)
- Pandas
- Processing special data types in Python



We will work with these APIs:
1. [Foursquare](https://developer.foursquare.com/places) - we have already encountered this one
2. [Yelp](https://www.yelp.com/developers/documentation/v3/get_started) - this API offers similar services as Foursquare.
3. (Stretch) [Google Places API](https://developers.google.com/places/web-service/intro) - this google api offers similar service as well.

The main goal of this mini-project is to build yourself a database of restaurants, bars, and various points of interest (POIs) in the area of your choice, then compare the quality of the different APIs' coverage of the area. Some of these APIs have a limited number of free requests, so start with a smaller area.  

#### The process will be roughly:

- pull data about POIs in your chosen area through each API. (Search Yelp for businesses that are in the area using [these instructions](https://www.yelp.com/developers/documentation/v3/business_search)). If you run out of requests for any of the APIs, don't worry, it's the approach and process that counts, not the actual number of places you're able to retrieve data on.
- create your own SQLite database and store the data you've collected on the POIs. **Put some thought into the structure of your database.** We've used and created sqlite3 databases before in the activity [**SQL in Python**](https://data.compass.lighthouselabs.ca/b9e08cd5-68c6-490c-a32b-a66f01bf53e1).
- compare the results you got from the different APIs using SQL or Pandas (your choice) and see which API has a better coverage of the area. 
*NOTE:* Your definition of 'coverage' is up to you. It could be simple 'number of POIs in the area', but it could also be something more specific like 'number of reviews per POI', or 'number of different attributes of each POI'.
- get a list of the top 10 POIs based on their popularity (number of reviews *or* average rating) ([Yelp](https://www.yelp.com/developers/documentation/v3/business), [Foursquare](https://developer.foursquare.com/docs/api-reference/venues/details/)).

- (Stretch) By implementing [travelling salesman problem (TSP)](https://en.wikipedia.org/wiki/Travelling_salesman_problem), how much time would it take to visit all of these places? ([Directions API](https://developers.google.com/maps/documentation/directions/start) from google will be helpful here). We will have to find travel time between all places (top 10). We can use [ortools](https://developers.google.com/optimization/routing/tsp) from Google to effectively implement TSP. These tools are very powerful and [easy to install](https://developers.google.com/optimization/install).

## Goals
#### Don't forget your overall learning goals for this project!
- practice retrieving data from APIs (in a variety of formats)
- store data you've retrieved from multiple sources in a SQL database (in this case an SQLite3 database on your local machine)
- do some Exploratory Data Analysis on the data you've retrieved <br>
***Enjoy!***


> #### Note
> Some APIs from Google aren't for free anymore but each account has $200 credit every month. Therefore we're able to use these services for learning purposes for free.


## Tasks
1. complete the [EDA Notebook](notebooks/EDA.ipynb) following the guidance and instructions it contains
2. fill out your [README](README.md) with:
    - a brief description of the project and its goals (2-3 sentences)
    - the steps you took to complete the project (simple description of each step and any difficulties it posed)
    - your results (which API had the best coverage in your chosen area?)
    - brief discussion of challenges you faced in this project (2-4 sentences or list points)
    - (optional) description what you would do with a bit more time
    - use images to spice up your README if you want (put them in your `images/` directory and [link the image](https://www.seancdavis.com/posts/three-ways-to-add-image-to-github-readme/))
3. save the schema of the SQLite database you made to a text file called `database_schema.txt` located in the `data/` directory:
    - open a terminal, navigate to your `data/` directory
    - run the command `sqlite3 [YOUR DATABASE FILE] .schema > database_schema.txt`, replacing [YOUR DATABASE FILE] with the name of your database, for example:
    `sqlite3 poi_data.db .schema > database_schema.txt`
