# Equijoin-MapReduce-Hadoop
Write a map-reduce program that will perform equijoin between input files in Python and Java.
The code would take two inputs, one would be the HDFS location of the file on which the equijoin should be performed and other would be the HDFS location of the file,
where the output should be stored.


## Data
We use the movies and ratings of MovieLens dataset to do this tasks. We have more than 3900 movies and 10000 record in ratings. They follows the format below:

1. Movies Dataset: User_ID, Movie_ID, Rating. For example:

1, 1, 4.0
1, 539, 5.0
1, 586, 5.0
1, 588, 5.0
1, 589, 5.0
1, 594, 5.0
1, 616, 5.0
2, 110, 5.0
2, 151, 3.0
2, 260, 5.0
2, 376, 3.0
2, 539, 3.0

2. Movies Dataset: Movie_ID, Name, Attribute. For example:

1, Toy Story (1995), Animation|Childrens|Comedy
2, Jumanji (1995), Adventure|Childrens|Fantasy
3, Grumpier Old Men (1995), Comedy|Romance
4, Waiting to Exhale (1995), Comedy|Drama
5, Father of the Bride Part II (1995), Comedy
6, Heat (1995), Action|Crime|Thriller
7, Sabrina (1995), Comedy|Romance
8, Tom and Huck (1995), Adventure|Childrens
9, Sudden Death (1995), Action
10, GoldenEye (1995), Action|Adventure|Thriller

## Solution
In Java, we need to write two Map classes, one reads the movies file, the other one reads the ratings file, and one reduce class. However, in Python, we only need to write two functions for Map and Reduce.

1. Because we want to join these two files via the movie_id column. Therefore, in the map task, we need to extract the second column of rating file and first column of movie file and make them as a key. The value of Map Phase will be the whole content of a record. For example:
1, 1, 4.0 => (key: 539, value : 1, 1, 4.0)
1, Toy Story (1995), Animation|Childrens|Comedy => (key: 1, value : 1, Toy Story (1995), Animation|Childrens|Comedy)
2. And in the Reduce phase, we will join them via key (1 in this case) and we will write the content to output file:
1, 1, 4.0, 1, Toy Story (1995), Animation|Childrens|Comedy
