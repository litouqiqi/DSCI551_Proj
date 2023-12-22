# readme of NoSQL Database System

## File Structure & Description

The file structure of the project is shown as follows.

```
> Proj
	> DB1
		> Album
		> Artist
			Artist_1.xml
			Artist_2.xml
			...
			Artist_6.xml
		> Track
		collInfos.txt
	> pmydb
		> core
			collection.py
			database.py
			mergeSort.py
			mergeAggr.py
			joinColls.py
	parse.py
	ui.py
	requirements.txt
	artists.txt
	albums.txt
	tracks.txt
```

Among them, the DB1 folder corresponds to the database named DB1, and the file collInfos.txt stores metadata of all collections in the database, including name, corresponding entity name (usually the lowercase of the collection name), primary key, and list field. The sub folders (Album, Artist, Track) are the names of the three collections in the DB1 database, which contain all the data stored in the collection, denoted as "collname_num.xml".

Files albums.txt, artist.txt, and tracks.txt are the raw data for three example collections. File requirements.txt contains libraries utilized in the project, including dictoxml (converting a dict to a serialized xml), xmltodict (converting a serialized xml to a dict), lxml (providing access to libraries using the ElementTree API, extending the ElementTree API significantly to offer support for XPath), prompt_toolkit (generating command line interface).

Below, I will explain the functions implemented by each. py program in the database system and the technology stacks used.

### collection.py

This program is mainly used to define the Collection class and utilizes the library lxml and XPath node localization to assist in manipulating XML data. Methods of the Collection class include:

reading data from existing XML/txt file, writing data to XML/txt file, outputing metadata of collection object;

add/remove fields to all documents in the collection, inserting/deleting/updating documents;

query on collection object, data processing pipeline: filtering, grouping, aggregation, ordering, projection, distinct, offset&limit.

### database.py

This program is mainly used to define the Database class. Methods of the Collection class include:

reading the metadata of each collection in the database from the collInfos.txt file;

creating/droping a collection.

### mergeSort.py

This program is used to concatenate 2 lists of documents obtained from queries on two Collection objects (excluding grouping and aggregation), with parameter sort specifying the order of documents on key-value pair. It would write a final set of sorted documents into a target file.

### mergeAggr.py

This program is used to concatenate 2 lists of documents obtained from grouping and aggregation of two Collection objects, with parameters specifying the order of documents on key-value pair, the aggregation function and the grouping fields. It would write a final set of merged and aggregated documents into a target file.

### joinColls.py

This program is used to join 2 lists of documents obtained from querying Collection objects of two different entities together, with parameters specifying the left & right list of documents and fields for joining in both sides. It would write a final set of joined documents into a target file.

### ui.py

This program calls Python's prompt_toolkit library is used to implement an interface for users to input commands and view system output in database systems. After receiving the user command, ui.py takes the command as a string as input to parse() in parse.py, and prints the return value of the latter to the interface.

### parse.py

The core of this program is parse(), which could parse command sent from ui.py, define objects of class Collection/Database and execute their CRUD methods, allocate temporary file to store intermediate data, and return the results to ui.py.

## Commands

After opening the proj folder in the terminal, execute

```
pip install -r requirements.txt
```
to download the Python library that the project imports.

Next, Run ui.py to enter commands like follows after the prefix of "QS-MyDBSystem~". When you need to exit the database system, simply enter "exit".

The lines starting with # is the explanatory notes or SQL queries corresponding to the nearby commands, please do not input them into the database system. Some of the outputs of these commands are shown in the PDF report of NoSQL database system.

```sql
transfer to DB1
DB1$showCollections()

# collection artist does not exist
DB1$dropCollection(name='artist')
DB1$createCollection(name='ArtistTest', priKey=['id'], listField='album')
DB1$showCollections()

# primary key constraint
DB1$Track$insertDoc(doc={'id': '0bI7K9Becu2dtXK1Q3cZNB', 'name': 'Dont Be Shy', 'danceability': 0.77, 'energy': 0.787, 'key': 11, 'class': 'song'})

DB1$Track$insertDoc(doc={'id': 'testid', 'name': 'Dont Be Shy', 'danceability': 0.77, 'energy': 0.787, 'key': 11, 'class': 'song'})
DB1$Track$deleteDocs(filter=[id='testid'])

DB1$Album$addField(name='test', value=8)
DB1$Album$search(filter=[all], limit=5)   # all docs with a new field test = 8
DB1$Album$deleteField(name='test')
DB1$Album$search(filter=[all], limit=5)   # no more field test


# SELECT * FROM Track WHERE key<6 OR key>9 LIMIT 5 OFFSET 3;
DB1$Track$search(filter=[key<6 or key>9], offset=3, limit=5)

# SELECT country AS c, name AS artistName FROM Artist WHERE verified='T'
# ORDER BY country ASC, name DESC LIMIT 10;
DB1$Artist$search(filter=[verified='T'], sort={'country':False, 'name':True}, project={'country':'c', 'name':'artistName'}, limit=10)

# SELECT DISTINCT country AS c FROM Artist
# WHERE verified = 'T' AND (country='UK' OR country='Brazil');
DB1$Artist$search(filter=[verified='T' and (country='UK' or country='Brazil')], project={'country':'c'}, distinct=True)

# SELECT COUNT(id) AS count_id FROM Album WHERE rlsMonth like '%e%'
# GROUP BY type, rlsMonth ORDER BY type ASC, count_id DESC;
DB1$Album$search(filter=[contains(rlsMonth, 'e')], groupOn=['type', 'rlsMonth'], func='count', aggrField='id', sort={'type':False, 'count_id':True})

# SELECT AVG(key) AS avg_key FROM Track GROUP BY class ORDER BY avg_key;
DB1$Track$search(filter=[all], groupOn=['class'], func='avg', aggrField='key', sort={'avg_key':False})


# SELECT *
# FROM (SELECT name, country AS c, album FROM Artist
#	WHERE country = 'UK' ORDER BY name DESC) A
# JOIN (SELECT id, name, rlsYear AS releaseYear FROM Album
#	WHERE rlsMonth = 'Jun' ORDER BY rlsYear) B ON A.album = B.id; 
DB1$Artist$search(filter=[country='UK'], sort={'name':True}, project={'name':'name', 'country': 'c', 'album':'album'})$join(foreignColl=DB1.Album.search(filter=[rlsMonth='Jun'], sort={'rlsYear':False}, project={'id':'id', 'name':'name', 'rlsYear': 'releaseYear'}), localField='album',foreignField='id')


exit
```