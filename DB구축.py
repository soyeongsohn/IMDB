import pymysql
conn = pymysql.connect(host='localhost', user='username', password='password', db='imdb')
curs = conn.cursor(pymysql.cursors.DictCursor)


# # 파일 읽기
# - 파일을 읽어 사전에 설계한 데이터베이스에 맞게 재조합해 데이터를 import하기 위함

# In[2]:


import pandas as pd

# title.akas.tsv
title_akas = pd.read_csv("/Users/USER/Downloads/title.akas.tsv/data.tsv", na_values='\\N', sep='\t', dtype={'attributes': str})
title_akas = title_akas.astype(object).where(pd.notnull(title_akas), None)
print("Complete: title.akas.tsv")
# title.basics.tsv
title_basics = pd.read_csv("/Users/USER/Downloads/title.basics.tsv/data.tsv", na_values='\\N', sep='\t')
title_basics = title_basics.astype(object).where(pd.notnull(title_basics), None)
print("Complete: title.basics.tsv")
#title.crew.tsv
title_crew = pd.read_csv("/Users/USER/Downloads/title.crew.tsv/data.tsv", na_values='\\N', sep='\t')
title_crew = title_crew.astype(object).where(pd.notnull(title_crew), None)
print("Complete: title.crew.tsv")
# title.episode.tsv
title_episode = pd.read_csv("/Users/USER/Downloads/title.episode.tsv/data.tsv", na_values='\\N', sep='\t')
title_episode = title_episode.astype(object).where(pd.notnull(title_episode), None)
print("Complete: title.episode.tsv")
# title.principals.tsv
title_principals = pd.read_csv("/Users/USER/Downloads/title.principals.tsv/data.tsv", na_values='\\N', sep='\t')
title_principals = title_principals.astype(object).where(pd.notnull(title_principals), None)
print("Complete: title.principals.tsv")
# title.ratings.tsv
title_ratings = pd.read_csv("/Users/USER/Downloads/title.ratings.tsv/data.tsv", na_values='\\N', sep='\t')
title_ratings = title_ratings.astype(object).where(pd.notnull(title_ratings), None)
print("Complete: title.ratings.tsv")
# name.basics.tsv
name_basics = pd.read_csv("/Users/USER/Downloads/name.basics.tsv/data.tsv", na_values='\\N', sep='\t')
name_basics = name_basics.astype(object).where(pd.notnull(name_basics), None)
print("Complete: name.basics.tsv")


# # 테이블 생성
# - 사전에 설계한 데이터베이스 모델에 맞게 테이블 생성

# In[18]:


# movie
create_movie = """CREATE TABLE movie (
    titleid varchar(20) not null,
    titleType varchar(15),
    primaryTitle varchar(2048),
    originalTitle varchar(2048),
    isAdult boolean,
    startYear int,
    endYear int,
    runtimeMinutes int
    );"""
curs.execute(create_movie)


# In[6]:


# person
create_person = """CREATE TABLE person (
    nconst varchar(20) not null,
    primaryName varchar(200) not null,
    birthYear int,
    deathYear int
    );"""
curs.execute(create_person)


# In[20]:


# akas
create_akas = """CREATE TABLE title_akas (
    titleid varchar(20) not null,
    ordering int not null,
    title varchar(2048) not null,
    region varchar(30),
    language varchar(30),
    isOriginalTitle varchar(10)
    );"""
curs.execute(create_akas)


# In[21]:


# types
create_types = """CREATE TABLE types (
    titleid varchar(20) not null,
    ordering int not null,
    type varchar(200) not null
    );"""
curs.execute(create_types)


# In[22]:


# attributes
create_attributes = """CREATE TABLE attributes (
    titleid varchar(20) not null,
    ordering int not null,
    attribute varchar(200) not null
    );"""
curs.execute(create_attributes)


# In[23]:


# genres
create_genres = """CREATE TABLE genres (
    titleid varchar(20) not null,
    genre varchar(200) not null
    );"""
curs.execute(create_genres)


# In[15]:


# review
create_review = """CREATE TABLE review (
    reviewid int not null auto_increment primary key,
    titleid varchar(20), 
    averageRatings float,
    numVotes int
    );"""
curs.execute(create_review)


# In[26]:


# episode
create_episode = """CREATE TABLE episode (
    titleid varchar(20) not null,
    parentid varchar(20) not null,
    seasonNumber int,
    episodeNumber int
    );"""
curs.execute(create_episode)


# In[27]:


# directors
create_directors = """CREATE TABLE directors (
    titleid varchar(20) not null,
    nconst varchar(20) not null
    );"""
curs.execute(create_directors)


# In[28]:


# writers
create_writers = """CREATE TABLE writers (
    titleid varchar(20) not null,
    nconst varchar(20) not null
    );"""
curs.execute(create_writers)


# In[8]:


# known_for
create_known_for = """CREATE TABLE known_for (
    nconst varchar(20) not null,
    titleid varchar(20) not null
    );"""
curs.execute(create_known_for)


# In[6]:


# cast
create_cast = """CREATE TABLE cast (
    titleid varchar(20) not null,
    nconst varchar(20) not null,
    characters varchar(2048)
    );"""
curs.execute(create_cast)


# In[31]:


# principals
create_principals = """CREATE TABLE principals (
    titleid varchar(20) not null,
    ordering int not null,
    nconst varchar(20) not null,
    category varchar(2048),
    job varchar(2048)
    );"""
curs.execute(create_principals)


# In[32]:


# profession
create_profession = """CREATE TABLE profession (
    nconst varchar(20) not null,
    profession varchar(200) not null
    );"""
curs.execute(create_profession)


# # 데이터 import
# - 설계한 데이터베이스에 맞게 불러온 7개의 테이블 데이터를 조정하여 import

# In[36]:


import time
def load_movie():   
    
    movie = title_basics[['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes']]
    
    
    insert_sql = """insert into movie (titleid, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes)
                    values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    rows = []

    for i in range(len(movie)):
        attr = tuple(movie.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(movie)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_movie()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[8]:


def load_person():   
    
    person = name_basics[['nconst', 'primaryName', 'birthYear', 'deathYear']]
    
    
    insert_sql = """insert into person (nconst, primaryName, birthYear, deathYear)
                    values (%s, %s, %s, %s)"""
    
    rows = []

    for i in range(len(person)):
        attr = tuple(person.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(person)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_person()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[22]:


def load_akas():   
    
    akas = title_akas[['titleId', 'ordering', 'title', 'region', 'language', 'isOriginalTitle']]
    
    
    insert_sql = """insert into title_akas (titleid, ordering, title, region, language, isOriginalTitle)
                    values (%s, %s, %s, %s, %s, %s)"""
    
    rows = []

    for i in range(len(akas)):
        attr = tuple(akas.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(akas)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_akas()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[25]:


def load_types():   
    
    types = title_akas[['titleId', 'ordering', 'types']]
    types = types.dropna()
    
    
    insert_sql = """insert into types (titleid, ordering, type)
                    values (%s, %s, %s)"""
    
    rows = []

    for i in range(len(types)):
        attr = tuple(types.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(types)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_types()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[27]:


def load_attributes():   
    
    attributes = title_akas[['titleId', 'ordering', 'attributes']]
    attributes = attributes.dropna()
    
    insert_sql = """insert into attributes (titleid, ordering, attribute)
                    values (%s, %s, %s)"""
    
    rows = []

    for i in range(len(attributes)):
        attr = tuple(attributes.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(attributes)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_attributes()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[29]:


def load_genres():   
    
    genre = title_basics[['tconst', 'genres']]
    genre = genre.dropna()
    genre = genre.assign(genres=genre.genres.str.split(',')).explode('genres').reset_index(drop=True)
    
    insert_sql = """insert into genres (titleid, genre)
                    values (%s, %s)"""
    
    rows = []
 
    for i in range(len(genre)):
        attr = tuple(genre.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(genre)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_genres()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[16]:


def load_review():   
    review = title_ratings
    insert_sql = """insert into review (titleid, averageRatings, numVotes)
                    values (%s, %s, %s)"""
    
    rows = []

    for i in range(len(review)):
        attr = tuple(review.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(review)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_review()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[31]:


def load_episode():   
    
    episode = title_episode
    
    
    insert_sql = """insert into episode (titleid, parentid, seasonNumber, episodeNumber)
                    values (%s, %s, %s, %s)"""
    
    rows = []

    for i in range(len(episode)):
        attr = tuple(episode.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(episode)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_episode()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[35]:


def load_directors():   
    
    director = title_crew[['tconst', 'directors']]
    director = director.dropna()
    director = director.assign(directors=director.directors.str.split(',')).explode('directors').reset_index(drop=True)
    
    insert_sql = """insert into directors (titleid, nconst)
                    values (%s, %s)"""
    
    rows = []

    for i in range(len(director)):
        attr = tuple(director.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(director)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_directors()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[36]:


def load_writers():   
    
    writer = title_crew[['tconst', 'writers']]
    writer= writer.dropna()
    writer = writer.assign(writers=writer.writers.str.split(',')).explode('writers').reset_index(drop=True)
    
    insert_sql = """insert into writers (titleid, nconst)
                    values (%s, %s)"""
    
    rows = []

    for i in range(len(writer)):
        attr = tuple(writer.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(writer)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_writers()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[10]:


def load_known_for():   
    
    known_for = name_basics[['nconst', 'knownForTitles']]
    known_for = known_for.dropna()
    known_for = known_for.assign(knownForTitles=known_for.knownForTitles.str.split(',')).explode('knownForTitles').reset_index(drop=True)
    
    insert_sql = """insert into known_for (nconst, titleid)
                    values (%s, %s)"""
    
    rows = []
 
    for i in range(len(known_for)):
        attr = tuple(known_for.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(known_for)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_known_for()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[14]:


def load_cast():   
    
    cast = title_principals[['tconst', 'nconst', 'characters']]
    cast = cast.dropna()
    cast['characters'] = cast['characters'].str.replace('[\"\[\]]','',regex=True).replace('\\', '|')
    cast = cast.assign(characters=cast.characters.str.split(',')).explode('characters').reset_index(drop=True)
    cast = cast.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=True)
    
    insert_sql = """insert into cast (titleid, nconst, characters)
                    values (%s, %s, %s)"""
    
    rows = []

    for i in range(len(cast)):
        attr = tuple(cast.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(cast)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_cast()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[39]:


def load_principals():   
    
    principals = title_principals[['tconst', 'ordering', 'nconst', 'category', 'job']]
    
    
    insert_sql = """insert into principals (titleid, ordering, nconst, category, job)
                    values (%s, %s, %s, %s, %s)"""
    
    rows = []

    for i in range(len(principals)):
        attr = tuple(principals.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(principals)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_principals()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[43]:


def load_profession():   
    
    professions = name_basics[['nconst', 'primaryProfession']]
    professions = professions.dropna()
    professions = professions.assign(primaryProfession=professions.primaryProfession.str.split(',')).explode('primaryProfession').reset_index(drop=True)
    
    insert_sql = """insert into profession (nconst, profession)
                    values (%s, %s)"""
    
    rows = []

    for i in range(len(professions)):
        attr = tuple(professions.iloc[i])
        rows.append(attr)
        if i % 10000 == 0:
            curs.executemany(insert_sql, rows)
            conn.commit()
            rows = []
        if i % 1000000 == 0:
            print("{} / {} complete" .format(i, len(professions)))
        
    if rows:
        curs.executemany(insert_sql, rows)
        conn.commit()

if __name__ == '__main__':
    start_time = time.time()
    load_profession()
    elapsed_time = time.time() - start_time
    print(elapsed_time, 'seconds')


# In[46]:


del title_akas
del title_basics
del title_crew
del title_episode
del title_principals
del title_ratings
del name_basics


# # Constraint 삽입
# 외래키, 기본키 등 <br/>
# 참조 무결성 문제를 해결하기 위해 MySQL에서 부모키에 없는 값들은 제거하였음. <br/>
# 예시 delete from title_akas where title_akas.titleid not in (select titleid from movie);

# In[ ]:


# movie
pri_movie = "ALTER TABLE movie ADD CONSTRAINT movie_primary PRIMARY KEY (titleid);"
curs.execute(pri_movie)

# person
pri_person = "ALTER TABLE person ADD CONSTRAINT person_primary PRIMARY KEY (nconst);"
curs.execute(pri_person)

# title_akas
pri_akas = "ALTER TABLE title_akas ADD CONSTRAINT akas_primary PRIMARY KEY (titleid, ordering);"
curs.execute(pri_akas)
for_akas = "ALTER TABLE title_akas ADD CONSTRAINT akas_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_akas)

# types
pri_types = "ALTER TABLE types ADD CONSTRAINT types_primary PRIMARY KEY (titleid, ordering);"
curs.execute(pri_types)
for_types = "ALTER TABLE types ADD CONSTRAINT types_foreign FOREIGN KEY (titleid) REFERENCES title_akas(titleid);"
curs.execute(for_types)

# attributes
pri_attributes = "ALTER TABLE attributes ADD CONSTRAINT attributes_primary PRIMARY KEY (titleid, ordering);"
curs.execute(pri_attributes)
for_attributes = "ALTER TABLE attributes ADD CONSTRAINT attributes_foreign FOREIGN KEY (titleid) REFERENCES title_akas(titleid);"
curs.execute(for_attributes)

# genres
pri_genres = "ALTER TABLE genres ADD CONSTRAINT genres_primary PRIMARY KEY (titleid, genre);"
curs.execute(pri_genres)
for_genres = "ALTER TABLE genres ADD CONSTRAINT genres_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_genres)

#review
for_review = "ALTER TABLE review ADD CONSTRAINT review_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_review)

# episode
pri_episode = "ALTER TABLE episode ADD CONSTRAINT episode_primary PRIMARY KEY (titleid);"
curs.execute(pri_episode)
for_episode = "ALTER TABLE episode ADD CONSTRAINT episode_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_episode)
for_episode1 = "ALTER TABLE episode ADD CONSTRAINT episode_foreign1 FOREIGN KEY (parentid) REFERENCES movie(titleid);"
curs.execute(for_episode1)

# directors
pri_directors = "ALTER TABLE directors ADD CONSTRAINT directors_primary PRIMARY KEY (titleid, nconst);"
curs.execute(pri_directors)
for_directors = "ALTER TABLE directors ADD CONSTRAINT directors_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_directors)
for_directors1 = "ALTER TABLE directors ADD CONSTRAINT directors_foreign1 FOREIGN KEY (nconst) REFERENCES person(nconst);"
curs.execute(for_directors1)

# writers
pri_writers = "ALTER TABLE writers ADD CONSTRAINT writers_primary PRIMARY KEY (titleid, nconst);"
curs.execute(pri_writers)
for_writers = "ALTER TABLE writers ADD CONSTRAINT writers_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_writers)
for_writers1 = "ALTER TABLE writers ADD CONSTRAINT writers_foreign1 FOREIGN KEY (nconst) REFERENCES person(nconst);"
curs.execute(for_writers1)

# known_for
pri_known_for = "ALTER TABLE known_for ADD CONSTRAINT known_for_primary PRIMARY KEY (nconst, titleid);"
curs.execute(pri_known_for)
for_known_for = "ALTER TABLE known_for ADD CONSTRAINT known_for_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_known_for)
for_known_for1 = "ALTER TABLE known_for ADD CONSTRAINT known_for_foreign1 FOREIGN KEY (nconst) REFERENCES person(nconst);"
curs.execute(for_known_for1)

# cast
pri_cast = "ALTER TABLE cast ADD CONSTRAINT cast_primary PRIMARY KEY (titleid, nconst, characters(255));" 
curs.execute(pri_cast)
for_cast = "ALTER TABLE cast ADD CONSTRAINT cast_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_cast)
for_cast1 = "ALTER TABLE cast ADD CONSTRAINT cast_foreign1 FOREIGN KEY (nconst) REFERENCES person(nconst);"
curs.execute(for_cast1)

# principals
pri_principals = "ALTER TABLE principals ADD CONSTRAINT principals_primary PRIMARY KEY (titleid, ordering);"
curs.execute(pri_principals)
for_principals = "ALTER TABLE principals ADD CONSTRAINT principals_foreign FOREIGN KEY (titleid) REFERENCES movie(titleid);"
curs.execute(for_principals)
for_principals = "ALTER TABLE principals ADD CONSTRAINT principals_foreign1 FOREIGN KEY (nconst) REFERENCES person(nconst);"
curs.execute(for_principals)

# profession
pri_profession = "ALTER TABLE profession ADD CONSTRAINT profession_primary PRIMARY KEY (nconst, profession);"
curs.execute(pri_profession)
for_profession = "ALTER TABLE profession ADD CONSTRAINT profession_foreign FOREIGN KEY (nconst) REFERENCES person(nconst);"
curs.execute(for_profession)


# # 인덱스 생성
# - 검색에 사용할 애트리뷰트에만 인덱스를 생성하였음

# In[13]:


movie_index = 'create index movie_index on movie (startYear);'
curs.execute(movie_index)
akas_index = 'create index akas_index on title_akas (title(255));'
curs.execute(akas_index)
person_index = 'create index person_index on person (primaryName);'
curs.execute(person_index)
genre_index = 'create index genre_index on genres (genre);'
curs.execute(genre_index)
vote_index = 'create index vote_index on review (titleid, numVotes);'
curs.execute(vote_index)
rating_index = 'create index rating_index on review (titleid, averageRatings);'
curs.execute(rating_index)

