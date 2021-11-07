#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import pymysql
conn = pymysql.connect(host='localhost', user='username, password='password', db='imdb')
curs = conn.cursor(pymysql.cursors.DictCursor)


# In[2]:


movie = input("영화 제목을 입력하세요: ")
movie_df = pd.DataFrame(columns = ['영화 제목', '지역', '언어', '개봉 연도', '런타임(분)'])
start_time = time.time()
movie_search = """select distinct title, region, language, startYear, runtimeMinutes
from title_akas, movie
where title_akas.titleid = movie.titleid and title = '%s';""" % (movie)
curs.execute(movie_search)
row = curs.fetchone()
while row:
    movie_df.loc[len(movie_df)] = [row['title'], row['region'], row['language'], row['startYear'], row['runtimeMinutes']]
    row = curs.fetchone()
elapsed_time = time.time() - start_time
print('소요시간: ', round(elapsed_time, 4), '초')
movie_df


# In[3]:


actor = input("배우 이름을 입력하세요: ")
actor_df = pd.DataFrame(columns = ['영화 제목', '개봉 연도', '배우 이름', '평점'])
start_time = time.time()
actor_search = """select primaryTitle, startYear, primaryName, averageRatings
from movie m, person p, cast c, review r
where c.nconst = p.nconst and c.titleid = m.titleid and r.titleid = m.titleid and primaryName = '%s'
order by averageRatings desc;""" % (actor)
curs.execute(actor_search)
row = curs.fetchone()
while row:
    actor_df.loc[len(actor_df)] = [row['primaryTitle'], row['startYear'], row['primaryName'], row['averageRatings']];
    row = curs.fetchone()
elapsed_time = time.time() - start_time
print('소요시간: ', round(elapsed_time, 4), '초')
actor_df


# In[4]:


director = input("감독 이름을 입력하세요: ")
director_df = pd.DataFrame(columns=['영화 제목', '개봉 연도', '감독 이름'])
start_time = time.time()
director_search = """select primaryTitle, startYear, primaryName
from movie m, directors d, person p
where m.titleid = d.titleid and p.nconst = d.nconst and primaryName = '%s'
order by startYear;""" % (director)
curs.execute(director_search)
row = curs.fetchone()
while row:
    director_df.loc[len(director_df)] = [row['primaryTitle'], row['startYear'], row['primaryName']]

    row = curs.fetchone()
elapsed_time = time.time() - start_time
print('소요시간: ', round(elapsed_time, 4), '초')
director_df


# In[5]:


start_time = time.time()
drama = """select primaryTitle, genre, numVotes 
from genres g, review r, movie m
where g.titleid = m.titleid and r.titleid = m.titleid and genre = 'drama' 
order by numVotes desc;"""
curs.execute(drama)
row = curs.fetchone()
while row:
    print("장르: {}\t영화 제목: {}\t리뷰 수: {}\n" .format(row['genre'],row['primaryTitle'],row['numVotes']))
    row = curs.fetchone()
elapsed_time = time.time() - start_time
print('소요시간: ', round(elapsed_time, 4), '초')


# In[6]:


year = input("검색할 개봉 연도를 입력하세요: ")
start_time = time.time()
year_search = """select title, region, language, startYear, averageRatings
from title_akas t, movie m, review r
where t.titleid = m.titleid and r.titleid = m.titleid and startYear = %s
order by averageRatings desc;""" % (year)
curs.execute(year_search)

row = curs.fetchone()
while row:
    print("개봉 연도: {}\t영화 제목: {}\t 지역: {}\t 언어: {}\t 평점: {}\n" .format(row['startYear'], row['title'], row['region'], row['language'], row['averageRatings']))
    row = curs.fetchone()
elapsed_time = time.time() - start_time
print('소요시간: ', round(elapsed_time, 4), '초')


# In[7]:


curs.close()
conn.close()

