'''
1. Установите MongoDB на локальной машине, а также зарегистрируйтесь в онлайн-сервисе.
2. Загрузите данные который вы получили на предыдущем уроке путем скрейпинга сайта с помощью Buautiful Soup в MongoDB и 
создайте базу данных и коллекции для их хранения.
3. Поэкспериментируйте с различными методами запросов.
'''
import json
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')
db = client['books_toscrape_com']
collection = db['books']

with open('books.json', 'r', encoding='utf-8') as f:
   books_data = json.load(f)

collection.insert_many(books_data)

# первая запись
print(collection.find()[0])

# количество документов в коллекции
print(collection.count_documents({}))

# фильтрация документов по критериям
query = {'category': 'Travel'}
print(collection.count_documents(query))

# Использование проекции
query = {'category': 'Travel'}
projection = {"_id": 0, "name": 1, "price": 1, "available": 1}
for rec in collection.find(query, projection):
   print(rec)

# Использование оператора $lt и $gte
MIN = 2
MAX = 20
query = {"available": {"$lt": MIN}}
print(f"Книги в наличии меньше {MIN}: {collection.count_documents(query)}")
query = {"available": {"$gte": MAX}}
print(f"Книги в наличии не меньше {MAX}: {collection.count_documents(query)}")

# Использование оператора $regex
WORD = "Moon"
query = {"name": {"$regex": WORD, "$options": "i"}}
print(f"Количество книг, в названии которых есть слово '{WORD}': {collection.count_documents(query)}")

# Использование оператора $in
query = {"category": {"$in": ["Travel", "Romance", "Classic"]}}
print(collection.count_documents(query))

# Использование оператора $all
query = {"category": {"$all": ["Mystery"]}}
print(collection.count_documents(query))

# Использование оператора $ne
query = {"category" : {"$ne": "Mystery"}}
print(collection.count_documents(query))

#=======================================================================================================================
'''
4. Зарегистрируйтесь в ClickHouse.
5. Загрузите данные в ClickHouse и создайте таблицу для их хранения.
'''
import clickhouse_connect
import pandas as pd
import os
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


client = clickhouse_connect.get_client(
    host=os.getenv('host'),
    user=os.getenv('user'),
    password=os.getenv('password'),
    secure=True
)

# Создание базы данных (если она не существует)
client.command('CREATE DATABASE IF NOT EXISTS books_toscrape_com')

# Создание таблицы
client.command('''
CREATE TABLE IF NOT EXISTS books_toscrape_com.books (
    id UInt32,
    category String,
    name String,
    url String,
    price Float32,
    available Int16,
    description String,
    PRIMARY KEY(id)
) ENGINE = MergeTree
ORDER BY id
''')

# заполнение таблицы
column_names = ["id", "category", "name", "url", "price", "available", "description"]
data = [[i]+list(book.values()) for i,book in enumerate(books_data,1)]
client.insert('books_toscrape_com.books', data, column_names=column_names)

# первая запись
result = client.query("SELECT * FROM books_toscrape_com.books")
print(result.result_set[0])

# книги в категории
result = client.query("SELECT * FROM books_toscrape_com.books WHERE category = 'Travel'")
df = pd.DataFrame(result.result_set, columns=column_names)
print(df)

# наличие книг
MIN = 18
MAX = 20
result = client.query(f"SELECT * FROM books_toscrape_com.books WHERE available BETWEEN {MIN} and {MAX}")
df = pd.DataFrame(result.result_set, columns=column_names)
print(df)

# наличие книг по убыванию (первые 10)
result = client.query(f"SELECT TOP 10 category, name, available FROM books_toscrape_com.books ORDER BY available DESC")
df = pd.DataFrame(result.result_set, columns=["category", "name", "available"])
print(df)

# количество книг по китегориям
result = client.query("SELECT category, COUNT(id) as cnt FROM books_toscrape_com.books GROUP BY category ORDER BY cnt DESC")
df = pd.DataFrame(result.result_set, columns=["category", "count"])
print(df)

# средняя цена
result = client.query("SELECT AVG(price) FROM books_toscrape_com.books")
print(f"Средняя цена: {round(result.result_set[0][0], 2)}")

# средняя цена по категориям
result = client.query("SELECT category, AVG(price) as avg_price FROM books_toscrape_com.books GROUP BY category ORDER BY avg_price DESC")
df = pd.DataFrame(result.result_set, columns=["category", "avg_price"])
print(df)
