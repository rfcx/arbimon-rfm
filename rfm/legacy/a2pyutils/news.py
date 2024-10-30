
def insertNews(cursor, user, project, data, news_type):
    cursor.execute("""
        INSERT INTO project_news(user_id, project_id, data, news_type_id)
        VALUES (%s, %s, %s, %s)
    """, [
        user, project, data, news_type
    ])
