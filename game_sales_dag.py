import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


default_args = {
    'owner': 'nikolaj-sofinskij-pwt6989',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 7, 1)
}


# Задаем декоратор для DAG
@dag(dag_id='lesson_3_nikolaj-sofinskij-pwt6989', default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def vgsales_analysis():
    @task()
    def get_data():
        link = "https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv"
        data = pd.read_csv(link)
        # Поскольку между тасками Airflow 2 передает данные в формате JSON, 
        # лучше всего преобразовать весь DataFrame в CSV-строку перед возвратом:
    
        return data.to_csv(index=False)


    @task()
    def get_top1_sales(data_csv):
        # Восстанавливаем DataFrame из переданной строки
        data = pd.read_csv(StringIO(data_csv))
    
        # Находим самую продаваемую игру в 2011 году
        top_game = data.query('Year == 2011').sort_values(by='Global_Sales', ascending=False)['Name'].iloc[0]
    
        return top_game


    @task()
    def get_top_sales_genre_eu(data_csv):
        # Восстанавливаем DataFrame из переданной строки
        data = pd.read_csv(StringIO(data_csv))
    
        # Находим самые продаваемые жанры игр в 2011 году в Европе
        genre_sales = data.query('Year == 2011').groupby('Genre', as_index=False).agg(EU_Sales=('EU_Sales', 'sum'))
        max_sales = genre_sales['EU_Sales'].max()
        top_genres_list = genre_sales.query('EU_Sales == @max_sales')['Genre'].tolist()
        top_genres_string = ', '.join(top_genres_list)
    
        return top_genres_string


    @task()
    def get_top_sales_platform_na(data_csv):
        # Восстанавливаем DataFrame из переданной строки
        data = pd.read_csv(StringIO(data_csv))
    
        # Находим платформы с наибольшим количеством игр, которые продались более чем миллионным тиражом в Северной Америке в 2011 году
        platform_sales = data.query('Year == 2011 and NA_Sales > 1').groupby('Platform', as_index=False).agg(number=('NA_Sales', 'count'))
        max_sales_platform = platform_sales['number'].max()
        top_platform_list = platform_sales.query('number == @max_sales_platform')['Platform'].tolist()
        top_platform_string = ', '.join(top_platform_list)
    
        return top_platform_string
    

    @task()
    def get_top_sales_mean_publisher_jp(data_csv):
        # Восстанавливаем DataFrame из переданной строки
        data = pd.read_csv(StringIO(data_csv))
    
        # Находим издателей, у которых самые высокие средние продажи в Японии в 2011 году
        publisher_sales = data.query('Year == 2011').groupby('Publisher', as_index=False).agg(mean=('JP_Sales', 'mean'))
        max_sales_publisher_mean_jp = publisher_sales['mean'].max()
        top_publisher_list = publisher_sales.query('mean == @max_sales_publisher_mean_jp')['Publisher'].tolist()
        top_publisher_string = ', '.join(top_publisher_list)
    
        return top_publisher_string


    @task()
    def get_number_game_eu_more_jp(data_csv):
        # Восстанавливаем DataFrame из переданной строки
        data = pd.read_csv(StringIO(data_csv))
    
        # Посчитаем количество игр, которые продались в Европе лучше, чем в Японии в 2011 году
        number_eu_more_jp = len(data.query('Year == 2011 and EU_Sales > JP_Sales'))
    
        return number_eu_more_jp


    @task()
    def print_data(top_game, top_genres, top_platforms, top_publisher, count_eu_jp):
        context = get_current_context()
        date = context['ds'] # Получаем дату запуска DAG

        # Печатаем ответы в лог
        print(f"--- Результаты анализа за 2011 год (дата запуска: {date}) ---")
        print(f"1. Самая продаваемая игра во всем мире: {top_game}")
        print(f"2. Самые продаваемые жанры в Европе: {top_genres}")
        print(f"3. Платформа(ы) с наибольшим числом миллионников в NA: {top_platforms}")
        print(f"4. Издатель(и) с самыми высокими средними продажами в Японии: {top_publisher}")
        print(f"5. Количество игр, продавшихся в Европе лучше, чем в Японии: {count_eu_jp}")


    # Загружаем данные
    data_csv = get_data()

    # Передаем данные в расчетные таски
    top_game = get_top1_sales(data_csv)
    top_genres = get_top_sales_genre_eu(data_csv)
    top_platforms = get_top_sales_platform_na(data_csv)
    top_publisher = get_top_sales_mean_publisher_jp(data_csv)
    count_eu_jp = get_number_game_eu_more_jp(data_csv)

    # Передаем все результаты в финальный таск для печати
    print_data(top_game, top_genres, top_platforms, top_publisher, count_eu_jp)

    
# Активируем наш DAG:
vgsales_analysis_dag = vgsales_analysis()
