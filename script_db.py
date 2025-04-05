# Данный python-скрипт имитирует запрос к БД
# Напишите ваш SQL-запрос в query и запустите данный python-скрипт для получения результата
# Перед запуском скрипта установите библиотеку duckdb

# Установка библиотеки duckdb
# pip install duckdb duckdb-engine

# Импорт библиотек
import pandas as pd
import duckdb

# Задание таблиц БД
users = pd.read_csv('users.csv')
course_users = pd.read_csv('course_users.csv')
courses = pd.read_csv('courses.csv')
course_types = pd.read_csv('course_types.csv')
lessons = pd.read_csv('lessons.csv')
subjects = pd.read_csv('subjects.csv')
cities = pd.read_csv('cities.csv')
homework_done = pd.read_csv('homework_done.csv')
homework = pd.read_csv('homework.csv')
homework_lessons = pd.read_csv('homework_lessons.csv')
user_roles = pd.read_csv('user_roles.csv') 

# Задание SQL-запроса
query = """
with t_cnt_hw as (
    select l.course_id, hd.user_id, count(hd.homework_id) cnt_done_hw
    from lessons l
    join homework_lessons hl
    on l.id = hl.lesson_id
    /*берем только те уроки, по которым есть дз*/
    right join homework_done hd
    on hl.homework_id = hd.homework_id
    group by l.course_id, hd.user_id
)
select 
    c.id "ID курса", 
    c.name "Название курса", 
    ct.name "Тип курса", 
    s.name "Предмет", 
    s.project "Тип предмета", 
    c.starts_at "Дата старта курса", 
    cu.user_id "ID ученика", 
    u.last_name "Фамилия ученика", 
    city.name "Город ученика", 
    cu.active "Ученик не отчислен с курса", 
    cu.created_at "Дата открытия курса ученику",
    floor(cu.available_lessons / c.lessons_in_month) "Сколько полных месяцев курса открыто у ученика", 
    /*кол-во полных открытых месяцев определяем как отношение кол-ва доступных уроков к кол-ву уроков в месяц по курсу*/
    coalesce(t_cnt_hw.cnt_done_hw, 0) "Число сданных ДЗ ученика на курсе"
     /*если пользователь дз не сдавал, то выводим вместо null 0*/ 
from courses c
join course_types ct
on c.course_type_id = ct.id
/* для каждого курса есть соответствующий ему тип */
join subjects s
on c.subject_id = s.id
/* все предметы привязаны к какому-либо курсу */
join course_users cu
on c.id = cu.course_id
/* нас интересуют только те курсы, на которых записаны ученики (поэтому берем внутреннее соединение) */
join users u
on cu.user_id = u.id
/* нас интересуют только те ученики, которые записаны на какие-либо курсы (поэтому берем внутреннее соединение) */
join user_roles ur
on u.user_role_id = ur.id
left join cities city
on u.city_id = city.id
/* не по всем ученикам указаны города, и чтобы не потерять таких пользователей, берем внешнее соединение слева */
left join t_cnt_hw
on c.id = t_cnt_hw.course_id and u.id = t_cnt_hw.user_id
/* не все ученики сдавали дз, и чтобы не потерять таких пользователей, берем внешнее соединение слева */
where ur.name = 'student' 
and ct.name = 'Годовой'
and s.project in ['ЕГЭ', 'ОГЭ']
"""

# Выполнение SQL-запроса
df_result = duckdb.query(query).to_df()

# Вывод результата
print(df_result)

df_result.to_csv('df_result_1_task.csv', index=False)