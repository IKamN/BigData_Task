# BigData_Task
![Pipeline](https://user-images.githubusercontent.com/47881109/149827347-3eb08604-710b-4704-bed3-d659655e0325.png)

К вам обратился заказчик из полиции Нью-Йорка. Ему необходимо в реальном времени обрабатывать данные об авариях в городе.

1. Сообщения об авариях поступают в Kafka
2. Сырые (необработанные) данные заказчик хочет сохранять в файловую систему с партицированием по дате и времени
3. Также заказчик хочет подсчитывать каждые n минут следующие метрики:
   - Суммарное количество пострадавших каждого типа
   - Топ 20 почтовых индексов с наибольшим количеством аварий
   - Топ 10 районов с наибольшим количеством аварий

**Стек технологий**:
1. Scala 2.12.12
2. Spark 3.2.0 (spark-streaming, spark-streaming-kafka-0-10)
3. Kafka 2.13-2.8.1
4. Postgres latest

**Структура витрины injured (количество пострадавших):**

| Name                      | Data type |
|:--------------------------|:----------|
| timestamp                 | timestamp |
| count_injured             | bigint    |
| count_pedestrians_injured | bigint    |
| count_cyclist_injured     | bigint    |
| count_motorist_injured    | bigint    |

**Структура витрины rating_zip_code (топ 20 почтовых индексов):**

| Name                   | Data type |
|:-----------------------|:----------|
| timestamp              | timestamp |
| rank                   | integer   |
| ZIP_CODE               | text      |
| count_crashes          | bigint    |

**Структура витрины rating_borough (топ 10 районов):**

| Name                   | Data type |
|:-----------------------|:----------|
| timestamp              | timestamp |
| rank                   | integer   |
| BOROUGH                | text      |
| count_crashes          | bigint    |

**Процесс разработки**
1. Для начала необходимо подготовить стенд. Используя Docker, разверните Kafka, Postgre и PgAdmin4 (для удобства просмотра витрин)
2. Если Spark не установлен на локальной машине, то установить
3. Создать проект на GitHub, разработать потоковое spark приложение, собрать jar-файл (maven/sbt)
4. Запустить spark-приложение, используя spark-submit
5. Заполнить Kafka [данными об авариях](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)
6. Протестировать работу приложения
