## Загрузка статистики команд NHL из API: https://statsapi.web.nhl.com/api/v1/teams

### Описание
Создан DAG (nhl_dag.py), который каждые 12 часов (в 11 утра и в 11 вечера) получает из апи данные и загружает в таблицы Clickhouse.

Данные загружаются в таблицы:
* nhl_stats.teams - Информация о командах
* nhl_stats.season_ranks - Рейтинги команд в сезоне (сохраняется историчность)
* nhl_stats.season_stats - Статистика команд в сезоне (сохраняется историчность)

Также создаются VIEW для получения актуальной статистики(последняя загруженная)
* nhl_stats.season_ranks_actual - Актуальный ретинг команд в сезоне
* nhl_stats.season_stats_actual - Актуальная статистика команд в сезоне

Для работы потребуется создать коннекшн в Airflow с именем "clickhouse".

Необходимые библиотеки: 
* requests
* json
* pandas

Также необходимо убедится, что дирректории python и sql находятся в одной дирректории с nhl_dag.py
