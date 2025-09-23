### Сеть для работы с AirFlow в Docker
[Первоисточник](https://github.com/datanlnja/airflow-course-examples/tree/main) этого дистрибутива. В него добавлен Jupyter для удобства редактирования кода.

```bash
# В WSL
git clone -b main --single-branch https://github.com/SainSinner/airflow-course-examples.git
cd airflow-course-examples
sudo docker-compose build --no-cache && sudo docker-compose up -d
```
Для входа в AirFlow берем данные login/password из scripts/init.sh

Для входа в Jupyter берем данные Token из лога запуска Jupyter
