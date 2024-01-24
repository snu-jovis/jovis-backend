# pg_queryplans backend

## Note

- SQL을 직접 다루는 서버이므로, 절대 외부 IP로 공개하지 마세요. SQL 공격에 매우 취약하고, 공격의 대상이 될 수 있습니다.
- `POST /query/` 만 지원하도록 간단하게 세팅했습니다. 아래 세팅을 완료한 뒤 postman이나 아래 명령어와 유사하게 테스트해보세요.

```bash
curl --location 'localhost:8000/query/' --form 'query="SELECT '\''hi!!!'\''"'
```

## 짱쉬운 세팅법

1. 아래 명령어를 이용하여 requirements를 설치합니다.
```bash
pip install -r requirements.txt
```

2. 데이터베이스 설정을 변경해야 합니다. 
`./web/views.py` 파일을 열고, `QueryView.post()` 함수의 `psycopg2.connect()` 부분을 확인하세요.
데이터베이스 세팅에 맞게 매개변수를 적절히 수정하세요.

3. (Optional) migration 하라고 경고가 뜰 수 있기 때문에, 아래 명령어를 이용하여 djgnao migration을 수행합니다.
```bash
python manage.py migrate
```

4. 아래 명령어를 이용하여 서버를 킵니다. 
```bash
python manage.py runserver
```