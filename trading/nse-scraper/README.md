#  nse-scraper



## Getting started

## Required Environment

**Python 3.10+**

**Mysql 8+**

**Ta-Lib**

## Instructions to set up the service

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    ```

2.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    ```

3.  **Activate the virtual environment:**
    ```bash
    source venv/bin/activate
    ```

4.  **Install requirements:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Config file updates:**
    
    - Create a config file, `config.py`, in the project root directory where `main.py` file is present.
    - Copy the following content to the `config.py` file and update it as needed.
   
    ```python
    # config.py
    from pydantic_settings import BaseSettings


    class Settings(BaseSettings):
        """Settings class defining configurations for the application."""
        MYSQL_SERVER: str = "localhost"
        MYSQL_USER: str = "admin"
        MYSQL_PASSWORD: str = "asdfghj"
        MYSQL_DB: str = "stock-market-data"
        DATABASE_URL: str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_SERVER}/{MYSQL_DB}"
        MAX_RETRIES: int = 5
        # Secret key for JWT token generation
        SECRET_KEY: str = "92cc7cc4470014ae19a33e672d72af3d"
        ALGORITHM: str = "HS256"  # Algorithm for JWT token generation
        # Expiry time for JWT tokens (in minutes)
        ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
        MAX_RETRIES: int = 5

        USERS: dict[str, str] = {'kpdasari@gmail.com': 'Kri$hnaD'}

        API_URL: str = "https://your_api_domain"
        # API_URL: str = "http://127.0.0.1:9009"

        SMTP_TLS: bool = True
        SMTP_SSL: bool = False
        SMTP_PORT: Optional[int] = 587
        SMTP_HOST: Optional[str] = 'smtp.gmail.com'
        SMTP_USER: Optional[str] = 'youreamil@yourdomain.com'
        SMTP_PASSWORD: Optional[str] = 'safa esff gyet uyuy'
        EMAILS_FROM_EMAIL: Optional[str] = "youreamil@yourdomain.com"
        EMAILS_FROM_NAME: Optional[str] = "youreamil@yourdomain.com"
        MONGO_URI: str = "mongodb://localhost:27017/market_data_live_feed"
        MONGO_DATABASE: str = "market_data_live_feed"
        MONGO_COLLECTION: str = "data"
        DHAN_CLIENT_ID: str = "dhan client id"
        DHAN_ACCESS_TOKEN: str = "dhan access token"


    
    settings = Settings()
    ```
    - Update the mysql server configuration details in the `config.py` file.
6.  **Run database migrations:**
    ```bash
    python -m db_migrations
    ```

7.  Run the following cmd for scraping the historical data, ***starting from 1st Jan 2024 to till date*.
    ```bash
    python -m load_hystorical_data
    ```
8.  Run the following cmd for scraping the daily data, ***Configure CRON job to run this cmd daily at 9:30pm***.
    ```bash
    python -m main
    ```
9. Run the following cmd to start the api server.
    ```bash
    # to run the server directly
    python -m server
    # to run the server using gunicorn with 2 workers and in backround.
    gunicorn --bind 0.0.0.0:9009 --log-file logs/gunicorn.log --workers 2 server:app -D
    ```
10. Run the following cmd for updating the  securities details daily, ***Configure CRON job to run this cmd daily at 8:30am***.
    ```bash
    python -m dhan_ref_data_parser
    ```
11. Run the following cmd to run the dhan live server.
    ```bash
    python -m dhan_server
    ```