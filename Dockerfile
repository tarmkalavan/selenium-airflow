FROM apache/airflow:2.2.4
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  chromium \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
  python-dotenv==0.20.0 \
  selenium==4.2.0 \
  webdriver-manager==3.5.3 \
  psycopg2==2.7.7 \
  joblib==1.1.0 \
  tensorflow==2.9.1 \
  scikit-learn==1.0.2