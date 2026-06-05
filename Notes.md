Pour Jupyter :

jupyter:
    image: jupyter/pyspark-notebook:spark-3.5.0
    container_name: ilam-jupyter
    restart: unless-stopped
    user: root
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - JUPYTER_TOKEN=ilam_jupyter_2025
      - SPARK_MASTER=spark://spark-master:7077
      - AWS_ACCESS_KEY_ID=ilam_admin
      - AWS_SECRET_ACCESS_KEY=ilam_password_2025
      - AWS_REGION=us-east-1
    ports:
      - "8888:8888"
    volumes:
      - ./notebooks:/home/jovyan/work
    networks:
      - ilam-net
    depends_on:
      - spark-master