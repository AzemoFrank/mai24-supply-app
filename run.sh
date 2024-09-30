sudo docker-compose down --remove-orphans
sudo chmod a+rw /var/run/docker.sock
sudo docker-compose up airflow-init
sudo docker-compose up -d --build
sudo docker-compose ps
sudo ./airflow.sh --help