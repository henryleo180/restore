# Get mysql configurations
DOCKER_IMAGE_NAME=$(sed -n "s/DOCKER_IMAGE_NAME=//p" config.ini)
CONTAINER_NAME=$(sed -n "s/CONTAINER_NAME=//p" config.ini)
MYSQL_ROOT_PASSWORD=$(sed -n "s/MYSQL_ROOT_PASSWORD=//p" config.ini)
MYSQL_USER=$(sed -n "s/MYSQL_USER=//p" config.ini)
MYSQL_DATABASE=$(sed -n "s/MYSQL_DATABASE=//p" config.ini)

# Create a new container on Docker
docker pull mysql:latest
docker run --name ${CONTAINER_NAME} -e MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD} -e MYSQL_USER=${MYSQL_USER} -e MYSQL_DATABASE=${MYSQL_DATABASE} -p 3306:3306 -d ${DOCKER_IMAGE_NAME}:latest

# check if the docker run well
docker ps