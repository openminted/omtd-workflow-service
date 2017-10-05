FROM ubuntu:16.04

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y  software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get clean

COPY ./target/omtd-workflow-service.jar /home/user/app.jar

COPY ./src/main/resources/application.properties /home/user/application.properties


CMD ["java", "-jar", "/home/user/app.jar", "--spring.config.location=/home/user/application.properties"]
#CMD ["java -version"]