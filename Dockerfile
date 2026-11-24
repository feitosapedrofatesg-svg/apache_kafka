FROM maven:3.9.9-eclipse-temurin-21 AS construcao
WORKDIR /lab
COPY pom.xml .
COPY src ./src
RUN mvn -B package

FROM eclipse-temurin:21-jre
WORKDIR /lab
COPY --from=construcao /lab/target/estoq-kafka.jar /lab/estoq-kafka.jar
ENTRYPOINT ["java", "-jar", "/lab/estoq-kafka.jar"]
CMD ["consumidor"]
