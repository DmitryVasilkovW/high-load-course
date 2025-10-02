FROM gradle:9.0.0-jdk17 AS build

COPY . /app

WORKDIR /app

RUN ./gradlew clean build

FROM openjdk:17-jdk-slim

WORKDIR /app
COPY --from=build /app/build/libs/*.jar /high-load-course.jar

CMD ["java", "-jar", "/high-load-course.jar"]
