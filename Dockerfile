FROM gradle:9.0.0-jdk17 AS build

WORKDIR /app

COPY . /app

RUN ./gradlew clean build --no-daemon

FROM openjdk:17-jdk-slim

WORKDIR /app

COPY --from=build /app/build/libs/*.jar /high-load-course.jar

CMD ["java", "-jar", "/high-load-course.jar"]
