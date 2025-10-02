FROM gradle:8.5-jdk17 AS build

WORKDIR /app
COPY build.gradle.kts build.gradle.kts
COPY settings.gradle.kts settings.gradle.kts

COPY src src

RUN gradle build --no-daemon

FROM openjdk:17-jdk-slim

WORKDIR /app
COPY --from=build /app/build/libs/*.jar /high-load-course.jar

CMD ["java", "-jar", "/high-load-course.jar"]
