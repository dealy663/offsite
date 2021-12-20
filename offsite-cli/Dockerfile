FROM openjdk:8-alpine

COPY target/uberjar/offsite-cli.jar /offsite-cli/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/offsite-cli/app.jar"]
