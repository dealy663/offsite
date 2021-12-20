FROM openjdk:8-alpine

COPY target/uberjar/offsite-svc.jar /offsite-svc/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/offsite-svc/app.jar"]
