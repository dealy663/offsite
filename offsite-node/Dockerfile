FROM openjdk:8-alpine

COPY target/uberjar/offsite-node.jar /offsite-node/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/offsite-node/app.jar"]
